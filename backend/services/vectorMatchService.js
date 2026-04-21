const supabase = require('../config/supabaseClient');
require('dotenv').config();

const ML_SERVICE_URL = process.env.ML_SERVICE_URL || `http://127.0.0.1:${process.env.PYTHON_PORT || 5000}`;

/**
 * Handles the AI similarity matching for cleaned merchant strings.
 * Waterfall: Personal Vector (3.1) → Global Keyword Rules (3.1.5) → Global Vector (3.2)
 *
 * @param {string} cleanString - The merchant name or VPA string.
 * @param {string} userId - The UUID of the authenticated user.
 * @param {string} transactionType - 'DEBIT' or 'CREDIT' to filter by balance_nature.
 * @returns {object|null} Returns { offset_account_id, categorised_by, confidence_score } if matched, else null.
 */
async function findVectorMatch(cleanString, userId, transactionType) {
  try {
    if (!cleanString || !userId) return null;

    const uppercaseString = cleanString.toUpperCase();
    const requiredBalanceNature = transactionType === 'DEBIT' ? 'DEBIT' : 'CREDIT';

    // 1. Embedding Generation (Python ML Microservice)
    const response = await fetch(`${ML_SERVICE_URL}/embed`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: uppercaseString })
    });

    if (!response.ok) {
      throw new Error(`Embedding generation failed with status: ${response.status}`);
    }

    const { embedding } = await response.json();
    if (!embedding || !Array.isArray(embedding)) {
      throw new Error('Failed to retrieve 384-dimensional array embedding');
    }

    // ------------------------------------------
    // 🛡️ STAGE 3.1: PERSONAL VECTOR CACHE
    // ------------------------------------------
    // User's own history always takes highest priority.
    const { data: pData, error: pError } = await supabase.rpc('match_personal_vectors', {
      p_user_id: userId,
      query_embedding: embedding,
      match_threshold: 0.35,
      match_count: 1
    });

    if (pError) {
      console.error('❌ findVectorMatch (Personal) rpc error:', pError);
    } else if (pData && pData.length > 0) {
      return {
        offset_account_id: pData[0].account_id,
        confidence_score: 1.00,
        categorised_by: 'P_VEC'
      };
    }

    // ------------------------------------------
    // 🔑 STAGE 3.1.5: GLOBAL KEYWORD RULES
    // ------------------------------------------
    // High-confidence deterministic matching for obvious keywords (e.g. COFFEE, PETROL, PIZZA).
    // Runs AFTER personal history so user overrides are always respected.
    // Rules sorted by priority (highest first): e.g. "AMAZON MUSIC" > "AMAZON".
    const { data: keywordRules, error: keywordError } = await supabase
      .from('global_keyword_rules')
      .select('keyword, match_type, target_template_id, priority')
      .eq('is_active', true)
      .order('priority', { ascending: false });

    if (keywordError) {
      console.error('❌ findVectorMatch (Keyword) query error:', keywordError);
    } else if (keywordRules && keywordRules.length > 0) {
      for (const rule of keywordRules) {
        const keyword = rule.keyword.toUpperCase();
        const isMatch = rule.match_type === 'EXACT'
          ? uppercaseString === keyword
          : uppercaseString.includes(keyword);

        if (!isMatch) continue;

        console.debug(`🔑 Keyword rule matched: "${keyword}" (priority:${rule.priority}, template:${rule.target_template_id}) on "${uppercaseString.slice(0, 60)}"`);

        // Map the global template to the user's specific account, filtered by transaction type
        const { data: accData, error: accError } = await supabase
          .from('accounts')
          .select('account_id')
          .eq('user_id', userId)
          .eq('template_id', rule.target_template_id)
          .eq('is_active', true)
          .eq('balance_nature', requiredBalanceNature)
          .limit(1);

        if (accError) {
          console.error('❌ findVectorMatch (Keyword) template mapping error:', accError);
          continue; // Try next rule on error
        }

        if (accData && accData.length > 0) {
          console.info(`✅ G_KEY winner: "${keyword}" → template:${rule.target_template_id} → account:${accData[0].account_id}`);
          return {
            offset_account_id: accData[0].account_id,
            confidence_score: 0.95,
            categorised_by: 'G_KEY'
          };
        }

        // balance_nature mismatch — continue to next rule
        console.warn(`⚠️ Keyword rule "${keyword}" (template:${rule.target_template_id}) skipped — balance_nature mismatch. Required: ${requiredBalanceNature}`);
      }
    }

    // ------------------------------------------
    // 🌐 STAGE 3.2: GLOBAL VECTOR CACHE Fallback
    // ------------------------------------------
    // Last resort: fuzzy semantic similarity against the global curated vector library.
    const { data: gData, error: gError } = await supabase.rpc('match_vectors', {
      query_embedding: embedding,
      match_threshold: 0.35,
      match_count: 1
    });

    if (gError) {
      console.error('❌ findVectorMatch (Global) rpc error:', gError);
      throw gError;
    }

    if (gData && gData.length > 0) {
      const targetTemplateId = gData[0].target_template_id;

      const { data: accData, error: accError } = await supabase
        .from('accounts')
        .select('account_id, balance_nature')
        .eq('user_id', userId)
        .eq('template_id', targetTemplateId)
        .eq('is_active', true)
        .eq('balance_nature', requiredBalanceNature)
        .limit(1);

      if (accError) {
        console.error('❌ findVectorMatch (Global) template mapping error:', accError);
        return null;
      }

      if (accData && accData.length > 0) {
        return {
          offset_account_id: accData[0].account_id,
          confidence_score: 0.85,
          categorised_by: 'G_VEC'
        };
      }

      console.warn(`⚠️ Global vector match found but balance_nature mismatch. Template: ${targetTemplateId}, Required: ${requiredBalanceNature}`);
    }

    return null;

  } catch (err) {
    console.error('❌ findVectorMatch encountered an error:', err.message);
    return null;
  }
}

/**
 * findVectorMatchWithEmbedding
 * ─────────────────────────────────────────────────────────────────────────────
 * Same waterfall as findVectorMatch() but accepts a pre-computed embedding
 * array directly — skips the /embed ML call entirely.
 *
 * Used by autoPipelineController and bulkController when a staging embedding
 * is already available from the merchant grouping job.
 *
 * @param {number[]} embedding      - Pre-computed float embedding array.
 * @param {string}   userId         - UUID of the authenticated user.
 * @param {string}   balanceNature  - 'DEBIT' or 'CREDIT'.
 * @returns {object|null} { offset_account_id, categorised_by, confidence_score } or null.
 */
async function findVectorMatchWithEmbedding(embedding, userId, balanceNature) {
  try {
    if (!embedding || !Array.isArray(embedding) || !userId) return null;

    const requiredBalanceNature = balanceNature === 'DEBIT' ? 'DEBIT' : 'CREDIT';

    // ── Stage 3.1: PERSONAL VECTOR CACHE ──────────────────────────────────────
    const { data: pData, error: pError } = await supabase.rpc('match_personal_vectors', {
      p_user_id: userId,
      query_embedding: embedding,
      match_threshold: 0.35,
      match_count: 1
    });

    if (pError) {
      console.error('❌ findVectorMatchWithEmbedding (Personal) rpc error:', pError);
    } else if (pData && pData.length > 0) {
      return {
        offset_account_id: pData[0].account_id,
        confidence_score: 1.00,
        categorised_by: 'P_VEC'
      };
    }

    // ── Stage 3.1.5: GLOBAL KEYWORD RULES ────────────────────────────────────
    // NOTE: Keyword rules match on the clean string, which is encoded inside
    // the embedding. Without the original text we skip keyword rules here —
    // they will be picked up by the LLM fallback if still unresolved.
    // The stage is intentionally a no-op for this embedding-first path.

    // ── Stage 3.2: GLOBAL VECTOR CACHE ───────────────────────────────────────
    const { data: gData, error: gError } = await supabase.rpc('match_vectors', {
      query_embedding: embedding,
      match_threshold: 0.35,
      match_count: 1
    });

    if (gError) {
      console.error('❌ findVectorMatchWithEmbedding (Global) rpc error:', gError);
      return null;
    }

    if (gData && gData.length > 0) {
      const targetTemplateId = gData[0].target_template_id;

      const { data: accData, error: accError } = await supabase
        .from('accounts')
        .select('account_id, balance_nature')
        .eq('user_id', userId)
        .eq('template_id', targetTemplateId)
        .eq('is_active', true)
        .eq('balance_nature', requiredBalanceNature)
        .limit(1);

      if (accError) return null;

      if (accData && accData.length > 0) {
        return {
          offset_account_id: accData[0].account_id,
          confidence_score: 0.85,
          categorised_by: 'G_VEC'
        };
      }

      console.warn(`⚠️ findVectorMatchWithEmbedding: Global match found but balance_nature mismatch. Required: ${requiredBalanceNature}`);
    }

    return null;

  } catch (err) {
    console.error('❌ findVectorMatchWithEmbedding encountered an error:', err.message);
    return null;
  }
}

module.exports = {
  findVectorMatch,
  findVectorMatchWithEmbedding
};
