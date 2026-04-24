/**
 * autoPipelineController.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Internal endpoint: POST /internal/auto-pipeline
 *
 * Called by the Python merchant_grouping job (via HTTP) after grouping
 * completes for a document. Not user-facing — protected by a shared secret.
 *
 * Runs Stages 1–3 of the categorisation pipeline:
 *   Stage 1   — FAST_PATH (rules engine)
 *   Stage 1.5 — EXACT_THEN_DUMP (P_EXACT personal exact cache)
 *   Stage 3   — Vector similarity (P_VEC → G_KEY → G_VEC)
 *
 * LLM leftovers are written to llm_queue for pickup when the user clicks
 * "LLM Categorise".
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * SQL — run once manually:
 *
 * CREATE TABLE llm_queue (
 *     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 *     uncategorized_transaction_id BIGINT REFERENCES uncategorized_transactions(uncategorized_transaction_id),
 *     user_id UUID NOT NULL,
 *     document_id INTEGER NOT NULL,
 *     status TEXT DEFAULT 'pending',  -- 'pending' | 'processing' | 'done'
 *     created_at TIMESTAMPTZ DEFAULT NOW()
 * );
 * CREATE INDEX idx_llm_queue_document ON llm_queue(document_id, status);
 * CREATE INDEX idx_llm_queue_user ON llm_queue(user_id, status);
 * ─────────────────────────────────────────────────────────────────────────────
 */

'use strict';

const logger = require('../utils/logger');
const supabase = require('../config/supabaseClient');
const rulesEngineService = require('../services/rulesEngineService');
const personalCacheService = require('../services/personalCacheService');
const vectorMatchService = require('../services/vectorMatchService');

// ── Person-name guard (mirrors Python _is_person_name) ───────────────────────
// Prevents UPI P2P transfer names (e.g. RUPALIMAHADEV) from reaching G_VEC.

const KNOWN_BRANDS = new Set([
  'AMAZON', 'SWIGGY', 'ZOMATO', 'NETFLIX', 'SPOTIFY', 'AIRTEL', 'JIOMART',
  'BLINKIT', 'ZEPTO', 'MYNTRA', 'FLIPKART', 'PAYTM', 'PHONEPE', 'GPAY',
  'IRCTC', 'HDFC', 'ICICI', 'AXIS', 'KOTAK', 'TATANEU', 'MEESHO', 'AJIO',
  'NYKAA', 'BIGBASKET', 'RAPIDO', 'OLA', 'UBER', 'MAKEMYTRIP', 'BOOKMYSHOW',
]);

function isPersonName(cleanName) {
  if (!cleanName) return false;
  const s = cleanName.trim().toUpperCase();
  if (!/^[A-Z]{4,25}$/.test(s)) return false;
  if ([...KNOWN_BRANDS].some(brand => s.includes(brand))) return false;
  return true;
}

// ── Auth ──────────────────────────────────────────────────────────────────────

function verifyInternalSecret(req) {
  const authHeader = req.headers['authorization'] || '';
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';
  const secret = process.env.INTERNAL_SECRET || '';
  if (!secret || token !== secret) return false;
  return true;
}

// ── Account-id helper (same as bulkController) ────────────────────────────────

async function getAccountIdFromTemplate(templateId, userId) {
  if (!templateId) return null;
  const { data, error } = await supabase
    .from('accounts')
    .select('account_id')
    .eq('user_id', userId)
    .eq('template_id', templateId)
    .eq('is_active', true)
    .limit(1);
  if (error || !data || data.length === 0) return null;
  return data[0].account_id;
}

// ── Main handler ──────────────────────────────────────────────────────────────

async function runAutoPipeline(req, res) {
  // ── Authentication ─────────────────────────────────────────────────────────
  if (!verifyInternalSecret(req)) {
    logger.warn('[AUTO-PIPELINE] Rejected — bad or missing INTERNAL_SECRET');
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { document_id, user_id } = req.body || {};

  if (!document_id || !user_id) {
    return res.status(400).json({ error: 'document_id and user_id are required' });
  }

  logger.info('[AUTO-PIPELINE] START', { document_id, user_id });

  // Mark pipeline as running so the frontend can disable interactions immediately
  await supabase
    .from('documents')
    .update({
      grouping_status: 'pipeline_running',
      pipeline_started_at: new Date().toISOString(),
      pipeline_error: null,  // clear any previous error
    })
    .eq('document_id', document_id);

  let pipelineError = null;  // set in catch; checked in finally to pick the right status
  try {
    // ══════════════════════════════════════════════════════════════════════════
    // STEP 1a — Fetch IDs of transactions that have already been categorised for this document
    const { data: existingTxns, error: existingErr } = await supabase
      .from('transactions')
      .select('uncategorized_transaction_id')
      .eq('document_id', document_id)
      .eq('user_id', user_id)
      .not('uncategorized_transaction_id', 'is', null);

    if (existingErr) {
      logger.error('[AUTO-PIPELINE] Failed to fetch existing transaction IDs', { error: existingErr.message });
      return res.status(500).json({ error: 'DB query failed', detail: existingErr.message });
    }

    const existingIds = (existingTxns || []).map(r => r.uncategorized_transaction_id);

    // STEP 1b — Fetch uncategorised transactions that grouping did NOT write
    //          to transactions yet (no P_VEC match from grouping job).
    let uncatQuery = supabase
      .from('uncategorized_transactions')
      .select(
        'uncategorized_transaction_id, details, txn_date, debit, credit, ' +
        'account_id, pre_pipeline_strategy, group_id, vector_cache_ref'
      )
      .eq('document_id', document_id)
      .eq('user_id', user_id)
      .eq('grouping_status', 'done');

    // Exclude rows already written to transactions
    if (existingIds.length > 0) {
      uncatQuery = uncatQuery.not('uncategorized_transaction_id', 'in', `(${existingIds.join(',')})`);
    }

    const { data: uncatRows, error: uncatErr } = await uncatQuery;

    if (uncatErr) {
      logger.error('[AUTO-PIPELINE] Failed to fetch uncategorised rows', { error: uncatErr.message });
      return res.status(500).json({ error: 'DB query failed', detail: uncatErr.message });
    }

    const pending = uncatRows || [];
    logger.info('[AUTO-PIPELINE] Pending rows to process', { count: pending.length, document_id });

    if (pending.length === 0) {
      logger.info('[AUTO-PIPELINE] Nothing to do — all rows already written or none pending');
      return res.json({ resolved: 0, llm_pending: 0, document_id });
    }

    // ══════════════════════════════════════════════════════════════════════════
    // STEP 2 — Batch-fetch pre-computed data
    // ══════════════════════════════════════════════════════════════════════════

    // Build preComputedMap: uncategorized_transaction_id → { pre_pipeline_strategy, group_id, vector_cache_ref }
    const preComputedMap = new Map();
    for (const row of pending) {
      preComputedMap.set(row.uncategorized_transaction_id, {
        pre_pipeline_strategy: row.pre_pipeline_strategy,
        group_id: row.group_id,
        vector_cache_ref: row.vector_cache_ref,
      });
    }

    // Fetch staging embeddings from personal_vector_cache
    const cacheRefs = pending
      .map(r => r.vector_cache_ref)
      .filter(Boolean);

    const embeddingMap = new Map(); // cache_id → float[]
    const cleanNameMap = new Map(); // cache_id → clean_name string

    if (cacheRefs.length > 0) {
      const { data: cacheRows, error: cacheErr } = await supabase
        .from('personal_vector_cache')
        .select('cache_id, embedding, clean_name')
        .in('cache_id', cacheRefs);

      if (cacheErr) {
        logger.warn('[AUTO-PIPELINE] Could not fetch staging embeddings', { error: cacheErr.message });
      } else {
        for (const row of (cacheRows || [])) {
          let emb = row.embedding;
          if (typeof emb === 'string') {
            try { emb = JSON.parse(emb); } catch { emb = null; }
          }
          if (Array.isArray(emb)) {
            embeddingMap.set(row.cache_id, emb);
          }
          if (row.clean_name) {
            cleanNameMap.set(row.cache_id, row.clean_name);
          }
        }
      }
    }

    logger.info('[AUTO-PIPELINE] Embedding cache loaded', { cacheRefs: cacheRefs.length, found: embeddingMap.size });

    // ══════════════════════════════════════════════════════════════════════════
    // STEP 3 — Group deduplication
    // Identify representative (lowest uncategorized_transaction_id) per group.
    // Only representatives go through the pipeline; results fan out to members.
    // ══════════════════════════════════════════════════════════════════════════

    // groupRepMap: group_id → lowest uncategorized_transaction_id
    const groupRepMap = new Map();

    for (const txn of pending) {
      const pre = preComputedMap.get(txn.uncategorized_transaction_id) || {};
      const gid = pre.group_id;
      if (!gid) continue;

      const txnId = txn.uncategorized_transaction_id;
      if (!groupRepMap.has(gid) || txnId < groupRepMap.get(gid)) {
        groupRepMap.set(gid, txnId);
      }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // STEP 4 — Run pipeline stages per group representative
    // groupResultMap: group_id → pipeline result object
    // ══════════════════════════════════════════════════════════════════════════

    const groupResultMap = new Map(); // group_id → result
    const resolvedRows = [];          // { txn, result } pairs ready to insert
    const llmLeftovers = [];          // uncategorized_transaction_ids needing LLM

    for (const txn of pending) {
      const txnId = txn.uncategorized_transaction_id;
      const pre = preComputedMap.get(txnId) || {};
      const gid = pre.group_id;
      const strategy = pre.pre_pipeline_strategy;

      // ── Non-representative: propagate rep result if available ───────────────
      if (gid && groupRepMap.has(gid) && groupRepMap.get(gid) !== txnId) {
        if (groupResultMap.has(gid)) {
          resolvedRows.push({ txn, result: groupResultMap.get(gid) });
        } else {
          // Rep not yet processed — handled below on rep's iteration.
          // We'll re-fan on a second pass after all reps finish.
          // Mark for deferred fan-out.
        }
        continue;
      }

      // ── This is the representative (or singleton) ──────────────────────────

      const transactionType = txn.debit ? 'DEBIT' : 'CREDIT';
      const balanceNature = transactionType; // same value

      // ─────────────────────────────────────────────────────────────────────
      // Stage 1 — FAST_PATH
      // ─────────────────────────────────────────────────────────────────────
      if (strategy === 'FAST_PATH') {
        const rulesResult = rulesEngineService.evaluateTransaction(txn.details);
        const categoryAccountId = rulesResult.hasRuleMatch
          ? await getAccountIdFromTemplate(rulesResult.targetTemplateId, user_id)
          : null;

        if (categoryAccountId) {
          const result = {
            offset_account_id: categoryAccountId,
            categorised_by: 'G_RULE',
            confidence_score: 1.00,
            attention_level: 'LOW',
            extracted_id: rulesResult.extractedId || null,
            clean_merchant_name: null,
          };
          if (gid) groupResultMap.set(gid, result);
          resolvedRows.push({ txn, result });
          continue;
        }
        // Template lookup failed — fall through to vector stage
      }

      // ─────────────────────────────────────────────────────────────────────
      // Stage 1.5 — P_EXACT (EXACT_THEN_DUMP)
      // ─────────────────────────────────────────────────────────────────────
      if (strategy === 'EXACT_THEN_DUMP') {
        const rulesResult = rulesEngineService.evaluateTransaction(txn.details);
        const searchKey = rulesResult.extractedId || txn.details;
        const personalMatch = await personalCacheService.checkExactMatch(user_id, searchKey);

        if (personalMatch) {
          logger.info('[AUTO-PIPELINE] P_EXACT HIT', { searchKey: searchKey?.slice(0, 60) });
          const result = {
            offset_account_id: personalMatch.offset_account_id,
            categorised_by: 'P_EXACT',
            confidence_score: 1.00,
            attention_level: 'LOW',
            extracted_id: rulesResult.extractedId || null,
            clean_merchant_name: searchKey?.toUpperCase() || null,
          };
          if (gid) groupResultMap.set(gid, result);
          resolvedRows.push({ txn, result });
        } else {
          logger.info('[AUTO-PIPELINE] P_EXACT MISS — grouping job already handled UNCATEGORISED dump', { txnId });
          // Grouping job already wrote UNCATEGORISED row — skip here.
        }
        continue;
      }

      // ─────────────────────────────────────────────────────────────────────
      // Stage 3 — Vector similarity (VECTOR_SEARCH / NO_RULE)
      // ─────────────────────────────────────────────────────────────────────
      const vecCacheRef = pre.vector_cache_ref;
      const embedding = vecCacheRef ? embeddingMap.get(vecCacheRef) : null;

      const cleanName = vecCacheRef ? cleanNameMap.get(vecCacheRef) || null : null;

      if (embedding) {
        // Guard: if clean_name looks like a person's name (UPI P2P), skip G_VEC
        // and send straight to LLM/manual review. This handles old rows that were
        // persisted before the Python-side fix was deployed.
        if (isPersonName(cleanName)) {
          logger.info('[AUTO-PIPELINE] Person name detected — skipping vector stage', {
            txnId,
            cleanName,
          });
          llmLeftovers.push(txnId);
          if (gid) groupResultMap.set(gid, null);
          continue;
        }

        const vectorMatch = await vectorMatchService.findVectorMatchWithEmbedding(
          embedding,
          user_id,
          balanceNature,
          cleanName
        );

        if (vectorMatch) {
          const result = {
            offset_account_id: vectorMatch.offset_account_id,
            categorised_by: vectorMatch.categorised_by,
            confidence_score: vectorMatch.confidence_score,
            attention_level: 'LOW',
            extracted_id: null,
            clean_merchant_name: null,
          };
          if (gid) groupResultMap.set(gid, result);
          resolvedRows.push({ txn, result });
          continue;
        }
      } else {
        logger.debug('[AUTO-PIPELINE] No embedding available for vector stage', { txnId, vecCacheRef });
      }

      // No match from any stage — add to LLM queue
      llmLeftovers.push(txnId);
      if (gid) {
        // Store a sentinel so group members know the rep went to LLM
        groupResultMap.set(gid, null);
      }
    }

    // ── Second pass: fan-out results to non-representatives ───────────────────
    for (const txn of pending) {
      const txnId = txn.uncategorized_transaction_id;
      const pre = preComputedMap.get(txnId) || {};
      const gid = pre.group_id;

      // Only handle non-reps that were skipped in the first pass
      if (!gid || groupRepMap.get(gid) === txnId) continue;

      // Check if already added to resolvedRows
      const alreadyResolved = resolvedRows.some(r => r.txn.uncategorized_transaction_id === txnId);
      if (alreadyResolved) continue;

      const repResult = groupResultMap.get(gid);

      if (repResult === null) {
        // Rep went to LLM — this member also goes to LLM queue
        llmLeftovers.push(txnId);
      } else if (repResult) {
        resolvedRows.push({ txn, result: repResult });
      }
      // If repResult is undefined (rep not processed?), skip — shouldn't happen
    }

    logger.info('[AUTO-PIPELINE] Stage results', {
      resolved: resolvedRows.length,
      llm_pending: llmLeftovers.length,
    });

    // ══════════════════════════════════════════════════════════════════════════
    // STEP 5 — Batch write resolved rows to transactions table
    // ══════════════════════════════════════════════════════════════════════════

    // Fetch fallback accounts once
    const { data: fallbackAccounts } = await supabase
      .from('accounts')
      .select('account_id, account_name')
      .eq('user_id', user_id)
      .eq('is_system_generated', true)
      .in('account_name', ['Uncategorised Expense', 'Uncategorised Income']);

    const uncategorisedExpenseId = fallbackAccounts?.find(
      a => a.account_name === 'Uncategorised Expense'
    )?.account_id;
    const uncategorisedIncomeId = fallbackAccounts?.find(
      a => a.account_name === 'Uncategorised Income'
    )?.account_id;

    if (resolvedRows.length > 0) {
      const insertRows = resolvedRows.map(({ txn, result }) => {
        const transactionType = txn.debit ? 'DEBIT' : 'CREDIT';
        let finalOffsetId = result?.offset_account_id || null;
        let finalCategorisedBy = result?.categorised_by || null;
        let finalAttentionLevel = result?.attention_level || 'LOW';
        let isUncategorised = false;

        if (!finalOffsetId) {
          finalOffsetId = transactionType === 'DEBIT' ? uncategorisedExpenseId : uncategorisedIncomeId;
          finalCategorisedBy = 'UNCATEGORISED';
          finalAttentionLevel = 'HIGH';
          isUncategorised = true;
        }

        return {
          user_id,
          base_account_id: txn.account_id || null,
          offset_account_id: finalOffsetId,
          document_id,
          transaction_date: txn.txn_date,
          details: txn.details,
          clean_merchant_name: result?.clean_merchant_name || null,
          amount: txn.debit || txn.credit || 0,
          transaction_type: transactionType,
          categorised_by: finalCategorisedBy,
          confidence_score: result?.confidence_score ?? 0.5,
          is_contra: false,
          posting_status: 'DRAFT',
          attention_level: finalAttentionLevel,
          review_status: 'PENDING',
          uncategorized_transaction_id: txn.uncategorized_transaction_id,
          extracted_id: result?.extracted_id || null,
          is_uncategorised: isUncategorised,
        };
      });

      const { error: insertErr } = await supabase.from('transactions').insert(insertRows);
      if (insertErr) {
        logger.error('[AUTO-PIPELINE] Batch insert failed', { error: insertErr.message, count: insertRows.length });
      } else {
        logger.info('[AUTO-PIPELINE] Batch insert OK', { count: insertRows.length });
      }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // STEP 6 — Store LLM leftovers in llm_queue
    // ══════════════════════════════════════════════════════════════════════════

    if (llmLeftovers.length > 0) {
      const queueRows = llmLeftovers.map(uncatId => ({
        uncategorized_transaction_id: uncatId,
        user_id,
        document_id,
        status: 'pending',
      }));

      const { error: queueErr } = await supabase.from('llm_queue').insert(queueRows);
      if (queueErr) {
        logger.error('[AUTO-PIPELINE] llm_queue insert failed', { error: queueErr.message, count: queueRows.length });
      } else {
        logger.info('[AUTO-PIPELINE] llm_queue populated', { count: queueRows.length });
      }
    }

    const response = {
      resolved: resolvedRows.length,
      llm_pending: llmLeftovers.length,
      document_id,
    };

    logger.info('[AUTO-PIPELINE] COMPLETE', response);
    return res.json(response);

  } catch (err) {
    pipelineError = err;
    logger.error('[AUTO-PIPELINE] Unhandled exception', { error: err.message, stack: err.stack });
    return res.status(500).json({ error: 'Internal server error', detail: err.message });
  } finally {
    if (pipelineError) {
      // Error path — record failure so frontend can show retry option
      await supabase
        .from('documents')
        .update({
          grouping_status: 'pipeline_failed',
          pipeline_error: pipelineError.message || 'Unknown error',
        })
        .eq('document_id', document_id);
    } else {
      // Success path
      await supabase
        .from('documents')
        .update({ grouping_status: 'pipeline_done' })
        .eq('document_id', document_id);
    }
  }
}

module.exports = { runAutoPipeline };
