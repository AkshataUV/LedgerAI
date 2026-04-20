const supabase = require('../config/supabaseClient');

const ML_SERVICE_URL = process.env.ML_SERVICE_URL || `http://127.0.0.1:${process.env.PYTHON_PORT || 5000}`;

/**
 * Helper function to identify garbage/noise in raw details
 * Returns true if the string looks like UPI, random hex, or system data
 */
function isGarbage(rawDetails) {
  if (!rawDetails) return false;
  const garbagePatterns = [
    /UPI\//i,
    /^\d+$/,
    /^[A-Z0-9]{10,}$/,
    /(@upi|@okaxis|@ybl|@paytm|@oksbi|@okicici|@okhdfcbank)/i
  ];
  return garbagePatterns.some(p => p.test(rawDetails.trim()));
}

/**
 * Stage 1.5: Personal Exact Cache Lookup
 * Checks if a user has manually categorized a specific, messy string (like a VPA or QR code) in the past.
 * 
 * @param {string} userId - The UUID of the user.
 * @param {string} rawString - The raw description string / VPA.
 * @returns {Promise<object|null>} { account_id, confidence_score, categorised_by } or null if no match.
 */
async function checkExactMatch(userId, rawString) {
  try {
    if (!userId || !rawString) {
      return null;
    }

    // Normalize to uppercase for case-insensitive matching
    const normalizedString = rawString.toUpperCase().trim();

    // Query top exact cache matches setup safely triggers forwards benchmarks accurately triggers
    const { data: matches, error } = await supabase
      .from('personal_exact_cache')
      .select('account_id')
      .eq('user_id', userId)
      .eq('raw_vpa', normalizedString)
      .limit(1);

    if (error) {
      console.error('❌ Error in checkExactMatch statement lookup:', error);
      return null;
    }

    if (matches && matches.length > 0) {
      return {
        offset_account_id: matches[0].account_id,
        confidence_score: 1.00, // Strict Requirement
        categorised_by: 'P_EXACT' // Strict Requirement
      };
    }

    return null;

  } catch (err) {
    console.error('❌ checkExactMatch encountered an exception:', err);
    return null;
  }
}

/**
 * Upsert a personal exact cache entry (exact VPA → account_id mapping)
 * Increments hit_count if the entry already exists, otherwise creates it.
 */
async function upsertExactCache(userId, rawVpa, accountId) {
  try {
    if (!userId || !rawVpa || !accountId) return;

    // Normalize to uppercase for case-insensitive matching
    const normalizedVpa = rawVpa.toUpperCase().trim();

    const { data: existing } = await supabase
      .from('personal_exact_cache')
      .select('hit_count')
      .eq('user_id', userId)
      .eq('raw_vpa', normalizedVpa)
      .maybeSingle();

    if (existing) {
      await supabase
        .from('personal_exact_cache')
        .update({ hit_count: existing.hit_count + 1 })
        .eq('user_id', userId)
        .eq('raw_vpa', normalizedVpa);
    } else {
      await supabase
        .from('personal_exact_cache')
        .insert({ user_id: userId, raw_vpa: normalizedVpa, account_id: accountId, hit_count: 1 });
    }
    console.log(`✅ personal_exact_cache upserted: ${normalizedVpa}`);
  } catch (err) {
    console.error('❌ upsertExactCache error:', err.message);
  }
}

/**
 * Upsert a personal vector cache entry (semantic merchant name → account_id mapping + embedding)
 * Calls the Python embedding service to generate the embedding vector.
 * Increments hit_count if the entry already exists.
 */
async function upsertVectorCache(userId, cleanName, accountId) {
  try {
    if (!userId || !cleanName || !accountId) return;

    const uppercaseName = cleanName.toUpperCase();

    // Check for existing entry
    const { data: existing } = await supabase
      .from('personal_vector_cache')
      .select('cache_id, hit_count')
      .eq('user_id', userId)
      .eq('clean_name', uppercaseName)
      .maybeSingle();

    if (existing) {
      await supabase
        .from('personal_vector_cache')
        .update({ hit_count: existing.hit_count + 1 })
        .eq('cache_id', existing.cache_id);
      console.log(`✅ personal_vector_cache hit_count updated: ${uppercaseName}`);
      return;
    }

    // Generate embedding via Python service
    const response = await fetch(`${ML_SERVICE_URL}/embed`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: uppercaseName })
    });

    if (!response.ok) {
      console.error(`❌ Embed call failed for ${uppercaseName}`);
      return;
    }

    const { embedding } = await response.json();
    if (!embedding || !Array.isArray(embedding)) return;

    await supabase
      .from('personal_vector_cache')
      .insert({
        user_id: userId,
        clean_name: uppercaseName,
        account_id: accountId,
        embedding,
        hit_count: 1
      });

    console.log(`✅ personal_vector_cache seeded: ${uppercaseName}`);
  } catch (err) {
    console.error('❌ upsertVectorCache error:', err.message);
  }
}

module.exports = {
  checkExactMatch,
  upsertExactCache,
  upsertVectorCache,
  isGarbage
};
