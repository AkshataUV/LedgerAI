const supabase = require('../config/supabaseClient');

let cachedRules = [];

/**
 * Loads active transaction routing rules from the Supabase database into memory.
 * Rules are sorted by priority from highest to lowest.
 */
async function loadRules() {
  try {
    const { data: rules, error } = await supabase
      .from('routing_rules')
      .select('*')
      .eq('is_active', true)
      .order('priority', { ascending: false });

    if (error) {
      console.error('❌ Error loading routing rules from Supabase:', error);
      return;
    }

    // Strict requirements: Use strict boolean evaluation (if (rule.is_active))
    cachedRules = (rules || []).filter(rule => rule.is_active === true);

    console.log(`✅ RULES ENGINE ONLINE: Loaded [${cachedRules.length}] active rules.`);
  } catch (err) {
    console.error('❌ Exception loading routing rules into memory:', err);
  }
}

/**
 * Evaluates a raw transaction string against the cached routing rules.
 * 
 * @param {string} rawDetails - The raw string description of the transaction.
 * @returns {object} An object containing { hasRuleMatch, strategy, extractedId, targetTemplateId }
 */
function evaluateTransaction(rawDetails) {
  if (!rawDetails) {
    return {
      hasRuleMatch: false,
      strategy: null,
      extractedId: null,
      targetTemplateId: null
    };
  }

  for (const rule of cachedRules) {
    try {
      if (!rule.pattern) continue;

      // Strict requirement: Force case-insensitivity
      const regex = new RegExp(rule.pattern, 'i');
      const match = rawDetails.match(regex);

      if (match) {
        // If the regex has capture groups, populate extractedId with the first capture group.
        // Otherwise, it will be null.
        const extractedId = match.length > 1 ? match[1] : null;

        return {
          hasRuleMatch: true,
          strategy: rule.strategy_type, // Map strategy_type to strategy
          extractedId: extractedId,
          targetTemplateId: rule.target_template_id
        };
      }
    } catch (err) {
      console.error(`❌ Rules Engine error evaluating rule [${rule.rule_name || 'Unnamed'}]:`, err);
    }
  }

  return {
    hasRuleMatch: false,
    strategy: null,
    extractedId: null,
    targetTemplateId: null
  };
}

module.exports = {
  loadRules,
  evaluateTransaction,
  // Export cached rules getter for verification/testing if needed
  _getCachedRules: () => cachedRules
};
