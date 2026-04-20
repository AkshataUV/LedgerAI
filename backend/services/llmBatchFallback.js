const logger = require('../utils/logger');
const { callLLM, getProviderInfo } = require('./llmService');

require('dotenv').config();

/**
 * Stage 4: LLM Batch Fallback
 * Asks an LLM to categorize a batch of transactions using a list of available categories.
 *
 * @param {Array} uncategorizedArray - List of transactions that failed deterministic checks.
 * @param {Array} availableCategories - List of valid categories [{ id: 123, name: 'Rent' }]
 * @returns {Promise<Array>} List of transaction items categorized with categorised_by='LLM'
 */
async function categorizeBatch(uncategorizedArray, availableCategories) {
  try {
    if (!uncategorizedArray || uncategorizedArray.length === 0) {
      return [];
    }

    const providerInfo = getProviderInfo();
    if (!providerInfo.configured) {
      logger.warn(`⚠️ LLM provider (${providerInfo.provider}) not configured, skipping LLM fallback`);
      return [];
    }

    logger.info('Using LLM provider', providerInfo);

    // Batch size of 25 is reliable with the 32K output token limit on Gemini 2.5 Flash
    const BATCH_SIZE = 25;
    const allResults = [];
    let successfulBatches = 0;
    let failedBatches = 0;

    for (let i = 0; i < uncategorizedArray.length; i += BATCH_SIZE) {
      const batch = uncategorizedArray.slice(i, i + BATCH_SIZE);
      const batchNum = Math.floor(i / BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(uncategorizedArray.length / BATCH_SIZE);

      logger.info('Processing LLM batch', {
        batchNum,
        totalBatches,
        batchSize: batch.length
      });

      const batchResults = await processBatch(batch, availableCategories);

      if (batchResults.length > 0) {
        successfulBatches++;
        allResults.push(...batchResults);
      } else {
        failedBatches++;
        logger.warn('⚠️ LLM batch returned no results', { batchNum, totalBatches });
      }
    }

    logger.info('LLM batch processing complete', {
      totalTransactions: uncategorizedArray.length,
      successfulBatches,
      failedBatches,
      categorizedCount: allResults.length
    });

    return allResults;

  } catch (err) {
    logger.error('❌ categorizeBatch encountered an error during processing', { error: err.message, stack: err.stack });
    return []; // Return empty on failure to proceed with other processes triggers safeguards
  }
}

async function processBatch(batch, availableCategories) {
  try {
    // 1. Construct Prompt
    const systemPrompt = `You are an expert accountant specializing in transaction categorization. You will be given a list of transactions and a list of valid account categories.

Your task is to match each transaction to the MOST APPROPRIATE category from the provided list.

IMPORTANT GUIDELINES:
- Analyze transaction details carefully (merchant names, keywords like DINNER, BREAKFAST, NETFLIX, etc.)
- ALWAYS try to assign a category - only use null if the transaction is completely unrecognizable
- Use context clues: DINNER/BREAKFAST/FOOD → Food & Dining, NETFLIX/ENTERTAINMENT → Living Expenses or Personal Care
- Be confident - if you're 50% sure or more, assign the category
- Extract a clean merchant_name from the raw details — strip bank codes, transaction IDs, dates, and account numbers, keep only the meaningful entity name
- Common patterns:
  * Food keywords (DINNER, BREAKFAST, RESTAURANT, CAFE) → Food & Dining
  * Transport (UBER, OLA, TAXI, METRO) → Travel & Transport
  * Utilities (ELECTRICITY, WATER, GAS) → Utilities
  * Entertainment (NETFLIX, SPOTIFY, MOVIES) → Living Expenses or Personal Care
  * Rent/Housing keywords → Housing & Rent

STRICT INSTRUCTION: Your response MUST be EXACTLY a raw JSON array. Do NOT wrap it inside markdown blocks (e.g., no \`\`\`json). Do NOT add conversational text.

Required JSON Structure:
[
  {
    "transaction_id": "...",
    "suggested_account_id": 123,
    "confidence": 0.85,
    "merchant_name": "SWIGGY"
  }
]

Rules for merchant_name:
- Extract the actual merchant or entity name from the transaction details
- For UPI: extract the business/person name, not the VPA suffix
- For NEFT/IMPS: extract the sender/receiver name if present
- For card transactions: extract the store/merchant name
- Keep it short, clean, and uppercase (e.g. "SWIGGY", "ZOMATO", "HDFC RENT", "AMAZON")
- Set to null only if no meaningful name can be extracted
- Do NOT include transaction IDs, dates, bank codes, or account numbers

Only set suggested_account_id to null if you truly cannot determine ANY reasonable category.`;

    const userPrompt = `
=== AVAILABLE ACCOUNTS ===
${JSON.stringify(availableCategories, null, 2)}

=== TRANSACTIONS TO CATEGORIZE ===
${JSON.stringify(batch.map(t => ({
  transaction_id: t.uncategorized_transaction_id || t.transaction_id,
  details: t.details,
  amount: t.debit || t.credit || 0,
  type: t.debit ? 'DEBIT' : 'CREDIT'
})), null, 2)}
`;

    // Debug logging
    logger.debug('LLM batch fallback', {
      categoriesCount: availableCategories.length,
      transactionsCount: batch.length,
      firstTransaction: batch[0]?.clean_merchant_name || batch[0]?.details || 'N/A'
    });

    // 2. Call LLM API (provider-agnostic)
    let contentString;
    try {
      contentString = await callLLM([
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userPrompt }
      ], 0.1);
    } catch (err) {
      logger.error('LLM API call failed', { error: err.message });
      return [];
    }

    // 3. Strip markdown code fences if the LLM disobeyed instructions
    const cleanContent = contentString
      .replace(/^```json\s*/i, '')
      .replace(/```\s*$/i, '')
      .trim();

    let parsedPredictions;
    try {
      parsedPredictions = JSON.parse(cleanContent);
    } catch (parseErr) {
      logger.error('LLM response was not valid JSON', {
        error: parseErr.message,
        preview: cleanContent.slice(0, 200)
      });
      return [];
    }

    if (!Array.isArray(parsedPredictions)) {
      logger.error('LLM returned non-array JSON', { type: typeof parsedPredictions });
      return [];
    }

    // Map IDs to lookup table for O(1) existence checks set benchmark frameworks forwards onwards
    const validAccountIds = new Set(availableCategories.map(cat => cat.id || cat.account_id));

    const safeResults = [];

    for (const prediction of parsedPredictions) {
      const { transaction_id, suggested_account_id, confidence, merchant_name } = prediction;

      // Safety Verification: Ensure suggested_account_id exists in available filters
      if (suggested_account_id && validAccountIds.has(suggested_account_id)) {
        // Find corresponding transaction from input to match accurate triggers benchmarks forwards
        const originalTxn = batch.find(t =>
          (t.uncategorized_transaction_id || t.transaction_id) == transaction_id
        );

        if (originalTxn) {
          safeResults.push({
            ...originalTxn,
            categorised_by: 'LLM',
            offset_account_id: suggested_account_id,
            confidence_score: parseFloat(confidence) || 0.50,
            llm_merchant_name: merchant_name || null
          });
        }
      } else {
        logger.warn('Discarded hallucinated account_id', {
          accountId: suggested_account_id,
          transactionId: transaction_id
        });
      }
    }

    return safeResults;

  } catch (err) {
    logger.error('processBatch error', { error: err.message, stack: err.stack });
    return [];
  }
}

module.exports = {
  categorizeBatch
};
