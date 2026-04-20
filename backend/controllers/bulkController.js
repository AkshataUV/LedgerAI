const logger = require('../utils/logger');
const contraRadarService = require('../services/contraRadarService');
const rulesEngineService = require('../services/rulesEngineService');
const vectorMatchService = require('../services/vectorMatchService');
const personalCacheService = require('../services/personalCacheService');
const supabase = require('../config/supabaseClient');
const llmBatchFallback = require('../services/llmBatchFallback');

const ML_SERVICE_URL = process.env.ML_SERVICE_URL || `http://127.0.0.1:${process.env.PYTHON_PORT || 5000}`;

// ─────────────────────────────────────────────────────────────────────────────
// CHANGE 3 HELPER — vector match using a pre-computed embedding
// Mirrors findVectorMatch() in vectorMatchService.js but accepts an embedding
// array directly, skipping the /embed ML call entirely.
// ─────────────────────────────────────────────────────────────────────────────
async function findVectorMatchWithEmbedding(embedding, cleanString, userId, transactionType) {
  try {
    if (!embedding || !userId) return null;

    const uppercaseString = (cleanString || '').toUpperCase();
    const requiredBalanceNature = transactionType === 'DEBIT' ? 'DEBIT' : 'CREDIT';

    // Stage 3.1: PERSONAL VECTOR CACHE
    const { data: pData, error: pError } = await supabase.rpc('match_personal_vectors', {
      p_user_id: userId,
      query_embedding: embedding,
      match_threshold: 0.35,
      match_count: 1
    });

    if (pError) {
      logger.error('findVectorMatchWithEmbedding (Personal) rpc error', { error: pError });
    } else if (pData && pData.length > 0) {
      return {
        offset_account_id: pData[0].account_id,
        confidence_score: 1.00,
        categorised_by: 'P_VEC'
      };
    }

    // Stage 3.1.5: GLOBAL KEYWORD RULES
    const { data: keywordRules, error: keywordError } = await supabase
      .from('global_keyword_rules')
      .select('keyword, match_type, target_template_id, priority')
      .eq('is_active', true)
      .order('priority', { ascending: false });

    if (keywordError) {
      logger.error('findVectorMatchWithEmbedding (Keyword) query error', { error: keywordError });
    } else if (keywordRules && keywordRules.length > 0) {
      for (const rule of keywordRules) {
        const keyword = rule.keyword.toUpperCase();
        const isMatch = rule.match_type === 'EXACT'
          ? uppercaseString === keyword
          : uppercaseString.includes(keyword);

        if (!isMatch) continue;

        const { data: accData, error: accError } = await supabase
          .from('accounts')
          .select('account_id')
          .eq('user_id', userId)
          .eq('template_id', rule.target_template_id)
          .eq('is_active', true)
          .eq('balance_nature', requiredBalanceNature)
          .limit(1);

        if (accError) continue;
        if (accData && accData.length > 0) {
          return {
            offset_account_id: accData[0].account_id,
            confidence_score: 0.95,
            categorised_by: 'G_KEY'
          };
        }
      }
    }

    // Stage 3.2: GLOBAL VECTOR CACHE
    const { data: gData, error: gError } = await supabase.rpc('match_vectors', {
      query_embedding: embedding,
      match_threshold: 0.35,
      match_count: 1
    });

    if (gError) {
      logger.error('findVectorMatchWithEmbedding (Global) rpc error', { error: gError });
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
    }

    return null;
  } catch (err) {
    logger.error('findVectorMatchWithEmbedding error', { error: err.message });
    return null;
  }
}


async function processUploadSSE(req, res) {
  try {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    const emit = (message, stage) => {
      res.write(`data: ${JSON.stringify({ message, stage })}\n\n`);
    };

    logger.info('Categorization request received', {
      transactionCount: req.body?.transactions?.length,
      userId: req.user?.id
    });

    const { transactions } = req.body;

    if (!transactions || !Array.isArray(transactions)) {
      logger.warn('Invalid payload received', { hasTransactions: !!transactions, isArray: Array.isArray(transactions) });
      emit('Something went wrong', 'error');
      res.write(`data: ${JSON.stringify({ error: true })}\n\n`);
      res.end();
      return;
    }

    const userId = req.user?.id;
    if (!userId) {
      logger.error('User authentication missing');
      emit('Something went wrong', 'error');
      res.write(`data: ${JSON.stringify({ error: true })}\n\n`);
      res.end();
      return;
    }

    // ==========================================
    // FETCH FALLBACK ACCOUNTS
    // ==========================================
    const { data: fallbackAccounts } = await supabase
      .from('accounts')
      .select('account_id, account_name, account_type')
      .eq('user_id', userId)
      .eq('is_system_generated', true)
      .in('account_name', ['Uncategorised Expense', 'Uncategorised Income']);

    const uncategorisedExpenseId = fallbackAccounts?.find(
      acc => acc.account_name === 'Uncategorised Expense'
    )?.account_id;
    const uncategorisedIncomeId = fallbackAccounts?.find(
      acc => acc.account_name === 'Uncategorised Income'
    )?.account_id;

    if (!uncategorisedExpenseId || !uncategorisedIncomeId) {
      logger.error('Fallback accounts not found', { userId });
      emit('Something went wrong', 'error');
      res.write(`data: ${JSON.stringify({ error: true })}\n\n`);
      res.end();
      return;
    }

    logger.info('Starting categorization pipeline', {
      totalTransactions: transactions.length,
      uncategorisedExpenseId,
      uncategorisedIncomeId
    });

    // ==========================================
    // INCREMENTAL FLUSH INFRASTRUCTURE
    // writtenUncatIds: tracks which uncategorized_transaction_ids have already
    // been inserted so no flush ever double-writes a row.
    // flushToDb: shared row-mapper + inserter used at every flush point.
    // ==========================================
    const writtenUncatIds = new Set();

    const flushToDb = async (items) => {
      const rows = items
        .filter(item => item.document_id)
        .filter(item => !item.uncategorized_transaction_id || !writtenUncatIds.has(item.uncategorized_transaction_id))
        .map(item => {
          const transactionType = item.debit ? 'DEBIT' : 'CREDIT';

          let finalOffsetAccountId = item.offset_account_id;
          let finalCategorisedBy   = item.categorised_by;
          let finalAttentionLevel  = item.attention_level;
          let isUncategorised      = item.is_uncategorised || false;

          if (!finalOffsetAccountId) {
            finalOffsetAccountId = transactionType === 'DEBIT'
              ? uncategorisedExpenseId
              : uncategorisedIncomeId;
            finalCategorisedBy  = 'UNCATEGORISED';
            finalAttentionLevel = 'HIGH';
            isUncategorised     = true;
          }

          return {
            user_id: userId,
            base_account_id: item.base_account_id || null,
            offset_account_id: finalOffsetAccountId,
            document_id: item.document_id,
            transaction_date: item.txn_date,
            details: item.details,
            clean_merchant_name: item.clean_merchant_name || null,
            amount: item.debit || item.credit || 0,
            transaction_type: transactionType,
            categorised_by: finalCategorisedBy,
            confidence_score: item.confidence_score || 0.5,
            is_contra: item.is_contra || false,
            posting_status: 'DRAFT',
            attention_level: finalAttentionLevel || 'LOW',
            review_status: 'PENDING',
            uncategorized_transaction_id: item.uncategorized_transaction_id || null,
            extracted_id: item.extracted_id || null,
            is_uncategorised: item.is_contra ? false : isUncategorised
          };
        });

      if (rows.length === 0) return;

      const { error } = await supabase.from('transactions').insert(rows);
      if (error) {
        logger.error('Flush insert failed', { error: error.message, count: rows.length });
      } else {
        logger.info('Flush insert successful', { count: rows.length });
        // Mark as written so subsequent flushes skip them
        for (const row of rows) {
          if (row.uncategorized_transaction_id) {
            writtenUncatIds.add(row.uncategorized_transaction_id);
          }
        }
      }
    };

    // ==========================================
    // CHANGE 1 — GROUPING WAIT GATE
    // Poll until all documents have finished the background grouping job
    // (grouping_status transitions from 'pending' → 'done').
    // Runs BEFORE contra radar so embeddings and group assignments are ready.
    // ==========================================
    const uniqueDocIds = [...new Set(
      transactions
        .map(t => t.document_id)
        .filter(Boolean)
    )];

    if (uniqueDocIds.length > 0) {
      logger.info('Change 1: Waiting for grouping to complete', { docIds: uniqueDocIds });

      const POLL_INTERVAL_MS = 2000;
      const TIMEOUT_MS = 30000;
      const startTime = Date.now();
      let pendingDocs = [];

      do {
        const { data: docRows } = await supabase
          .from('documents')
          .select('document_id, grouping_status')
          .in('document_id', uniqueDocIds)
          .eq('grouping_status', 'pending');

        pendingDocs = docRows || [];

        if (pendingDocs.length > 0) {
          if (Date.now() - startTime >= TIMEOUT_MS) {
            logger.warn('Grouping wait gate timed out — proceeding anyway', {
              pendingDocIds: pendingDocs.map(d => d.document_id)
            });
            break;
          }
          emit('Preparing transactions, almost ready…', 'grouping');
          await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
        }
      } while (pendingDocs.length > 0);

      logger.info('Grouping wait gate passed');
    }

    // ==========================================
    // STAGE 0: BATCH CONTRA RADAR (Pre-Loop)
    // ==========================================
    emit('Checking for internal transfers…', 'contra');
    logger.info('Stage 0: Running Contra Radar');
    const resolvedTransactions = await contraRadarService.findAndLinkContras(transactions, userId, supabase);

    // ── FLUSH 1: Contra transactions ─────────────────────────────────────────
    // Write contra rows immediately. They are excluded from pipelineTransactions
    // below so they will never be re-processed or double-inserted.
    const contraItems = resolvedTransactions
      .filter(t => t.is_contra === true)
      .map(t => ({ ...t, base_account_id: t.account_id || null }));

    if (contraItems.length > 0) {
      await flushToDb(contraItems);
      res.write(`data: ${JSON.stringify({ flush: true, stage: 'contra' })}\n\n`);
      logger.info('Flush 1 (contra) complete', { count: contraItems.length });
    }

    // ==========================================
    // CHANGE 2 — SKIP ALREADY-CATEGORISED ROWS
    // Remove transactions that were auto-categorised by the Python grouping
    // job (P_VEC). They already exist in the transactions table.
    // ==========================================
    const allUncatIds = resolvedTransactions
      .map(t => t.uncategorized_transaction_id)
      .filter(Boolean);

    let alreadyCategorisedIds = new Set();

    if (allUncatIds.length > 0) {
      const { data: existingTxns } = await supabase
        .from('transactions')
        .select('uncategorized_transaction_id')
        .in('uncategorized_transaction_id', allUncatIds);

      if (existingTxns && existingTxns.length > 0) {
        alreadyCategorisedIds = new Set(existingTxns.map(t => t.uncategorized_transaction_id));
        logger.info('Change 2: Skipping pre-categorised transactions', {
          count: alreadyCategorisedIds.size
        });
      }
    }

    // ── FLUSH 2: Pre-categorised rows (written by Python grouping job) ────────
    // These rows already exist in the transactions table — no insert needed.
    // Just mark them written and signal the frontend to refresh.
    if (alreadyCategorisedIds.size > 0) {
      for (const id of alreadyCategorisedIds) writtenUncatIds.add(id);
      res.write(`data: ${JSON.stringify({ flush: true, stage: 'pre_categorised' })}\n\n`);
      logger.info('Flush 2 (pre_categorised) signal sent', { count: alreadyCategorisedIds.size });
    }

    // Filter out already-categorised transactions and contras (already flushed)
    const pipelineTransactions = resolvedTransactions.filter(
      t => !t.is_contra &&
           (!t.uncategorized_transaction_id || !alreadyCategorisedIds.has(t.uncategorized_transaction_id))
    );

    logger.info('Transactions entering pipeline', {
      total: resolvedTransactions.length,
      skipped: resolvedTransactions.length - pipelineTransactions.length,
      remaining: pipelineTransactions.length
    });

    // ==========================================
    // CHANGE 3 — BATCH-FETCH PRE-COMPUTED STRATEGY & EMBEDDING
    // Fetch pre_pipeline_strategy and embedding for all remaining transactions
    // before the loop so we avoid N individual DB queries.
    // ==========================================
    const pipelineUncatIds = pipelineTransactions
      .map(t => t.uncategorized_transaction_id)
      .filter(Boolean);

    const preComputedMap = new Map(); // uncategorized_transaction_id → { pre_pipeline_strategy, embedding, merchant_group_id }

    if (pipelineUncatIds.length > 0) {
      const { data: preRows } = await supabase
        .from('uncategorized_transactions')
        .select('uncategorized_transaction_id, pre_pipeline_strategy, embedding, merchant_group_id')
        .in('uncategorized_transaction_id', pipelineUncatIds);

      if (preRows) {
        for (const row of preRows) {
          preComputedMap.set(row.uncategorized_transaction_id, {
            pre_pipeline_strategy: row.pre_pipeline_strategy,
            embedding: row.embedding,
            merchant_group_id: row.merchant_group_id,
          });
        }
      }
    }

    // ==========================================
    // CHANGE 4 — GROUP-LEVEL DEDUPLICATION SETUP
    // For non-contra, non-FAST_PATH, non-EXACT_THEN_DUMP transactions,
    // group by merchant_group_id. Only run the representative (lowest id)
    // through vector match and LLM. Apply the result to all group members.
    // ==========================================
    // We build two lookup structures:
    //   groupRepMap:    merchant_group_id → uncategorized_transaction_id of the representative
    //   groupResultMap: merchant_group_id → { offset_account_id, categorised_by, confidence_score, attention_level }
    //                   (populated when the representative is processed)
    const groupRepMap = new Map();       // gid → lowest uncategorized_transaction_id
    const groupResultMap = new Map();    // gid → pipeline result

    for (const txn of pipelineTransactions) {
      if (txn.is_contra) continue;
      const pre = preComputedMap.get(txn.uncategorized_transaction_id);
      const gid = pre?.merchant_group_id;
      if (!gid) continue;
      const strategy = pre?.pre_pipeline_strategy;
      // Only group transactions that will reach the vector / LLM stage
      if (strategy === 'FAST_PATH' || strategy === 'EXACT_THEN_DUMP') continue;

      const txnId = txn.uncategorized_transaction_id;
      if (!groupRepMap.has(gid) || txnId < groupRepMap.get(gid)) {
        groupRepMap.set(gid, txnId);
      }
    }

    const finalResults = [];

    emit('Matching known patterns…', 'rules');

    for (const txn of pipelineTransactions) {

      // Capture source bank account BEFORE pipeline touches it
      const sourceAccountId = txn.account_id || null;

      // ==========================================
      // STAGE 0: CONTRA SHORT-CIRCUIT
      // ==========================================
      if (txn.is_contra === true) {
        finalResults.push({
          ...txn,
          base_account_id: sourceAccountId,
          // offset_account_id already set by contraRadarService
        });
        continue;
      }

      // ── Fetch pre-computed data for this transaction ───────────────
      const preData = preComputedMap.get(txn.uncategorized_transaction_id) || {};
      const preStrategy = preData.pre_pipeline_strategy;   // may be undefined for old rows
      const preEmbedding = preData.embedding;              // float[] | null | undefined
      const preGroupId = preData.merchant_group_id;

      // CHANGE 4: If this transaction belongs to a group and is not the representative,
      // check if the representative has already been processed this loop iteration.
      if (preGroupId && groupRepMap.has(preGroupId)) {
        const isRep = groupRepMap.get(preGroupId) === txn.uncategorized_transaction_id;
        if (!isRep && groupResultMap.has(preGroupId)) {
          // Non-representative: propagate the rep's result
          const repResult = groupResultMap.get(preGroupId);
          finalResults.push({
            ...txn,
            base_account_id: sourceAccountId,
            offset_account_id: repResult.offset_account_id,
            clean_merchant_name: repResult.clean_merchant_name || null,
            categorised_by: repResult.categorised_by,
            confidence_score: repResult.confidence_score,
            attention_level: repResult.attention_level,
            extracted_id: repResult.extracted_id || null,
          });
          continue;
        }
        // If non-rep but rep not yet processed, fall through and process normally.
        // The rep will be processed first in most orderings; worst case this txn
        // goes through the full pipeline (still correct, just less optimised).
      }

      let cleanMerchantName = txn.details;

      // ==========================================
      // STAGE 1: RULES ENGINE
      // ==========================================

      // CHANGE 3: FAST_PATH short-circuit using pre-computed strategy
      if (preStrategy === 'FAST_PATH') {
        const rulesResult = rulesEngineService.evaluateTransaction(txn.details);
        const categoryAccountId = rulesResult.hasRuleMatch
          ? await getAccountIdFromTemplate(rulesResult.targetTemplateId, userId, supabase)
          : null;

        if (categoryAccountId) {
          const result = {
            ...txn,
            base_account_id: sourceAccountId,
            offset_account_id: categoryAccountId,
            categorised_by: 'G_RULE',
            confidence_score: 1.00,
            attention_level: 'LOW'
          };
          finalResults.push(result);
          if (preGroupId) groupResultMap.set(preGroupId, result);
          continue;
        }
        // Template lookup failed — fall through to full pipeline
      }

      const rulesResult = rulesEngineService.evaluateTransaction(txn.details);

      if (rulesResult.hasRuleMatch) {
        if (rulesResult.strategy === 'FAST_PATH') {
          const categoryAccountId = await getAccountIdFromTemplate(rulesResult.targetTemplateId, userId, supabase);

          if (categoryAccountId) {
            const result = {
              ...txn,
              base_account_id: sourceAccountId,
              offset_account_id: categoryAccountId,
              categorised_by: 'G_RULE',
              confidence_score: 1.00,
              attention_level: 'LOW'
            };
            finalResults.push(result);
            if (preGroupId) groupResultMap.set(preGroupId, result);
            continue;
          }
        }
        // CHANGE 3: EXACT_THEN_DUMP — skip straight to Stage 1.5 (P_EXACT check)
        else if (rulesResult.strategy === 'EXACT_THEN_DUMP' || preStrategy === 'EXACT_THEN_DUMP') {
          const searchKey = rulesResult.extractedId || txn.details;
          const personalMatch = await personalCacheService.checkExactMatch(userId, searchKey);

          if (personalMatch) {
            logger.info('Exact cache HIT before dump', { searchKey });
            const result = {
              ...txn,
              base_account_id: sourceAccountId,
              offset_account_id: personalMatch.offset_account_id,
              clean_merchant_name: searchKey.toUpperCase(),
              categorised_by: 'P_EXACT',
              confidence_score: 1.00,
              extracted_id: rulesResult.extractedId || null,
              attention_level: 'LOW'
            };
            finalResults.push(result);
            if (preGroupId) groupResultMap.set(preGroupId, result);
            continue;
          }

          // Keyword Rescue before dumping
          const trimmedDetails = txn.details.trim();
          const trailingKeywordMatch = trimmedDetails.match(/-([A-Z]{4,})$/i)
            || (() => {
              const parts = trimmedDetails.split('-');
              if (parts.length <= 1) return null;
              const lastDashPart = parts.pop()?.trim().replace(/\s+/g, '');
              return lastDashPart && lastDashPart.length >= 4 && /^[A-Z]+$/i.test(lastDashPart) ? [null, lastDashPart] : null;
            })();

          if (trailingKeywordMatch) {
            const rescuedKeyword = trailingKeywordMatch[1].toUpperCase();
            logger.info('Trapdoor RESCUED by trailing keyword', {
              details: txn.details.slice(0, 60),
              keyword: rescuedKeyword
            });
            cleanMerchantName = rescuedKeyword;
            // Fall through to vector stage below
          } else {
            const categoryAccountId = await getAccountIdFromTemplate(rulesResult.targetTemplateId, userId, supabase);
            const fallbackAccountId = txn.debit ? uncategorisedExpenseId : uncategorisedIncomeId;

            const result = {
              ...txn,
              base_account_id: sourceAccountId,
              offset_account_id: categoryAccountId || fallbackAccountId,
              categorised_by: 'FILTER',
              confidence_score: 0.00,
              attention_level: 'HIGH',
              is_uncategorised: true,
              extracted_id: rulesResult.extractedId || null
            };
            finalResults.push(result);
            if (preGroupId) groupResultMap.set(preGroupId, result);
            continue;
          }
        }
        else if (rulesResult.strategy === 'VECTOR_SEARCH') {
          cleanMerchantName = rulesResult.extractedId || txn.details;
        }
      }

      // ==========================================
      // STAGE 1.5: PERSONAL EXACT CACHE
      // ==========================================
      if (rulesResult.hasRuleMatch && rulesResult.strategy === 'VECTOR_SEARCH' && rulesResult.extractedId) {
        logger.debug('Checking exact cache', { extractedId: rulesResult.extractedId });
        const personalMatch = await personalCacheService.checkExactMatch(userId, rulesResult.extractedId);
        if (personalMatch) {
          logger.info('Exact cache HIT', { extractedId: rulesResult.extractedId });
          const result = {
            ...txn,
            base_account_id: sourceAccountId,
            offset_account_id: personalMatch.offset_account_id,
            clean_merchant_name: rulesResult.extractedId.toUpperCase(),
            categorised_by: 'P_EXACT',
            confidence_score: 1.00,
            extracted_id: rulesResult.extractedId || null,
            attention_level: 'LOW'
          };
          finalResults.push(result);
          if (preGroupId) groupResultMap.set(preGroupId, result);
          continue;
        } else {
          logger.debug('Exact cache MISS', { extractedId: rulesResult.extractedId });
        }
      }

      // ==========================================
      // STAGE 2: REGEX CLEANER
      // CHANGE 3: Skip entirely for VECTOR_SEARCH and NO_RULE — their
      // clean_merchant_name is derived from the pre-computed embedding text.
      // ==========================================
      const usePreComputedEmbedding =
        (preStrategy === 'VECTOR_SEARCH' || preStrategy === 'NO_RULE') && preEmbedding;

      if (!usePreComputedEmbedding) {
        let textToClean = rulesResult.extractedId || txn.details;

        // 1. Remove VPA Suffixes
        const vpaRegex = /@(okicici|okaxis|ybl|okhdfcbank|upi|paytm|oksbi|sbi|axl|ibl|icici)\b/gi;
        textToClean = textToClean.replace(vpaRegex, ' ');

        // 2. Strip non-alphanumeric
        textToClean = textToClean.replace(/[^a-zA-Z0-9\s]/g, ' ');

        // 3. Remove long numbers
        textToClean = textToClean.replace(/\b\d{4,}\b/g, ' ');

        // 4. Remove noise words
        const noiseWords = ['UPI', 'IMPS', 'NEFT', 'RTGS', 'TXN', 'POS', 'ECOM', 'REF', 'ATM', 'TRANSFER'];
        const noiseRegex = new RegExp(`\\b(${noiseWords.join('|')})\\b`, 'gi');
        textToClean = textToClean.replace(noiseRegex, ' ');

        // 5. Collapse spaces
        textToClean = textToClean.replace(/\s+/g, ' ').trim();

        cleanMerchantName = textToClean || 'UNKNOWN';
      }
      // If preStrategy is VECTOR_SEARCH/NO_RULE and we have a stored embedding,
      // cleanMerchantName stays as txn.details — the actual clean string is
      // encoded in preEmbedding and will be passed directly to stage 3.

      // ==========================================
      // STAGE 3: VECTOR SIMILARITY
      // CHANGE 3: Use stored embedding if available; skip /embed call.
      // CHANGE 4: Only run if this is the group representative (or no group).
      // ==========================================
      const isGroupRep = !preGroupId || groupRepMap.get(preGroupId) === txn.uncategorized_transaction_id;

      if (!isGroupRep && groupResultMap.has(preGroupId)) {
        // This shouldn't normally be reached (handled above), but as a safety net:
        const repResult = groupResultMap.get(preGroupId);
        finalResults.push({
          ...txn,
          base_account_id: sourceAccountId,
          offset_account_id: repResult.offset_account_id,
          clean_merchant_name: repResult.clean_merchant_name || null,
          categorised_by: repResult.categorised_by,
          confidence_score: repResult.confidence_score,
          attention_level: repResult.attention_level,
          extracted_id: repResult.extracted_id || null,
        });
        continue;
      }

      emit('Looking up your categorisation history…', 'vector');
      let vectorMatch = null;
      try {
        const transactionType = txn.debit ? 'DEBIT' : 'CREDIT';

        if (usePreComputedEmbedding) {
          // CHANGE 3: Bypass /embed — use the pre-computed embedding directly
          vectorMatch = await findVectorMatchWithEmbedding(
            preEmbedding,
            cleanMerchantName,
            userId,
            transactionType
          );
        } else {
          vectorMatch = await vectorMatchService.findVectorMatch(cleanMerchantName, userId, transactionType);
        }
      } catch (err) {
        logger.error('Vector match failed', { error: err.message });
      }

      if (vectorMatch) {
        const result = {
          ...txn,
          base_account_id: sourceAccountId,
          offset_account_id: vectorMatch.offset_account_id,
          clean_merchant_name: cleanMerchantName.toUpperCase(),
          categorised_by: vectorMatch.categorised_by,
          confidence_score: vectorMatch.confidence_score,
          extracted_id: rulesResult.extractedId || null,
          attention_level: 'LOW'
        };
        finalResults.push(result);
        if (preGroupId) groupResultMap.set(preGroupId, result);
        continue;
      }

      // Stage 3 failed — forward to LLM with no category
      const noMatchResult = {
        ...txn,
        base_account_id: sourceAccountId,
        offset_account_id: null,
        clean_merchant_name: cleanMerchantName
      };
      finalResults.push(noMatchResult);
      // Don't store in groupResultMap yet — LLM will fill it in below
    }

    // ── FLUSH 3: Pipeline results (Stages 1–3) ───────────────────────────────
    // Rows that have an offset_account_id at this point were resolved by the
    // rules engine, exact cache, or vector match. Write them now so the UI
    // can show them while LLM is still running.
    {
      const pipelineReady = finalResults.filter(t => t.offset_account_id && !t.is_contra);
      if (pipelineReady.length > 0) {
        await flushToDb(pipelineReady);
        res.write(`data: ${JSON.stringify({ flush: true, stage: 'pipeline' })}\n\n`);
        logger.info('Flush 3 (pipeline) complete', { count: pipelineReady.length });
      }
    }

    // ==========================================
    // STAGE 4: BATCH LLM FALLBACK
    // ==========================================
    const leftovers = finalResults.filter(t => !t.offset_account_id && !t.is_contra);

    logger.info('Stage 4: LLM Batch Fallback', { leftoverCount: leftovers.length });

    // CHANGE 4: For leftovers that belong to a group, only run the representative
    // through LLM and propagate the result to the rest of the group.
    // Identify non-representative leftovers whose representative IS also a leftover.
    const leftoverRepIds = new Set(
      leftovers.map(t => {
        const pre = preComputedMap.get(t.uncategorized_transaction_id) || {};
        const gid = pre.merchant_group_id;
        if (!gid || !groupRepMap.has(gid)) return t.uncategorized_transaction_id;
        return groupRepMap.get(gid);
      })
    );

    // Only send representative leftovers to LLM
    const llmCandidates = leftovers.filter(t => {
      const pre = preComputedMap.get(t.uncategorized_transaction_id) || {};
      const gid = pre.merchant_group_id;
      if (!gid || !groupRepMap.has(gid)) return true;
      return groupRepMap.get(gid) === t.uncategorized_transaction_id;
    });

    if (llmCandidates.length > 0) {
      emit(`Asking AI to categorise ${llmCandidates.length} transactions…`, 'llm');

      const debitLeftovers  = llmCandidates.filter(t => t.debit);
      const creditLeftovers = llmCandidates.filter(t => t.credit);

      logger.info('LLM batch separation', {
        debitCount: debitLeftovers.length,
        creditCount: creditLeftovers.length
      });

      // Helper: apply LLM prediction to a transaction AND all its group siblings
      const applyLlmResult = (prediction) => {
        const repId = prediction.uncategorized_transaction_id || prediction.transaction_id;
        const pre = preComputedMap.get(repId) || {};
        const gid = pre.merchant_group_id;

        // Which final-results items should receive this prediction?
        const targets = gid
          ? finalResults.filter(t => {
              const tPre = preComputedMap.get(t.uncategorized_transaction_id) || {};
              return tPre.merchant_group_id === gid;
            })
          : finalResults.filter(t =>
              (t.uncategorized_transaction_id || t.transaction_id) === repId
            );

        for (const match of targets) {
          match.offset_account_id = prediction.offset_account_id;
          match.categorised_by    = prediction.categorised_by || 'LLM';
          match.confidence_score  = prediction.confidence_score;
          match.llm_merchant_name = prediction.llm_merchant_name || null;
          match.attention_level   = prediction.confidence_score >= 0.8 ? 'LOW'
            : prediction.confidence_score >= 0.5 ? 'MEDIUM' : 'HIGH';
        }
      };

      // Process DEBIT transactions
      if (debitLeftovers.length > 0) {
        const { data: debitAccounts } = await supabase
          .from('accounts')
          .select('account_id, account_name, balance_nature')
          .eq('user_id', userId)
          .eq('is_active', true)
          .eq('is_system_generated', false)
          .eq('balance_nature', 'DEBIT')
          .eq('account_type', 'EXPENSE')
          .not('account_name', 'in', '("Uncategorised Expense")');

        const debitCategories = debitAccounts || [];
        logger.info('DEBIT categories for LLM', { count: debitCategories.length });

        if (debitCategories.length > 0) {
          const debitLlmResults = await llmBatchFallback.categorizeBatch(debitLeftovers, debitCategories);
          logger.info('DEBIT LLM categorization complete', { resultsCount: debitLlmResults.length });
          for (const prediction of debitLlmResults) {
            applyLlmResult(prediction);
          }

          // ── FLUSH 4: Debit LLM results ─────────────────────────────────────
          // applyLlmResult() mutates finalResults in-place. Collect all debit
          // entries that now have an offset_account_id (or will get the fallback).
          const debitUncatIds = new Set(debitLeftovers.map(t => t.uncategorized_transaction_id));
          const debitResolved = finalResults.filter(
            t => debitUncatIds.has(t.uncategorized_transaction_id)
          );
          if (debitResolved.length > 0) {
            await flushToDb(debitResolved);
            res.write(`data: ${JSON.stringify({ flush: true, stage: 'llm_debit' })}\n\n`);
            logger.info('Flush 4 (llm_debit) complete', { count: debitResolved.length });
          }
        }
      }

      // Process CREDIT transactions
      if (creditLeftovers.length > 0) {
        const { data: creditAccounts } = await supabase
          .from('accounts')
          .select('account_id, account_name, balance_nature')
          .eq('user_id', userId)
          .eq('is_active', true)
          .eq('is_system_generated', false)
          .eq('balance_nature', 'CREDIT')
          .eq('account_type', 'INCOME')
          .not('account_name', 'in', '("Uncategorised Income")');

        const creditCategories = creditAccounts || [];
        logger.info('CREDIT categories for LLM', { count: creditCategories.length });

        if (creditCategories.length > 0) {
          const creditLlmResults = await llmBatchFallback.categorizeBatch(creditLeftovers, creditCategories);
          logger.info('CREDIT LLM categorization complete', { resultsCount: creditLlmResults.length });
          for (const prediction of creditLlmResults) {
            applyLlmResult(prediction);
          }

          // ── FLUSH 5: Credit LLM results ─────────────────────────────────────
          const creditUncatIds = new Set(creditLeftovers.map(t => t.uncategorized_transaction_id));
          const creditResolved = finalResults.filter(
            t => creditUncatIds.has(t.uncategorized_transaction_id)
          );
          if (creditResolved.length > 0) {
            await flushToDb(creditResolved);
            res.write(`data: ${JSON.stringify({ flush: true, stage: 'llm_credit' })}\n\n`);
            logger.info('Flush 5 (llm_credit) complete', { count: creditResolved.length });
          }
        }
      }
    }

    // ── SAFETY FLUSH: Any remaining unwritten rows ────────────────────────────
    // Covers edge cases: no LLM categories available (empty account list),
    // or transactions that somehow escaped the above flush points.
    {
      const unwritten = finalResults.filter(
        t => t.uncategorized_transaction_id && !writtenUncatIds.has(t.uncategorized_transaction_id)
      );
      if (unwritten.length > 0) {
        logger.info('Safety flush: writing remaining rows', { count: unwritten.length });
        await flushToDb(unwritten);
      }
    }

    // ==========================================
    // CATEGORIZATION SUMMARY LOG
    // ==========================================
    const summaryCounts = {};
    for (const item of finalResults) {
      const method = item.categorised_by || 'UNCATEGORISED';
      summaryCounts[method] = (summaryCounts[method] || 0) + 1;
    }
    const totalCategorised   = finalResults.filter(t => t.categorised_by).length;
    const totalUncategorised = finalResults.filter(t => !t.categorised_by).length;

    logger.info('Categorization summary', {
      total: finalResults.length,
      categorised: totalCategorised,
      uncategorised: totalUncategorised,
      breakdown: summaryCounts,
      writtenToDb: writtenUncatIds.size
    });

    logger.info('Categorization complete', { totalResults: finalResults.length });

    emit('Done', 'done');
    res.write(`data: ${JSON.stringify({ done: true })}\n\n`);
    res.end();
    return;

  } catch (err) {
    logger.error('Bulk categorization exception', { error: err.message, stack: err.stack });
    emit('Something went wrong', 'error');
    res.write(`data: ${JSON.stringify({ error: true })}\n\n`);
    res.end();
    return;
  }
}


async function getAccountIdFromTemplate(templateId, userId, supabase) {
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

module.exports = {
  processUpload: processUploadSSE
};