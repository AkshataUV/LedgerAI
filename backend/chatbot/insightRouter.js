/**
 * Insight Router — Smart Routing
 *
 * Philosophy:
 *   - If the query is about the user's OWN financial data → STATISTICAL (DB query)
 *   - If the query needs external/real-time info (gold rates, tax law, investment advice) → LLM
 *   - Default for anything financial = STATISTICAL (fast, free, always accurate)
 *
 * The router should be LIBERAL about routing to STATISTICAL — the agent handles
 * the nuance. Never fail a user's data question just because the pattern wasn't listed.
 */

// const logger = require('../utils/logger');
const logger = require('../utils/logger');
// ─── Queries that MUST go to LLM (external / real-time / advice) ─────
// These patterns are checked FIRST. If none match → STATISTICAL by default.
const LLM_ONLY_PATTERNS = [
  // External market data
  /(?:gold|silver|crude|oil|petroleum|forex|currency)\s*(?:rate|price|value|today|current|live|now)/i,
  /(?:current|today|live|latest|real.?time)\s*(?:gold|silver|crude|oil|forex|currency)\s*(?:rate|price)/i,
  /(?:stock|share|nifty|sensex|bse|nse|market)\s*(?:price|rate|index|today|current|live)/i,
  /(?:bitcoin|crypto|ethereum|btc|eth)\s*(?:price|rate|value)/i,

  // Tax law & regulations — ONLY external knowledge, NOT user's own tax data
  /(?:income\s*tax|gst|tds|itr)\s*(?:slab|rate|rule|regulation|filing|deadline|return|calculation)/i,
  /(?:slab|rate|rule|regulation|filing|deadline|deduction)\s+(?:for|of|under)\s+(?:tax|gst|itr|income\s*tax)/i,
  /(?:section)\s*(?:80c|80d|80g|24b?|10|87a)/i,
  /(?:how\s+to|when\s+to|steps?\s+to)\s+(?:file|calculate|submit|claim)\s+(?:tax|itr|gst|return)/i,
  /(?:save|reduce|cut|minimize)\s+(?:my\s+)?(?:income\s+)?tax(?:es)?\b/i,
  /tax\s+(?:saving|saver|savings|planning|exemption|rebate|relief)/i,
  /(?:what\s*is|what\s*are|define|meaning\s*of)\s*(?:inflation|repo|gdp|npa|fiscal|mutual\s*fund|etf|bond|cagr|xirr|nav)/i,

  // Investment & General Financial Advice
  /(?:tips?|advice|tricks?|ways?|guide|suggest(?:ions?)?|recommend(?:ations?)?)\s+(?:to|for|on|about|regarding|me)\s+(?:invest(?:ing)?|save|saving|manage|build|budget(?:ing)?|finance|financial|money|wealth|economy)/i,
  /(?:should\s*i\s*(?:invest|buy|sell|put\s+money|start\s+sip|save|budget))/i,
  /(?:is\s*it\s*(?:good|bad|safe|risky|worth))\s*(?:to\s*)?(?:invest|buy|put|save)/i,
  /(?:how\s+(?:can|do|should)\s+i|can\s+you\s+help\s+me)\s+(?:budget|save|invest|manage|plan|cut|reduce)/i,
  /(?:advise|advice|suggest|recommend|guide)\s*(?:me|on|about|for)\s+(?:invest|fund|stock|plan|sav(?:e|ings?)|budget(?:ing)?|finance)/i,
  /(?:mutual\s*fund|sip|fd|fixed\s*deposit|ppf|nps|elss|ulip)\s*(?:return|interest|rate|comparison|benefit|vs|versus|or)/i,
  /(?:which|what)\s+(?:is|would\s+be)\s+(?:better|best|more\s*profitable).*or/i,
  /(?:which|what)\s+(?:is|would\s+be)\s+(?:better|best|more\s*profitable)\s*:/i,
  /(?:difference\s+between|compare).*(?:and|or|vs|versus)/i,
  /(?:pros\s+and\s+cons|advantages|disadvantages)\s+of/i,
  /(?:what\s*is|what\s*are|define|meaning\s*of)\s*(?:a\s+|an\s+)?(?:high.?yield|hysa|fd|fixed\s*deposit|rd|recurring|sip|mutual\s*fund|nfo|ipo)/i,
  /(?:retirement\s*plan|insurance\s*plan\s*advice)/i,
  /(?:rbi|reserve\s*bank|sebi)\s*(?:policy|rule|regulation|update|news|announcement|guideline)/i,
  /(?:new|latest|recent)\s*(?:banking|finance)\s*(?:rule|regulation|policy|news)/i,
  /(?:repo\s*rate|inflation\s*rate|cpi|gdp\s*growth)/i,
  /(?:upi|rtgs|neft|imps)\s*(?:limit|charge|fee|rule|regulation)/i,
  /(?:explain|analyse|analyze|predict|forecast)\s+(?:my\s+)?(?:spending\s+trend|pattern|behaviour|behavior)/i,
  /(?:what\s+caused|reason\s+for|how\s+come)\s+(?:my|the)\s+/i,
  /(?:tips?|advice|tricks?|ways?)\s+(?:to|for)\s+(?:reduce\s+expense|save\s+money|manage\s+budget)/i,
];

// ─── Queries that are definitely about the user's OWN data → STATISTICAL ─
// Checked only if LLM patterns didn't match. These ensure correct routing.
// But even without these, financial queries default to STATISTICAL.
const STATISTICAL_SIGNALS = [
  /\b(?:my|i|i've|my\s+account|my\s+bank|my\s+spending|my\s+expense|my\s+income|my\s+saving|my\s+balance|my\s+transaction)\b/i,
  /(?:how\s+much|how\s+many|what\s+(?:is|was|were|are|did))\s+\b(?:i|my)\b/i,
  /(?:show|tell|give|fetch|get|display)\s+(?:me\s+)?\b(?:my|the|all|a|an)\b/i,
  /(?:last|this|past|previous)\s+(?:month|year|week|quarter|30\s*days|7\s*days|90\s*days|6\s*months)/i,
  /(?:january|february|march|april|may|june|july|august|september|october|november|december)\s*\d{4}/i,
  /\b20\d{2}\b/,
  /(?:biggest|largest|smallest|lowest|highest|maximum|minimum|average|avg|mean|top|most)\s+(?:transaction|expense|income|spend|category|payment|transfer|debit|credit)/i,
  /(?:total|sum|count|number)\s+(?:transaction|expense|income|debit|credit|spend|earning)/i,
  /(?:income|expense|saving|asset|liability|balance|net\s*worth|portfolio|transaction|category|bank\s*account|linked\s*account)\b/i,
  // Category spend queries — ANY phrasing
  /(?:spend(?:ing)?|spendings?|spent|expenditure|paid)\s+(?:on|in|for|at|towards?)\s+\w/i,
  /(?:spendings?|spend(?:ing)?)\s+in\s+\w/i,
  /what\s+(?:are|is)\s+(?:my\s+)?(?:spendings?|spend(?:ing)?|expenses?)\s+(?:in|on|for|at)\s+\w/i,
  // Budget & savings planning queries
  /\bbudget\b/i,
  /(?:how\s+much\s+should\s+i\s+(?:save|spend|allocat))/i,
  /(?:savings?\s+(?:goal|target|plan|progress|growth|trend|rate))/i,
  /(?:categories?\s+(?:to\s+)?(?:reduce|cut|limit))/i,
  /(?:reduce|cut)\s+(?:my\s+)?(?:spending|expenses?)/i,
  /(?:planned\s+vs|actual\s+vs|vs\s+actual)/i,
];

/**
 * Classify user query into routing lane.
 *
 * Rule: Default to STATISTICAL for any financial query.
 * Only send to LLM if the query explicitly needs external/real-world knowledge.
 */
function classifyQuery(query) {
  const trimmed = query.trim();

  // ─── STEP 1: Conversational greetings ──────────────────────────────────
  if (/^(?:hi|hello|hey|how\s+are\s+you|who\s+are\s+you|what\s+can\s+you\s+do|good\s+morning|good\s+evening|good\s+afternoon|thanks?|thank\s+you|bye|goodbye)\b/i.test(trimmed)) {
    logger.info('InsightRouter → LLM_REALTIME (greeting)', { query: trimmed.slice(0, 60) });
    return { lane: 'LLM_REALTIME', confidence: 0.8, matchedPattern: 'greeting' };
  }

  // ─── STEP 2: LLM-only explicit patterns ─────────────────────────────────
  // We check this FIRST because if the user explicitly asks about tax laws,
  // gold rates, or CA advice, it MUST go to the LLM—even if it contains
  // words like "my" or "expenses".
  for (const pattern of LLM_ONLY_PATTERNS) {
    if (pattern.test(trimmed)) {
      logger.info('InsightRouter → LLM_REALTIME (explicit pattern)', { query: trimmed.slice(0, 60) });
      return { lane: 'LLM_REALTIME', confidence: 0.95, matchedPattern: pattern.toString() };
    }
  }

  // ─── STEP 3: Finance fast-track (Database DB) ────────────────────────
  // If ANY strong personal financial signal is present → route to STATISTICAL.
  for (const pattern of STATISTICAL_SIGNALS) {
    if (pattern.test(trimmed)) {
      logger.info('InsightRouter → STATISTICAL (fast-track)', { query: trimmed.slice(0, 60) });
      return { lane: 'STATISTICAL', confidence: 0.95, matchedPattern: pattern.toString() };
    }
  }

  const FINANCE_FASTTRACK = /(?:\btransaction(?:s)?\b|\bexpense(?:s)?\b|\bincome\b|\bspend(?:ing)?\b|\bsav(?:e|ings?)\b|\bbudget\b|\bbalance\b|\bbank\b|\bemi\b|\bloan\b|\bcredit\b|\bdebit\b|\bpayment\b|\brupee\b|₹|\bledger\b|\bearning(?:s)?\b|\basset(?:s)?\b|\bliabilit(?:y|ies)\b|\bnet\s+worth\b|\bfinancial\b|\bportfolio\b|\bcategory\b|\bcashback\b|\brefund\b|\bsummary\b|\bmonthly\b|\breport\b)/i;
  if (FINANCE_FASTTRACK.test(trimmed)) {
    logger.info('InsightRouter → STATISTICAL (finance fast-track)', { query: trimmed.slice(0, 60) });
    return { lane: 'STATISTICAL', confidence: 0.9, matchedPattern: 'finance-fast-track' };
  }

  // ─── STEP 4: OUT_OF_SCOPE guard ────────────────────────────────────────
  // Only runs AFTER LLM and Statistical explicit tracks.
  const OUT_OF_SCOPE_PATTERNS = [
    /\b(?:recipe|how\s+to\s+(?:make|cook|bake)|ingredients?\s+for|dish\s+to\s+make|pasta\s+recipe|pizza\s+recipe)\b/i,
    /\b(?:movie\s+review|film\s+recommendation|cricket\s+score|ipl\s+score|football\s+score|song\s+lyrics|music\s+playlist|celebrity\s+news|bollywood\s+news)\b/i,
    /\b(?:how\s+to\s+(?:lose\s+weight|build\s+muscle|do\s+yoga|meditate)|diet\s+plan|workout\s+routine|calorie\s+count|protein\s+intake)\b/i,
    /\b(?:visa\s+process|passport\s+apply|flight\s+booking|hotel\s+booking|weather\s+forecast|tourist\s+spots?)\b/i,
    /\b(?:how\s+to\s+code|programming\s+tutorial|debug\s+(?:this|my)\s+code|software\s+review|hardware\s+spec|gaming\s+setup|minecraft)\b/i,
    /\b(?:who\s+(?:is|was)\s+(?!my)|capital\s+of|population\s+of|largest\s+country|how\s+tall\s+is|how\s+old\s+is|born\s+in\s+\d{4}|planet\s+(?:earth|mars)|space\s+exploration|chemistry\s+formula|biology\s+class|physics\s+law|history\s+of\s+(?:india|world|science|art|war|the|a\s+\w))\b/i,
    /\b(?:tell\s+me\s+a\s+joke|funny\s+meme|riddle\s+for|love\s+quote|relationship\s+advice|dating\s+app)\b/i,
  ];

  for (const pattern of OUT_OF_SCOPE_PATTERNS) {
    if (pattern.test(trimmed)) {
      logger.info('InsightRouter → OUT_OF_SCOPE', { query: trimmed.slice(0, 60) });
      return { lane: 'OUT_OF_SCOPE', confidence: 1.0 };
    }
  }

  // ─── STEP 4: Finance word default (General QA) ────────────────────────
  // Any query that has a financial word but skipped the DB fast-tracks above
  // should gracefully fall back to the intelligent LLM.
  const hasFinanceWord = /(?:account|bank|finance|financial|money|budget|spend|expense|income|save|salary|market|transaction|credit|debit|loan|emi|payment|transfer|amount|rupee|₹|balance|category|earning|saving|asset|liability|profit|loss|inflow|outflow|worth|net|total|bill|invoice|ledger|summary|report|month|year|tax|taxes|invest|investment|ca|chartered\s+accountant|afford|affordability|property|wealth|fund|stock|share|sip|crypto|trading|economy|economy|portfolio|mutual\s+fund)/i.test(trimmed);
  if (hasFinanceWord) {
    logger.info('InsightRouter → LLM_REALTIME (general finance chat)', { query: trimmed.slice(0, 60) });
    return { lane: 'LLM_REALTIME', confidence: 0.7, matchedPattern: 'general-finance-chat' };
  }

  // ─── STEP 5: Final fallback ────────────────────────────────────────────
  logger.info('InsightRouter → OUT_OF_SCOPE (no finance signal)', { query: trimmed.slice(0, 60) });
  return { lane: 'OUT_OF_SCOPE', confidence: 0.9 };
}


// ─── Financial keyword blocklist — these are NOT category names ──────
const FINANCIAL_META_WORDS = new Set([
  // Core financial terms
  'income', 'expense', 'expenses', 'saving', 'savings', 'balance',
  'asset', 'assets', 'liability', 'liabilities', 'total', 'net',
  'debit', 'credit', 'transaction', 'transactions', 'account', 'accounts',
  'financial', 'overview', 'summary', 'breakdown', 'worth', 'profit',
  'loss', 'earnings', 'inflow', 'outflow', 'budget', 'money', 'my',
  'overall', 'vs', 'versus', 'and', 'compared', 'the', 'a', 'an',
  'this', 'last', 'month', 'monthly', 'month-by-month', 'year', 'yearly', 'annual', 'week', 'today', 'current', 'all',
  'every', 'each', 'spend', 'spending', 'spent',
  'how', 'what', 'much', 'is', 'are', 'was', 'were', 'do', 'did', 'does', 'have', 'has',
  'remaining', 'left', 'allocate', 'allocating', 'set', 'reduce', 'cut',
  'planned', 'actual', 'suggest', 'suggested', 'recommended', 'ideal',
  'percentage', 'percent', 'track', 'tracking', 'goal', 'target',
  // ── Superlatives / size modifiers ────────────────────────────────────
  // Prevents "my largest transaction" → SPECIFIC_CATEGORY_SPEND
  'largest', 'biggest', 'highest', 'maximum', 'max', 'most', 'top',
  'smallest', 'minimum', 'min', 'least', 'lowest',
  'single', 'one', 'recent', 'latest', 'new', 'old', 'first', 'last',
  // ── Query meta words ─────────────────────────────────────────────────
  'history', 'trend', 'trends', 'growth', 'report', 'analysis',
  'data', 'stats', 'statistics', 'info', 'information', 'detail', 'details',
  'show', 'tell', 'give', 'get', 'list', 'display', 'check', 'see',
]);


/**
 * Detect the specific sub-intent for routing to the right DB handler.
 *
 * Tier ordering is CRITICAL — specific intents must come before generic catch-alls.
 * The final fallback is UNIVERSAL_QUERY which parses anything dynamically.
 */
function detectStatisticalIntent(query) {
  const q = query.toLowerCase().trim();

  // ═══ TIER 1: Account queries ════════════════════════════════════════
  if (/(?:how\s*many|number\s*of|count|total)\s*(?:my\s*)?(?:linked\s*|connected\s*|added\s*)?(?:bank\s*)?accounts?/i.test(q)) {
    return 'ACCOUNT_COUNT';
  }
  if (/(?:list|show|what\s+are|which)\s*(?:my\s*|all\s*)?(?:linked\s*|connected\s*|added\s*)?(?:bank\s*)?accounts?/i.test(q)) {
    return 'ACCOUNT_LIST';
  }
  if (/(?:bank\s*)?account\s*(?:summary|balance|balances|overview|details)/i.test(q) ||
      /(?:balance\s*in\s*(?:my\s*)?(?:bank|account|linked\s*account))/i.test(q) ||
      /(?:how\s*much)\s*(?:do\s*i\s*have\s*in\s*(?:my|my\s*bank|account|linked\s*account))/i.test(q) ||
      /(?:all\s*account|each\s*account|every\s*account)\s*balance/i.test(q)) {
    return 'BANK_ACCOUNT_SUMMARY';
  }

  // ═══ TIER 2: Income vs Expense comparison ════════════════════════════
  // MUST come before individual income/expense checks
  if (/income\s*(?:vs|versus|and|compared|or|&|against)\s*expense/i.test(q) ||
      /expense\s*(?:vs|versus|and|compared|or|&|against)\s*income/i.test(q) ||
      /(?:profit|loss)\s*(?:and\s*loss|account|statement)?$/i.test(q) ||
      /p\s*&\s*l|p\s*and\s*l/i.test(q)) {
    return 'INCOME_VS_EXPENSE';
  }

  // ═══ TIER 2.5: Budget, Affordability & Savings Advice — MUST come before individual savings/expense tiers ══════════════
  // Catches all "budget", "goals", and "affordability" queries before they fall into hardcoded db handlers
  if (/\bbudget\b/i.test(q) || 
      /(?:savings?\s+(?:goal|target|plan|challenge|tip|advice|strategy|progress))/i.test(q) || 
      /(?:save|saving)s?\s+(?:more|better|faster|money|everything|anything)/i.test(q) ||
      /(?:should\s+i\s+(?:be\s+)?(?:saving|allocating|spending|buying|investing|purchasing))/i.test(q) ||
      /(?:how\s+to\s+(?:save|invest|budget)\s*(?:for|more)?)/i.test(q) ||
      /(?:percentage\s+of\s+my\s+income)/i.test(q) ||
      /(?:how\s+much\s+(?:do|should|can)\s+i\s+(?:need\s+to\s+)?(?:save|spend|afford|invest|buy))/i.test(q) ||
      /(?:can\s+i\s+(?:afford|buy|purchase|get|manage))/i.test(q) ||
      /(?:what|which|where).*(?:can|should|could|would)\s*(?:i|we)\s*(?:buy|afford|get|purchase|invest)/i.test(q) ||
      /(?:on|with|given)\s+(?:my|our)\s+(?:current\s+)?(?:savings?|budget|income|salary|money|finances).*(?:which|what|can|should|buy)/i.test(q) ||
      /(?:am\s+i\s+(?:spending|saving)\s+(?:too\s+much|enough|a\s+lot|properly))/i.test(q) ||
      /(?:is\s+it\s+(?:a\s+good\s+idea|smart|wise|safe|bad|okay)\s+to\s+(?:buy|spend|invest|purchase|afford))/i.test(q) ||
      /(?:help\s+me\s+(?:plan|save|budget|afford|cut|reduce|manage))/i.test(q) ||
      (/(?:invest|mutual\s+fund|sip|fd|stock|share)(?:s)?/i.test(q) && /(?:advice|suggest|recommend|should|where|how|what)/i.test(q))) {
    return 'BUDGET_INSIGHT';
  }
  // "what categories to reduce/cut", "reduce spending", "overspending", "allocate income"
  if (/(?:what\s+categories?|which\s+categories?)\s+(?:to|should\s+i)\s+(?:reduc|cut|limit|control)/i.test(q) ||
      /(?:reduc|cut|minimize)\s+(?:my\s+)?(?:spending|expense|cost|expenses)/i.test(q) ||
      /(?:where\s+(?:am\s+i|should\s+i)\s+(?:overspend|spend\s+(?:too|more)))/i.test(q) ||
      /(?:how\s+much\s+(?:should|do)\s+i\s+(?:allocat|spend|put|save|need\s+to\s+save)\s*(?:for|on|in|to|towards)?)/i.test(q) ||
      /(?:planned\s+vs\s+actual|actual\s+vs\s+planned|vs\s+actual)/i.test(q) ||
      /(?:track\s+(?:my\s+)?(?:budget|spending|saving|progress)\s+(?:for|next|over|toward))/i.test(q) ||
      /(?:prioritize\s+(?:my\s+)?(?:financial\s+)?(?:goals?|saving|debt))/i.test(q) ||
      /(?:automate\s+(?:my\s+)?savings?)/i.test(q) ||
      /(?:small\s+changes\s+can\s+i\s+make)/i.test(q) ||
      /(?:build\s+(?:a\s+)?budget)/i.test(q)) {
    return 'BUDGET_INSIGHT';
  }

  // ═══ TIER 3: Savings — before individual income/expense ══════════════
  if (/(?:(?:my|total|net|overall)\s+)?savings?\b/i.test(q) ||
      /(?:how\s*much)\s*(?:did\s*i|have\s*i|i)\s*(?:save|saved)/i.test(q) ||
      /(?:am\s*i)\s*(?:saving|in\s*profit|in\s*loss|profitable)/i.test(q) ||
      /(?:net|total)\s*saving/i.test(q) ||
      /(?:money\s+(?:left|remaining|saved))/i.test(q)) {
    // Savings TREND (month-by-month growth)
    if (/(?:grown|grow|trend|progress|over\s+(?:the\s+)?(?:past|last)|history|month.?by.?month|how\s+much\s+.{0,20}saved)/i.test(q)) {
      return 'SAVINGS_TREND';
    }
    return 'TOTAL_SAVINGS';
  }

  // ═══ TIER 4: Net Worth ═══════════════════════════════════════════════
  if (/net\s*worth/i.test(q) ||
      /(?:what\s*(?:is|do)\s*i\s*(?:own|owe))/i.test(q) ||
      /(?:total\s*)?(?:wealth|net\s*value|financial\s*position)/i.test(q)) {
    return 'NET_WORTH';
  }

  // ═══ TIER 5: Assets / Liabilities — specific ════════════════════════
  // NOTE: \bemi\b has word boundaries — prevents "academic" matching "emi"
  if (/(?:my\s*)?(?:debt|debts|liabilit(?:y|ies)|loans?\b|\bemi\b|borrowed|owe|outstanding)/i.test(q) &&
      !/(?:what\s*is|define|meaning)/i.test(q) &&
      !/(?:expense|spend|paid|payment|category)/i.test(q)) {
    return 'LIABILITIES_ONLY';
  }
  if (/(?:my\s*)?(?:assets?)\b/i.test(q) &&
      !/(?:what\s*is|define|meaning)/i.test(q)) {
    return 'ASSETS_ONLY';
  }

  // ═══ TIER 5.5: Specific category spend — MUST be before Total Income/Expense ══
  // "how much did i spend in subscriptions" MUST hit here, NOT Total Expense.
  {
    const earlyMatch =
      // A) [spend verb] + [preposition] + [category]: "spent in subscriptions"
      q.match(/(?:spend(?:ing)?|spendings?|spent|expense|expenditure|paid|pay)\s+(?:on|in|for|at|towards?)\s+([a-z][a-z\s&\/,'\-]{1,40})/i) ||
      // B) how much did i spend on/in [category]
      q.match(/(?:how\s+much)\s+(?:did\s+i|have\s+i|i)\s+(?:spend|spent|paid|pay)\s+(?:on|in|for|at|towards?)\s+([a-z][a-z\s&\/,'\-]{1,40})/i) ||
      q.match(/(?:how\s+much)\s+(?:do\s+i\s+)?(?:spend|spent|pay|paid)\s+(?:on|for|in|at|towards?)\s+([a-z][a-z\s&\/,'\-]{1,40})/i) ||
      // C) what are my expenses in [category]
      q.match(/(?:what\s+(?:are|is)\s+)?(?:my\s+)?(?:spendings?|spend(?:ing)?|expenses?)\s+(?:in|on|for|at|towards?)\s+([a-z][a-z\s&\/,'\-]{1,40})/i) ||
      // D) [category] + expense/spending/bills: "academic expenses", "healthcare bills"
      q.match(/([a-z][a-z\s&\/,'\-]{1,30})\s+(?:expense|spend(?:ing)?|spendings?|expenditure|bills?|costs?|charges?|payment)/i) ||
      // E) my [total] [category]: "my total Refunds & Cashbacks", "my food costs"
      q.match(/(?:my|show(?:\s+me)?|get|list|what(?:'s|\s+are)?(?:\s+my)?)\s+(?:total\s+|all\s+|overall\s+)?([a-z][a-z\s&\/,'\-]{3,50})$/i);

    if (earlyMatch) {
      let extracted = (earlyMatch[1] || earlyMatch[2] || '').trim().toLowerCase()
        .replace(/\b(this|last|in|for|of|overall|total|all|my|past|20\d{2}|month|year|week|today|current)\b.*$/i, '')
        .replace(/\b(expense|expenses|spending|spend|spendings|bills?|costs?|charges?|payment|payments)\b/ig, '')
        .replace(/\s+/g, ' ').trim();
      const words = extracted.split(/\s+/).filter(Boolean);
      const allMeta = words.length > 0 && words.every(w => FINANCIAL_META_WORDS.has(w));
      if (extracted.length > 1 && !allMeta) {
        return 'SPECIFIC_CATEGORY_SPEND';
      }
    }
  }

  // ═══ TIER 6: Total Income ════════════════════════════════════════════════════

  if (/(?:total|overall|cumulative|my\s+total?|all\s+my)\s*(?:income|earning|inflow|revenue|salary)/i.test(q) ||
      /(?:how\s*much)\s*(?:(?:did\s+i|have\s+i|i)\s+)?(?:earn(?:ed)?|received?|got|made)/i.test(q) ||
      /(?:what|whats|what's)\s*(?:is\s*)?(?:my\s*)(?:income|earning|salary|revenue)/i.test(q) ||
      /(?:income|earning|salary|revenue)\s*(?:this|last|in|for|of|till)/i.test(q)) {
    return 'TOTAL_INCOME';
  }

  // ═══ TIER 7: Total Expense ═══════════════════════════════════════════
  // Guard on the "how much" line: if followed by preposition+category, TIER 5.5 handles it.
  if (/(?:total|overall|cumulative|my\s+total?|all\s+my)\s*(?:expense|spend(?:ing)?|outflow|payment|expenditure)/i.test(q) ||
      /(?:what|whats|what's)\s*(?:is\s*)?(?:my\s*)(?:total\s*)?(?:expense|spend(?:ing)?|expenditure)/i.test(q) ||
      (/(?:how\s*much)\s*(?:(?:did\s+i|have\s+i|i)\s+)?(?:spend|spent|paid|pay(?:ed)?)/i.test(q) &&
       !/(?:spend|spent|paid|pay(?:ed)?)\s+(?:on|in|for|at|towards?)\s+\S/i.test(q))) {
    return 'TOTAL_EXPENSE';
  }


  // ═══ TIER 8: Top spending categories ════════════════════════════════
  if (/(?:highest|maximum|max|top|biggest|largest|most)\s*(?:spend(?:ing)?|expense|expenditure|category)/i.test(q) ||
      /(?:spend|spent)\s*(?:the\s+)?(?:most|maximum|highest)/i.test(q) ||
      /(?:where|which\s*(?:category|categories))\s*(?:did|do|am|is)\s*(?:i|my)\s*(?:spend|spending|spent)/i.test(q) ||
      /top\s*\d*\s*(?:categor|expense|spending)/i.test(q) ||
      /(?:category|categor(?:y|ies))\s*(?:wise|breakdown|split|distribution)/i.test(q) ||
      /(?:all|every|each)\s*(?:categor|expense|spend)/i.test(q) ||
      /(?:summary|breakdown)\s*(?:of\s*)?(?:my\s*)?(?:spending|expenses)/i.test(q)) {
    return 'TOP_SPENDING_CATEGORY';
  }

  // ═══ TIER 9: Specific single transaction extremes ════════════════════
  if (/(?:largest|biggest|highest|max(?:imum)?)\s*(?:single\s*)?(?:credit|income|inflow|earning|receipt|money\s+received)/i.test(q)) {
    return 'MAX_CREDIT';
  }
  if (/(?:minimum|min|smallest|lowest|least)\s*(?:transaction|spend(?:ing)?|expense|amount|debit|payment)/i.test(q)) {
    return 'MIN_TRANSACTION';
  }
  if (/(?:largest|biggest|highest|max(?:imum)?)\s*(?:single\s*)?(?:transaction|txn|debit|payment|expense|transfer|bill|purchase)/i.test(q) ||
      /(?:most\s+expensive|costliest|priciest)\s*(?:transaction|purchase|item|bill|payment)/i.test(q)) {
    return 'MAX_TRANSACTION';
  }

  // ═══ TIER 10: Average / Count ═══════════════════════════════════════
  if (/(?:average|avg|mean|per\s+transaction)\s*(?:transaction|spend(?:ing)?|expense|amount|debit|payment)/i.test(q) ||
      /(?:transaction|spend(?:ing)?|expense)\s*(?:average|avg|mean)/i.test(q)) {
    return 'AVG_TRANSACTION';
  }
  if (/(?:total|how\s*many|count|number\s*of)\s*(?:transactions?|txn|debits?|credits?)/i.test(q) ||
      /(?:transaction|txn)\s*(?:count|total|number|volume)/i.test(q)) {
    return 'TRANSACTION_COUNT';
  }

  // ═══ TIER 11: Date-period summaries ══════════════════════════════════
  if (/(?:yearly|annual)\s*(?:summary|breakdown|report|overview|total|expense|spend|income)/i.test(q) ||
      /(?:summary|breakdown|report|overview|total)\s*(?:for\s*)?(?:the\s*)?(?:year|annual)/i.test(q) ||
      /(?:year\s*on\s*year|yoy|year\s*by\s*year)/i.test(q)) {
    return 'YEARLY_SUMMARY';
  }
  if (/(?:monthly|month\s*(?:on|by)\s*month|mom|month-by-month)\s*(?:spend(?:ing)?|expense|expenses|summary|breakdown|trend|report)/i.test(q) ||
      /(?:spent|spend|income|expense(?:s)?)\s*(?:this|last|every|each|per)\s*month/i.test(q) ||
      /last\s*month\b/i.test(q)) {
    return 'MONTHLY_SUMMARY';
  }

  // ═══ TIER 12: Recent transactions ════════════════════════════════════
  if (/(?:recent|latest|last\s*\d+|latest\s*\d+)\s*(?:transactions?|txn|payments?|purchases?|expenses?|debits?|credits?)/i.test(q) ||
      /(?:show|list|get)\s*(?:my\s*)?(?:last\s*\d+|recent|latest)\s*(?:transactions?|txn)/i.test(q)) {
    return 'RECENT_TRANSACTIONS';
  }

  // ═══ TIER 13: Specific category spend ════════════════════════════════
  // Catches any phrasing like:
  //   "spendings in food", "what are my expenses in healthcare",
  //   "how much on dining", "food expense", "spend on travel"
  let catMatch =
    q.match(/(?:spend(?:ing)?|spendings?|spent|expense|expenditure|paid|pay)\s+(?:on|in|for|at|towards?)\s+([a-z][a-z\s&/,'-]{1,40})/i) ||
    q.match(/(?:what\s+(?:are|is)\s+)?(?:my\s+)?(?:spendings?|spend(?:ing)?|expenses?)\s+(?:in|on|for|at|towards?)\s+([a-z][a-z\s&/,'-]{1,40})/i) ||
    q.match(/(?:on|in|for|at|towards?)\s+([a-z][a-z\s&/,'-]{1,30})\s+(?:spend(?:ing)?|spendings?|expense|expenditure|payment)/i) ||
    q.match(/([a-z][a-z\s&/,'-]{1,30})\s+(?:expense|spend(?:ing)?|spendings?|expenditure|bill|cost|payment)/i) ||
    q.match(/(?:expense|spend(?:ing)?|spendings?)\s+(?:of|in|on)\s+([a-z][a-z\s&/,'-]{1,30})/i) ||
    q.match(/(?:how\s+much)\s+(?:do\s+i\s+)?(?:spend|spent|pay|paid)\s+(?:on|for|in)\s+([a-z][a-z\s&/,'-]{1,40})/i);

  if (catMatch) {
    let extracted = (catMatch[1] || catMatch[2] || '').trim().toLowerCase();
    // Strip trailing date/time noise
    extracted = extracted.replace(/\b(this|last|in|for|of|overall|total|all|my|past|20\d{2}|month|year|week|today)\b.*$/i, '').trim();
    const words = extracted.split(/\s+/).filter(Boolean);
    const allMeta = words.length > 0 && words.every(w => FINANCIAL_META_WORDS.has(w));
    if (extracted.length > 1 && !allMeta) {
      return 'SPECIFIC_CATEGORY_SPEND';
    }
  }

  // ═══ TIER 14: Balance / Overview ══════════════════════════════════════
  if (/(?:balance|balances)\b/i.test(q) ||
      /(?:financial|money|finance)\s*(?:overview|summary|snapshot|status|position|health|report)/i.test(q) ||
      /(?:how\s*much)\s*(?:do\s*i\s*have|money\s+do\s*i|savings?\s+do\s*i)/i.test(q) ||
      /(?:show|give|tell)\s*(?:me\s*)?(?:my\s*)?(?:overview|financial\s+summary)/i.test(q) ||
      /\boverview\b/i.test(q)) {
    return 'BALANCE_OVERVIEW';
  }

  // ═══ TIER 15: SEMANTIC INTENT SCORING ════════════════════════════════
  // When regex tiers miss, score the query against intent profiles.
  // This handles natural/informal phrasings that exact patterns can't catch.
  const semanticIntent = semanticIntentScore(q);
  if (semanticIntent) {
    logger.info('StatAgent → SEMANTIC match', { intent: semanticIntent, query: q.slice(0,60) });
    return semanticIntent;
  }

  // ═══ TIER 16: UNIVERSAL_QUERY — final catch-all ════════════════════════
  return 'UNIVERSAL_QUERY';
}

// ════════════════════════════════════════════════════════════════════════
// SEMANTIC INTENT SCORING ENGINE
//
// How it works:
//  1. Normalize & tokenize the query into meaningful word tokens
//  2. Each intent has: keyword tokens (weighted), synonym groups, phrase signals
//  3. Score = sum of weights for matched tokens + phrase bonuses
//  4. Winner must exceed MIN_THRESHOLD to avoid false positives
//  5. Returns the highest-scoring intent, or null (falls to UNIVERSAL_QUERY)
//
// This enables understanding of informal / creative phrasings like:
//   "where is my money going"     → TOP_SPENDING_CATEGORY
//   "am I bleeding cash"          → TOTAL_SAVINGS
//   "what eats my wallet"         → TOP_SPENDING_CATEGORY
//   "track my finances this month"→ MONTHLY_SUMMARY
//   "show the damage this month"  → TOTAL_EXPENSE
//   "am I on track financially"   → BALANCE_OVERVIEW
// ════════════════════════════════════════════════════════════════════════

const SEMANTIC_INTENT_PROFILES = {

  TOTAL_SAVINGS: {
    // Core intent: net = income - expense, am I saving?
    tokens: {
      // Strong signals (weight 2)
      'save': 2, 'saved': 2, 'saving': 2, 'savings': 2, 'surplus': 2,
      'leftover': 2, 'left': 1.5, 'remaining': 1.5, 'net': 1.5,
      // Medium signals (weight 1)
      'profit': 1, 'break': 1, 'even': 1, 'excess': 1, 'retain': 1,
      'keepable': 1, 'pocket': 1, 'afford': 1,
      // Informal / metaphorical
      'bleeding': 1.5, 'burn': 1.5, 'burning': 1.5, 'drain': 1.5,
      'draining': 1.5, 'losing': 1, 'broke': 1.5, 'negative': 1,
    },
    phrases: [
      'how much left', 'money left', 'what i saved', 'am i saving',
      'do i save', 'save enough', 'money remaining', 'cash left',
      'income minus', 'after expenses', 'broke this month', 'in the red',
      'in the green', 'bleeding money', 'burning through', 'am i broke',
      'financial health', 'money situation',
    ],
    threshold: 2.5,
  },

  SAVINGS_TREND: {
    tokens: {
      'grown': 3, 'grow': 2, 'growth': 3, 'trend': 2.5, 'progress': 2,
      'trajectory': 2, 'history': 1.5, 'track': 1.5, 'over': 1,
      'months': 1.5, 'monthly': 1.5, 'improving': 2, 'worsening': 2,
      'better': 1, 'worse': 1, 'changed': 1.5, 'compare': 1.5,
    },
    phrases: [
      'savings grown', 'how savings', 'over the past', 'over last',
      'month by month', 'over time', 'savings history', 'financial progress',
      'savings progress', 'getting better', 'getting worse', 'savings trend',
      'compared to last', 'vs last month',
    ],
    threshold: 3,
  },

  BUDGET_INSIGHT: {
    tokens: {
      'budget': 3, 'allocate': 2.5, 'allocation': 2.5, 'plan': 1.5,
      'limit': 2, 'cap': 1.5, 'overspend': 3, 'overspending': 3,
      'underspend': 2, 'target': 1.5, 'planned': 2, 'actual': 1.5,
      'reduce': 1.5, 'cut': 1.5, 'control': 1.5, 'manage': 1.5,
      'distribute': 1.5, 'divide': 1.5, 'split': 1.5, 'proportion': 1.5,
      'ideal': 1.5, 'recommended': 1.5, 'suggest': 1.5, 'guideline': 1.5,
      'rule': 1, 'framework': 1, 'strategy': 1,
    },
    phrases: [
      'how much should i', 'should i allocate', 'how to budget',
      'set a budget', 'create a budget', 'budget breakdown',
      'budget for', 'where should i', 'how do i distribute',
      'ideal budget', 'recommended budget', '50 30 20', '50/30/20',
      'needs wants savings', 'stay within budget', 'within my budget',
      'budget categories', 'categories to cut', 'categories to reduce',
      'categories i should reduce', 'reduce by category',
      'overspending in', 'spending too much on',
    ],
    threshold: 2.5,
  },

  TOP_SPENDING_CATEGORY: {
    tokens: {
      'top': 2, 'highest': 2, 'biggest': 2, 'largest': 2, 'most': 2,
      'categories': 2, 'category': 2, 'where': 1.5, 'which': 1,
      'breakdown': 2, 'distribution': 2, 'split': 1.5, 'going': 1.5,
      'spent': 1.5, 'spending': 1.5,
      // Informal
      'eating': 2, 'draining': 1.5, 'killing': 1.5, 'leaking': 2,
      'sucking': 1.5, 'hemorrhaging': 2, 'burning': 1.5, 'guzzling': 2,
      'wallet': 1.5, 'pocket': 1, 'money': 1,
    },
    phrases: [
      'where is my money going', 'where does my money go', 'what eats my',
      'what\'s eating my', 'biggest expense', 'most spent on',
      'category breakdown', 'spending breakdown', 'expense breakdown',
      'where am i spending', 'where do i spend', 'money going to',
      'most money on', 'highest spending', 'top categories',
      'what drains', 'what kills', 'what leaks', 'where leaking',
      'show me spending', 'expense distribution',
    ],
    threshold: 2.5,
  },

  TOTAL_EXPENSE: {
    tokens: {
      'spent': 2.5, 'spend': 2, 'spending': 2, 'expense': 2.5,
      'expenses': 2.5, 'outflow': 2, 'paid': 2, 'debit': 2,
      'outgoing': 2, 'cost': 1.5, 'costs': 1.5, 'bills': 1.5,
      'total': 1.5, 'overall': 1.5, 'cumulative': 1.5,
      // Informal
      'damage': 2, 'tab': 2, 'bill': 1.5, 'dent': 1.5,
    },
    phrases: [
      'total spending', 'total expenses', 'how much spent', 'what i spent',
      'amount spent', 'total paid', 'total debit', 'how much did i spend',
      'show the damage', 'whats the damage', 'what\'s the tab',
      'total outflow', 'expenditure this month', 'money spent',
    ],
    threshold: 2.5,
  },

  TOTAL_INCOME: {
    tokens: {
      'income': 3, 'earned': 2.5, 'earning': 2, 'earnings': 2,
      'received': 2, 'salary': 2.5, 'revenue': 2, 'inflow': 2,
      'credit': 1.5, 'credited': 2, 'made': 1.5, 'got': 1,
      'received': 2, 'collected': 1.5,
      // Informal
      'paycheck': 2, 'payslip': 2, 'bring': 1.5, 'brought': 1.5,
      'rake': 1.5, 'raking': 1.5,
    },
    phrases: [
      'total income', 'how much earned', 'what i earned', 'total earnings',
      'how much received', 'salary this month', 'money earned',
      'money received', 'income this month', 'what came in',
      'how much came in', 'total inflow', 'what i made',
    ],
    threshold: 2.5,
  },

  TOTAL_SAVINGS: {
    tokens: {
      'save': 2, 'saved': 2, 'saving': 2, 'savings': 2, 'surplus': 2,
      'net': 1.5, 'left': 1.5, 'remaining': 1.5, 'leftover': 2,
      'profit': 1, 'pocket': 1, 'retain': 1, 'keepable': 1,
    },
    phrases: [
      'how much left', 'money left', 'what i saved', 'am i saving',
      'net savings', 'total savings', 'save this month',
    ],
    threshold: 2.5,
  },

  MONTHLY_SUMMARY: {
    tokens: {
      'monthly': 2.5, 'month': 2, 'months': 1.5, 'monthly': 2,
      'recap': 2.5, 'summary': 2, 'report': 2, 'overview': 1.5,
      'finances': 1.5, 'financial': 1.5, 'track': 1.5, 'review': 1.5,
      // "this month" / "last month"
      'this': 1, 'last': 1,
    },
    phrases: [
      'this month summary', 'monthly recap', 'month review', 'monthly report',
      'track my finances', 'financial recap', 'month overview',
      'how did i do this month', 'how was last month', 'month breakdown',
      'spending this month', 'income this month', 'net this month',
      'how am i doing', 'how did i do', 'financial summary this month',
    ],
    threshold: 3,
  },

  INCOME_VS_EXPENSE: {
    tokens: {
      'vs': 2.5, 'versus': 2.5, 'compare': 2.5, 'comparison': 2.5,
      'against': 2, 'ratio': 2, 'proportion': 1.5,
      'income': 1.5, 'expense': 1.5, 'in': 1, 'out': 1,
      'inflow': 1.5, 'outflow': 1.5,
    },
    phrases: [
      'income vs expense', 'income versus expense', 'income and expense',
      'compare income', 'in vs out', 'money in money out', 'income to expense',
      'how much in how much out', 'inflow outflow', 'income against expense',
    ],
    threshold: 3,
  },

  NET_WORTH: {
    tokens: {
      'worth': 3, 'wealth': 3, 'networth': 3, 'assets': 2, 'liabilities': 2,
      'own': 2, 'owe': 2, 'rich': 1.5, 'value': 1.5, 'financial': 1.5,
      // Informal
      'richer': 2, 'poorer': 2, 'wealthy': 2, 'broke': 1.5,
    },
    phrases: [
      'net worth', 'total worth', 'how much am i worth', 'what do i own',
      'what do i owe', 'assets and liabilities', 'my wealth',
      'how wealthy am i', 'assets vs liabilities', 'financial value',
      'am i rich', 'total financial position',
    ],
    threshold: 2.5,
  },

  RECENT_TRANSACTIONS: {
    tokens: {
      'recent': 3, 'latest': 3, 'last': 1.5, 'show': 1, 'list': 1,
      'transactions': 2, 'payments': 2, 'purchases': 2, 'history': 2,
      'activity': 2, 'log': 1.5, 'entries': 1.5,
      // Informal
      'happened': 1.5, 'done': 1, 'made': 1, 'recently': 2,
    },
    phrases: [
      'recent transactions', 'latest transactions', 'last transactions',
      'transaction history', 'show transactions', 'list transactions',
      'what happened recently', 'payment history', 'recent activity',
      'last 5 transactions', 'last 10 transactions', 'recent payments',
      'what did i recently', 'my history',
    ],
    threshold: 2.5,
  },

  MAX_TRANSACTION: {
    tokens: {
      'biggest': 3, 'largest': 3, 'highest': 2.5, 'maximum': 2.5, 'max': 2,
      'most': 2, 'expensive': 2.5, 'costly': 2, 'costliest': 3,
      'single': 1.5, 'one': 1, 'payment': 1.5, 'purchase': 1.5,
      // Informal
      'mega': 2, 'huge': 2, 'massive': 2, 'giant': 2, 'whopper': 2,
    },
    phrases: [
      'biggest transaction', 'largest transaction', 'maximum transaction',
      'most expensive purchase', 'single biggest', 'biggest expense',
      'biggest payment', 'highest payment', 'largest debit',
      'biggest bill', 'most i spent on', 'what cost the most',
    ],
    threshold: 2.5,
  },

  SPECIFIC_CATEGORY_SPEND: {
    // Semantic fallback for named-category queries not caught by regex patterns.
    // Strategy: category keyword (weight 2-3) + spending context (weight 1) = threshold met.
    // This means "my grocery bills" scores: grocery(2) + bills(1) = 3 → hits threshold.
    tokens: {
      // ── Food & Dining ─────────────────────────────────────────────────
      'food': 3, 'groceries': 3, 'grocery': 3, 'dining': 3, 'restaurant': 3,
      'meal': 2, 'meals': 2, 'eating': 2, 'cafe': 2, 'canteen': 2,
      'swiggy': 3, 'zomato': 3, 'delivery': 2, 'takeout': 2, 'takeaway': 2,
      // ── Housing ───────────────────────────────────────────────────────
      'rent': 3, 'housing': 3, 'accommodation': 3, 'flat': 2, 'apartment': 2,
      'landlord': 2, 'lease': 2, 'maintenance': 2,
      // ── Transport ─────────────────────────────────────────────────────
      'transport': 3, 'travel': 2, 'fuel': 3, 'petrol': 3, 'diesel': 3,
      'cab': 3, 'taxi': 3, 'uber': 3, 'ola': 3, 'commute': 2, 'bus': 2,
      'metro': 2, 'auto': 2, 'vehicle': 2, 'parking': 2, 'toll': 2,
      // ── Entertainment ─────────────────────────────────────────────────
      'entertainment': 3, 'movies': 2, 'movie': 2, 'outing': 2, 'event': 2,
      'gaming': 2, 'games': 2, 'sports': 2, 'leisure': 2,
      // ── Subscriptions / OTT ───────────────────────────────────────────
      'subscription': 3, 'subscriptions': 3, 'netflix': 3, 'prime': 2,
      'spotify': 3, 'hotstar': 3, 'youtube': 2, 'ott': 3, 'streaming': 2,
      'membership': 2,
      // ── Shopping ──────────────────────────────────────────────────────
      'shopping': 3, 'clothes': 3, 'clothing': 3, 'fashion': 2, 'apparel': 2,
      'amazon': 2, 'flipkart': 2, 'myntra': 3, 'online': 1.5, 'ecommerce': 2,
      // ── Health & Medical ──────────────────────────────────────────────
      'health': 2.5, 'medical': 3, 'medicine': 3, 'hospital': 3, 'pharmacy': 3,
      'doctor': 3, 'clinic': 3, 'healthcare': 3, 'checkup': 2, 'therapy': 2,
      // ── Education ─────────────────────────────────────────────────────
      'education': 3, 'school': 3, 'college': 3, 'fees': 2.5, 'tuition': 3,
      'course': 2, 'learning': 2, 'books': 2, 'stationery': 2, 'exam': 2,
      // ── Utilities ─────────────────────────────────────────────────────
      'utilities': 3, 'electricity': 3, 'internet': 3, 'wifi': 3, 'broadband': 3,
      'water': 2, 'gas': 2, 'phone': 2.5, 'mobile': 2.5, 'recharge': 2.5,
      'telecom': 2, 'bill': 1.5, 'bills': 1.5,
      // ── Finance & Banking ─────────────────────────────────────────────
      'insurance': 3, 'emi': 3, 'loan': 2.5, 'premium': 2, 'investment': 2,
      'interest': 2, 'credit': 1.5,
      // ── Fitness ───────────────────────────────────────────────────────
      'gym': 3, 'fitness': 3, 'yoga': 3, 'pilates': 2,
      // ── Personal Care ─────────────────────────────────────────────────
      'salon': 3, 'grooming': 3, 'haircut': 3, 'beauty': 2, 'spa': 2,
      // ── Refunds / Cashbacks / Credits ─────────────────────────────────
      'refund': 3, 'refunds': 3, 'cashback': 3, 'cashbacks': 3,
      'reimbursement': 3, 'reimbursements': 3, 'reversal': 3, 'reversals': 3,
      'credit': 2, 'credits': 2, 'reward': 2, 'rewards': 2,
      // ── Donations / Gifts ─────────────────────────────────────────────
      'donation': 3, 'donations': 3, 'charity': 3, 'gift': 2.5, 'gifts': 2.5,
      'tithe': 2, 'contribution': 2,
      // ── Academic / Education ───────────────────────────────────────────
      'academic': 3, 'academics': 3, 'tuition': 3, 'tutorial': 2,
      'coaching': 3, 'training': 2, 'workshop': 2, 'seminar': 2,
      // ── Living / Lifestyle ────────────────────────────────────────────
      'living': 2.5, 'lifestyle': 2, 'household': 2.5, 'domestic': 2,
      'daily': 1.5, 'essentials': 2, 'necessities': 2,
      // ── Spending context signals (lower weight, need category to trigger) ──
      'spending': 1, 'spent': 1, 'expense': 1, 'expenses': 1, 'paid': 1,
      'cost': 1, 'costs': 1, 'charges': 1, 'payment': 1, 'total': 0.5,

    },
    phrases: [
      // Informal named-category phrasings
      'tell me about subscriptions', 'my grocery bills', 'rent situation',
      'food expenses', 'transport costs', 'medical bills', 'school fees',
      'phone bill', 'electricity bill', 'internet bill', 'utility bills',
      'gym fees', 'gym membership', 'ott subscription', 'streaming subscription',
      'fuel costs', 'petrol expenses', 'cab spending', 'taxi expenses',
      'shopping expenses', 'clothes spending', 'dining expenses', 'restaurant bills',
      'insurance premium', 'emi payment', 'tuition fees', 'doctor visits',
    ],
    threshold: 2.5,  // category keyword alone (score 3) triggers this
  },


  BALANCE_OVERVIEW: {
    tokens: {
      'overview': 3, 'snapshot': 3, 'position': 2.5, 'status': 2.5,
      'report': 2, 'summary': 2, 'dashboard': 2, 'picture': 2,
      'health': 2.5, 'shape': 2, 'state': 2, 'situation': 2,
      'overall': 2, 'complete': 1.5, 'full': 1.5, 'total': 1.5,
      // Informal
      'doing': 1.5, 'standing': 2, 'look': 1.5, 'checking': 1.5,
    },
    phrases: [
      'financial overview', 'financial snapshot', 'financial position',
      'how am i doing', 'financial health', 'financial status',
      'overall status', 'complete picture', 'full picture',
      'financial report', 'financial situation', 'where do i stand',
      'how am i financially', 'financial shape', 'am i on track',
      'check my finances', 'financial standing',
    ],
    threshold: 2.5,
  },
};

/**
 * Semantic intent scorer.
 *
 * Tokenizes the query and scores it against each intent profile's
 * keyword weights and phrase signals. Returns the best intent
 * above the profile's threshold, or null if no confident match.
 *
 * Time complexity: O(intents × tokens) — typically <1ms.
 */
function semanticIntentScore(query) {
  // Normalize: lowercase, collapse whitespace, remove punctuation
  const normalized = query.toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  // Stop words (reduce noise)
  const STOP = new Set([
    'a', 'an', 'the', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
    'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
    'should', 'may', 'might', 'shall', 'can', 'that', 'this', 'these',
    'those', 'it', 'its', 'of', 'to', 'in', 'for', 'on', 'with', 'at',
    'by', 'from', 'up', 'about', 'into', 'through', 'during', 'and',
    'or', 'not', 'but', 'so', 'than', 'such', 'both', 'too',
    'very', 'just', 'also', 'like', 'over', 'like', 'out',
    // financial noise words that are too generic to disambiguate
    'money', 'financial', 'finance', 'rupee', 'amount', 'number',
  ]);

  // Tokenize — keep only meaningful words
  const queryTokens = normalized.split(' ').filter(t => t.length > 1 && !STOP.has(t));

  let bestIntent = null, bestScore = 0;

  for (const [intent, profile] of Object.entries(SEMANTIC_INTENT_PROFILES)) {
    let score = 0;

    // 1. Token weight matching
    for (const token of queryTokens) {
      if (profile.tokens[token]) {
        score += profile.tokens[token];
        continue;
      }
      // Partial stem match (e.g., "spending" → "spend", "earnings" → "earn")
      for (const [kw, wt] of Object.entries(profile.tokens)) {
        if (kw.length >= 4 && (token.startsWith(kw) || kw.startsWith(token))) {
          score += wt * 0.7; // partial match gets 70% weight
          break;
        }
      }
    }

    // 2. Phrase bonuses (high-confidence signals)
    for (const phrase of profile.phrases) {
      if (normalized.includes(phrase)) {
        score += 3; // phrase match is a strong signal
      }
    }

    // Track best
    if (score >= profile.threshold && score > bestScore) {
      bestScore = score;
      bestIntent = intent;
    }
  }

  return bestIntent; // null if nothing confident
}

module.exports = { classifyQuery, detectStatisticalIntent };
