/**
 * LLM Agent — Handles complex / real-time queries
 *
 * Uses its OWN dedicated Gemini config (CHATBOT_*) from .env
 * so it is completely isolated from the rest of the app's LLM setup.
 *
 * For queries that need:
 *   - Real-time data (gold rates, tax rules, market info)
 *   - Complex reasoning / anomaly detection
 *   - Financial advice / planning
 */

const supabase = require('../config/supabaseClient');
const logger = require('../utils/logger');
// require('dotenv').config();
require('dotenv').config();
// ─── Chatbot-specific config (won't affect any other service) ────────
let CHATBOT_PROVIDER = process.env.CHATBOT_LLM_PROVIDER || 'google';
const CHATBOT_API_KEY = process.env.CHATBOT_GEMINI_API_KEY;
const CHATBOT_MODEL = process.env.CHATBOT_LLM_MODEL || 'gemini-1.5-flash';

if (CHATBOT_MODEL.startsWith('openrouter/')) {
  CHATBOT_PROVIDER = 'openrouter';
}

const { getFinancialPersona } = require('./statisticalAgent');

const SYSTEM_PROMPT = `You are LedgerBuddy, an intelligent AI financial assistant built into LedgerAI — a personal finance management app.

Your CORE mission is to provide personalized, data-driven financial insights. 

**CRITICAL ACCURACY RULES (STRICTLY ENFORCED):**
- **NEVER HALLUCINATE PERSONAL DATA**: Base all personal insights strictly on the provided context. Do not invent, estimate, or assume expenses, incomes, or transactions that were not provided to you.
- **LIVE MARKET DATA LIMITATION**: Because you do not possess real-time internet access right this second, if asked for "today's" stats (like live stock prices or today's exact gold rate), you MUST provide a realistic recent estimate based on your training data (e.g., "Gold is currently trading around ₹X per 10 grams"), but you MUST always append this exact disclaimer: *"Please verify current market rates as my data may not be 100% real-time."* Do NOT refuse to answer the question. Give the best approximate response you can!
- Factual and mathematical precision regarding the user's ledger is your absolute highest priority. 

When user financial data is provided in the context:
1. **Analyze for tax savings**: If you see rent paid but few investments, suggest HRA and 80C options. If investments are low, remind about the ₹1.5L limit.
2. **Detect anomalies**: Mention if spending in a category is unusually high or if Recurring patterns look like unnecessary subscriptions.
3. **Budgeting & Savings**: When asked to create a budget, savings plan, or savings challenge, evaluate their current 'savingsRate', 'income', and 'topSpending'. Provide structured, realistic allocations (e.g., 50/30/20 rule) tailored to their actual numbers.
4. **Be Specific**: Don't just give general tips. Say "I notice you've spent ₹X on Y, try Z to save."
5. **Disclaimers**: Always conclude with: "_This is for informational purposes only. Consult a certified financial advisor or CA._"

General Responsibilities:
- Indian Tax rules, CA queries, and Banking regulations
- Budget planning, saving advice, and personalized financial challenges
- Explaining complex financial, asset, and investment concepts thoroughly

Formatting:
- Use emojis (📊💰🔥📈🏦)
- **Bold** important numbers and terms
- Concise, bullet-pointed lists
- Currency: ₹ with Indian comma format (₹1,00,000)`;

/**
 * Call Google Gemini directly using the chatbot-dedicated API key.
 * This does NOT use llmService.js or any shared config.
 */
async function callChatbotLLM(messages, temperature = 0.4) {
  logger.info('Chatbot LLM call', { provider: CHATBOT_PROVIDER, model: CHATBOT_MODEL, messageCount: messages.length });

  if (CHATBOT_PROVIDER === 'openrouter') {
    const OPENROUTER_KEY = process.env.OPENROUTER_API_KEY;
    if (!OPENROUTER_KEY) throw new Error('OPENROUTER_API_KEY is not configured in .env');

    const response = await fetch('https://openrouter.ai/api/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${OPENROUTER_KEY}`,
        'HTTP-Referer': 'http://localhost:5173',
        'X-Title': 'LedgerBuddy'
      },
      body: JSON.stringify({
        model: CHATBOT_MODEL,
        messages: messages,
        temperature
      })
    });

    if (!response.ok) {
      const errText = await response.text();
      throw new Error(`OpenRouter API error (${response.status}): ${errText}`);
    }

    const data = await response.json();
    const content = data.choices?.[0]?.message?.content?.trim();
    if (!content) throw new Error('Empty response from OpenRouter');
    return content;
    
  } else {
    // ─── Google Gemini Direct Pipeline ─────────────
    if (!CHATBOT_API_KEY || CHATBOT_API_KEY === 'YOUR_GEMINI_API_KEY_HERE') {
      throw new Error('CHATBOT_GEMINI_API_KEY is not configured in .env');
    }

    const url = `https://generativelanguage.googleapis.com/v1beta/models/${CHATBOT_MODEL}:generateContent?key=${CHATBOT_API_KEY}`;

    // Convert OpenAI-style messages → Google Gemini format
    const systemMsg = messages.find(m => m.role === 'system');
    const userMsgs = messages.filter(m => m.role !== 'system');

    const contents = userMsgs.map((m, idx) => {
      const text = idx === 0 && systemMsg
        ? `${systemMsg.content}\n\n${m.content}`
        : m.content;
      return {
        role: m.role === 'assistant' ? 'model' : 'user',
        parts: [{ text }]
      };
    });

    const body = {
      contents,
      generationConfig: {
        temperature,
        maxOutputTokens: 2048
      }
    };

    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });

    if (!response.ok) {
      const errText = await response.text();
      let errDetail = errText;
      try { errDetail = JSON.parse(errText)?.error?.message || errText; } catch { }
      throw new Error(`Gemini API error (${response.status}): ${errDetail}`);
    }

    const data = await response.json();
    const content = data.candidates?.[0]?.content?.parts?.[0]?.text?.trim();

    if (!content) throw new Error('Empty response from Gemini');
    return content;
  }
}

/**
 * @param {string} query  - User's message
 * @param {string} userId - The authenticated user's UUID
 * @param {Array} history - Chat history array from DB
 * @param {string|null} extraStatisticalContext - Hard database output passed from StatisticalAgent
 */
async function handleLLMQuery(query, userId, history = [], extraStatisticalContext = null) {
  logger.info('LLMAgent processing', { userId: userId?.slice(0, 8), queryLength: query.length, historyCount: history.length });

  // 1. Gather deep financial persona for context
  let financialContext = '';
  try {
    const persona = await getFinancialPersona(userId);
    if (persona) {
      financialContext = `\n\n[USER DATA CONTEXT - 90 DAYS]:\n${JSON.stringify(persona, null, 2)}`;
    }
    if (extraStatisticalContext) {
      financialContext += `\n\n[EXACT STATISTICAL DATABASE MATCH]:\nHere are the exact numbers calculated directly from the user's database. USE THESE NUMBERS FACTUALLY and build your advice around them:\n${extraStatisticalContext}`;
    }
  } catch (err) {
    logger.warn('Failed to fetch persona for LLM', { error: err.message });
  }

  const rawMessages = [];
  if (history && history.length > 0) {
    history.forEach(h => {
      rawMessages.push({ role: h.sender === 'user' ? 'user' : 'assistant', content: h.message_text });
    });
    if (rawMessages[rawMessages.length - 1].content !== query) {
      rawMessages.push({ role: 'user', content: query });
    }
  } else {
    rawMessages.push({ role: 'user', content: query });
  }

  const lastMsg = rawMessages[rawMessages.length - 1];
  if (lastMsg.role === 'user') {
    lastMsg.content = `${lastMsg.content}${financialContext}`;
  }

  const mergedUserMsgs = [];
  rawMessages.forEach(msg => {
    if (mergedUserMsgs.length > 0 && mergedUserMsgs[mergedUserMsgs.length - 1].role === msg.role) {
      mergedUserMsgs[mergedUserMsgs.length - 1].content += `\n\n${msg.content}`;
    } else {
      mergedUserMsgs.push({ role: msg.role, content: msg.content });
    }
  });

  const messages = [
    { role: 'system', content: SYSTEM_PROMPT },
    ...mergedUserMsgs
  ];

  try {
    const response = await callChatbotLLM(messages, 0.4);
    return { text: response, source: 'gemini-chatbot' };
  } catch (err) {
    logger.error('LLMAgent call failed', { error: err.message });

    return {
      text: `⚠️ I'm having trouble connecting to my AI engine right now.\n\nFor the question: "_${query}_"\n\nPlease try again in a moment, or ask me a data-related question (like "what's my top spending category?") which I can answer instantly from your data!`,
      source: 'fallback'
    };
  }
}

module.exports = { handleLLMQuery };
