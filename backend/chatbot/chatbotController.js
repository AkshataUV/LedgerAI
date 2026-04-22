/**
 * Chatbot Controller — Orchestrates the Agentic Router Pattern
 * 
 * Flow:
 *   User Message → InsightRouter.classify() → 
 *     STATISTICAL → StatisticalAgent (DB query)
 *     LLM_REALTIME → LLMAgent (AI call)
 *   → Save to DB → Return response
 */

const supabase = require('../config/supabaseClient');
const logger = require('../utils/logger');
const { classifyQuery, detectStatisticalIntent } = require('./insightRouter');
const { handleStatisticalQuery } = require('./statisticalAgent');
const { handleLLMQuery } = require('./llmAgent');

/**
 * POST /api/chatbot/message
 * Main message handler using the Agentic Router pattern
 */
const handleInsightMessage = async (req, res) => {
  try {
    const { message } = req.body;
    const userId = req.user.id;

    if (!message || !message.trim()) {
      return res.status(400).json({ error: 'Message is required' });
    }

    const startTime = Date.now();

    // ─── Step 1: Manage Chat Session ────────────────────────────
    let { data: session } = await supabase
      .from('ai_chat_sessions')
      .select('session_id')
      .eq('user_id', userId)
      .order('started_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (!session) {
      const { data: newSession, error: createErr } = await supabase
        .from('ai_chat_sessions')
        .insert([{ user_id: userId }])
        .select()
        .single();
      if (createErr) throw createErr;
      session = newSession;
    }

    // ─── Step 2: Save User Message ──────────────────────────────
    await supabase.from('ai_chat_messages').insert([{
      session_id: session.session_id,
      sender: 'user',
      message_text: message
    }]);

    // ─── Step 2.5: Fetch Chat History for LLM Context ───────────
    const { data: rawHistory } = await supabase
      .from('ai_chat_messages')
      .select('sender, message_text')
      .eq('session_id', session.session_id)
      .order('created_at', { ascending: true })
      .limit(10);

    // ─── Step 3: Agentic Router — Classify the query ────────────
    const classification = classifyQuery(message);
    let responseText = '';
    let routingInfo = {};

    if (classification.lane === 'OUT_OF_SCOPE') {
      // ─── OUT OF SCOPE: Instant rejection — no LLM, no DB ───────
      responseText = `🚫 I'm **LedgerBuddy**, a specialized financial assistant. I can only help with:\n\n  💰 Your personal finances & transactions\n  📊 Income, expenses, savings & budgets\n  🏦 Banking, tax, and investment queries\n\nI'm not equipped to answer questions outside of finance and banking. Try asking:\n  • _"What's my top spending category?"_\n  • _"Tax saving tips"_\n  • _"Gold rate today"_`;
      routingInfo = {
        lane: 'OUT_OF_SCOPE',
        confidence: classification.confidence,
        latencyMs: Date.now() - startTime
      };
    } else if (classification.lane === 'STATISTICAL') {
      // ─── STATISTICAL LANE: Direct DB query ─────────────────
      const subIntent = detectStatisticalIntent(message);
      
      // Explicit rerouting for complex advice intents that need LLM + user data context
      if (subIntent === 'BUDGET_INSIGHT' || subIntent === 'SAVINGS_TREND') {
        logger.info('InsightRouter → Pre-fetching DB stats for LLM', { subIntent });
        
        let exactDbContext = null;
        try {
          // 1. Get the precise, mathematically verified numbers from the DB directly!
          const dbResult = await handleStatisticalQuery(subIntent, userId, message);
          exactDbContext = dbResult.text;
        } catch(err) {
          logger.warn('Could not fetch DB stats for LLM re-route. Proceeding with generic persona.', { error: err.message });
        }

        // 2. Feed the DB math directly to the Agent!
        const result = await handleLLMQuery(message, userId, rawHistory, exactDbContext);
        
        responseText = result.text;
        routingInfo = {
          lane: 'LLM_REALTIME',
          source: result.source,
          originalIntent: subIntent,
          confidence: classification.confidence,
          latencyMs: Date.now() - startTime
        };
      } else {
        const result = await handleStatisticalQuery(subIntent, userId, message);
        responseText = result.text;
        routingInfo = {
          lane: 'STATISTICAL',
          subIntent,
          confidence: classification.confidence,
          latencyMs: Date.now() - startTime
        };
      }
    } else {
      // ─── LLM LANE: AI-powered response ─────────────────────
      const result = await handleLLMQuery(message, userId, rawHistory);
      responseText = result.text;
      routingInfo = {
        lane: 'LLM_REALTIME',
        source: result.source,
        confidence: classification.confidence,
        latencyMs: Date.now() - startTime
      };
    }

    // ─── Step 4: Save Bot Response ───────────────────────────────
    await supabase.from('ai_chat_messages').insert([{
      session_id: session.session_id,
      sender: 'bot',
      message_text: responseText
    }]);

    logger.info('Chatbot response sent', {
      userId,
      lane: routingInfo.lane,
      latencyMs: routingInfo.latencyMs,
      confidence: routingInfo.confidence
    });

    return res.status(200).json({
      text: responseText,
      type: 'bot',
      routing: routingInfo
    });

  } catch (err) {
    logger.error('handleInsightMessage error:', { error: err.message, stack: err.stack });
    return res.status(500).json({ error: 'Failed to process your message. Please try again.' });
  }
};

/**
 * GET /api/chatbot/history
 * Fetch chat history for the current user
 */
const getInsightHistory = async (req, res) => {
  try {
    const userId = req.user.id;

    // Find latest session
    const { data: session } = await supabase
      .from('ai_chat_sessions')
      .select('session_id')
      .eq('user_id', userId)
      .order('started_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (!session) return res.status(200).json([]);

    const { data: messages, error } = await supabase
      .from('ai_chat_messages')
      .select('message_id, sender, message_text, created_at')
      .eq('session_id', session.session_id)
      .order('created_at', { ascending: true });

    if (error) throw error;

    return res.status(200).json(messages.map(m => ({
      id: m.message_id,
      type: m.sender,
      text: m.message_text,
      timestamp: m.created_at
    })));
  } catch (err) {
    logger.error('getInsightHistory error:', { error: err.message });
    return res.status(500).json({ error: 'Failed to fetch chat history' });
  }
};

/**
 * POST /api/chatbot/clear
 * Clear the current chat session (start fresh)
 */
const clearInsightHistory = async (req, res) => {
  try {
    const userId = req.user.id;

    // Soft-clear: create a new session. Old messages remain in DB for audit.
    const { data: newSession, error } = await supabase
      .from('ai_chat_sessions')
      .insert([{ user_id: userId }])
      .select()
      .single();

    if (error) throw error;

    return res.status(200).json({ success: true, sessionId: newSession.session_id });
  } catch (err) {
    logger.error('clearInsightHistory error:', { error: err.message });
    return res.status(500).json({ error: 'Failed to clear chat' });
  }
};

module.exports = {
  handleInsightMessage,
  getInsightHistory,
  clearInsightHistory
};
