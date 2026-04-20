const supabase = require('../config/supabaseClient');
const axios = require('axios');
const logger = require('../utils/logger');

const ML_SERVICE_URL = process.env.ML_SERVICE_URL || `http://127.0.0.1:${process.env.PYTHON_PORT || 5000}`;

const getChatHistory = async (req, res) => {
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
        logger.error('getChatHistory error:', err);
        return res.status(500).json({ error: 'Failed to fetch history' });
    }
};

/**
 * Handle user messages for LedgerBuddy
 */
const handleChatMessage = async (req, res) => {
    try {
        const { message } = req.body;
        const userId = req.user.id;

        if (!message) {
            return res.status(400).json({ error: 'Message is required' });
        }

        // 1. Manage Chat Session (Find active session or create new)
        let { data: session, error: sessionErr } = await supabase
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

        // 2. Save User Message to DB
        await supabase.from('ai_chat_messages').insert([{
            session_id: session.session_id,
            sender: 'user',
            message_text: message
        }]);

        // 3. Get Intent from AI
        let intentResponse;
        try {
            intentResponse = await axios.post(`${ML_SERVICE_URL}/chat/intent`, { 
                text: message,
                user_id: userId
            });
        } catch (err) {
            return res.status(200).json({ 
                text: "I'm having trouble connecting to my brain! Please try again.",
                type: 'bot'
            });
        }

        const { intent } = intentResponse.data;

        // 4. Fetch Context (Optional based on intent)
        let financialContext = "";
        if (['SPENDING_SUMMARY', 'ANOMALY_DETECTION', 'COMPARISON'].includes(intent)) {
             // Fetch standard RPC summary
             const { data: summary } = await supabase.rpc('get_user_spending_summary', { p_user_id: userId });
             
             // Fetch latest monthly narrative summary if available
             const { data: monthlySummary } = await supabase
                .from('ai_monthly_summaries')
                .select('summary_text')
                .eq('user_id', userId)
                .order('generated_at', { ascending: false })
                .limit(1)
                .maybeSingle();

             financialContext = JSON.stringify({
                stats: summary || {},
                narrative_history: monthlySummary?.summary_text || ""
             });
        }

        // 5. Generate Narrative
        const narrativeResponse = await axios.post(`${ML_SERVICE_URL}/chat/summarize`, {
            user_query: message,
            context_data: financialContext,
            user_id: userId
        });

        const botText = narrativeResponse.data.text;

        // 6. Save Bot response to DB
        await supabase.from('ai_chat_messages').insert([{
            session_id: session.session_id,
            sender: 'bot',
            message_text: botText
        }]);

        return res.status(200).json({
            text: botText,
            type: 'bot'
        });

    } catch (err) {
        logger.error('handleChatMessage error:', err);
        return res.status(500).json({ error: 'Failed to process chat message' });
    }
};

module.exports = {
    handleChatMessage,
    getChatHistory
};
