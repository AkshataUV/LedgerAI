/**
 * Chatbot Routes — Agentic Smart Insight Chatbot
 * 
 * Mounted at /api/chatbot in server.js
 * Separate from existing /api/chat routes to avoid any conflicts.
 */

const express = require('express');
const router = express.Router();
const authMiddleware = require('../middleware/authMiddleware');
const {
  handleInsightMessage,
  getInsightHistory,
  clearInsightHistory
} = require('./chatbotController');

// All chatbot routes require authentication
// router.use(authMiddleware);
router.use(authMiddleware);

/**
 * GET /api/chatbot/history
 * Fetch the user's chat history
 */
router.get('/history', getInsightHistory);

/**
 * POST /api/chatbot/message
 * Send a message to the smart insight chatbot
 */
router.post('/message', handleInsightMessage);

/**
 * POST /api/chatbot/clear
 * Clear current chat session
 */
router.post('/clear', clearInsightHistory);

module.exports = router;
