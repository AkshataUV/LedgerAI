const express = require('express');
const router = express.Router();
const chatController = require('../controllers/chatController');
const authMiddleware = require('../middleware/authMiddleware');

/**
 * LedgerBuddy AI Assistant Routes
 */
router.get('/history', authMiddleware, chatController.getChatHistory);
router.post('/message', authMiddleware, chatController.handleChatMessage);

module.exports = router;
