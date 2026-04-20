const express = require('express');
const router = express.Router();
const { 
  deleteModule, 
  getGlobalKeywordRules, 
  createKeywordRule, 
  bulkCreateKeywordRules,
  deleteKeywordRule,
  getGlobalVectorCache,
  createVectorCacheEntry,
  bulkCreateVectorCacheEntries,
  deleteVectorCacheEntry,
  getReviewDocuments,
  getRandomQCResults,
  getFrequentlyChangedDocs
} = require('../qc/qcController');
const authMiddleware = require('../middleware/authMiddleware');
const { requireQC } = require('../middleware/roleMiddleware');
const rulesEngineService = require('../services/rulesEngineService');

// Dashboard Status Endpoints
router.get('/review-documents', authMiddleware, requireQC, getReviewDocuments);
router.get('/random-qc-results', authMiddleware, requireQC, getRandomQCResults);
router.get('/frequently-changed-docs', authMiddleware, requireQC, getFrequentlyChangedDocs);

// COA Modules
router.delete('/modules/:id', authMiddleware, requireQC, deleteModule);

// Global Keyword Rules
router.get('/keyword-rules', authMiddleware, requireQC, getGlobalKeywordRules);
router.post('/keyword-rules', authMiddleware, requireQC, createKeywordRule);
router.post('/keyword-rules/bulk', authMiddleware, requireQC, bulkCreateKeywordRules);
router.delete('/keyword-rules/:id', authMiddleware, requireQC, deleteKeywordRule);

// Global Vector Cache
router.get('/vector-cache', authMiddleware, requireQC, getGlobalVectorCache);
router.post('/vector-cache', authMiddleware, requireQC, createVectorCacheEntry);
router.post('/vector-cache/bulk', authMiddleware, requireQC, bulkCreateVectorCacheEntries);
router.delete('/vector-cache/:id', authMiddleware, requireQC, deleteVectorCacheEntry);

// Hot-reload rules endpoint
router.post('/reload-rules', authMiddleware, requireQC, async (req, res) => {
  try {
    await rulesEngineService.loadRules();
    res.json({ success: true, message: 'Rules reloaded successfully' });
  } catch (err) {
    console.error('Failed to reload rules:', err);
    res.status(500).json({ error: 'Failed to reload rules', details: err.message });
  }
});

module.exports = router;
