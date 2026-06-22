const express = require('express');
const shardController = require('../controllers/shardController');

const router = express.Router();

router.get('/health', shardController.getClusterHealth);

router.get('/stats', shardController.getClusterStats);

router.get('/nodes', shardController.getNodeAllocation);

router.get('/nodes/stats', shardController.getNodeStats);

router.get('/shards', shardController.getShards);

router.get('/shards/summary', shardController.getShardSummary);

router.get('/shards/unassigned', shardController.getUnassignedShards);

router.get('/shards/unassigned/analyze', shardController.analyzeUnassigned);

router.post('/shards/allocation-explain', shardController.getAllocationExplain);

router.get('/report', shardController.getFullReport);

router.get('/relocate/settings', shardController.getRelocationSettings);

router.get('/relocate/idle-nodes', shardController.getIdleNodes);

router.get('/relocate/candidates', shardController.getMigrationCandidates);

router.post('/relocate/execute', shardController.executeRelocation);

router.post('/relocate/batch', shardController.executeBatchRelocation);

router.post('/relocate/cancel', shardController.cancelRelocation);

router.post('/relocate/throttle', shardController.setRelocationThrottle);

module.exports = router;
