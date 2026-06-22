const shardService = require('../services/shardService');

async function getClusterHealth(req, res, next) {
  try {
    const health = await shardService.getClusterHealth();
    res.json({
      success: true,
      data: health,
    });
  } catch (err) {
    next(err);
  }
}

async function getClusterStats(req, res, next) {
  try {
    const stats = await shardService.getClusterStats();
    res.json({
      success: true,
      data: stats,
    });
  } catch (err) {
    next(err);
  }
}

async function getNodeAllocation(req, res, next) {
  try {
    const allocation = await shardService.getShardAllocation();
    res.json({
      success: true,
      data: allocation,
    });
  } catch (err) {
    next(err);
  }
}

async function getShards(req, res, next) {
  try {
    const { index, bytes } = req.query;
    const params = {};
    if (index) params.index = index;
    if (bytes) params.bytes = bytes;
    const shards = await shardService.getShardAllocationDetails(params);
    res.json({
      success: true,
      data: shards,
    });
  } catch (err) {
    next(err);
  }
}

async function getAllocationExplain(req, res, next) {
  try {
    const { index, shard, primary } = req.body;
    if (!index || shard === undefined || shard === null) {
      return res.status(400).json({
        success: false,
        error: '缺少必要参数: index 和 shard',
      });
    }
    const isPrimary = primary === true || primary === 'true' || primary === 1;
    const explain = await shardService.getAllocationExplain(
      index,
      parseInt(shard, 10),
      isPrimary
    );
    res.json({
      success: true,
      data: explain,
    });
  } catch (err) {
    next(err);
  }
}

async function getUnassignedShards(req, res, next) {
  try {
    const unassigned = await shardService.getUnassignedShards();
    res.json({
      success: true,
      count: unassigned.length,
      data: unassigned,
    });
  } catch (err) {
    next(err);
  }
}

function buildAlertSummary(analyzed) {
  const alertLevelCounts = { critical: 0, warning: 0, info: 0 };
  const rootCauseCounts = {};
  const rootCauseAlerts = {};

  analyzed.forEach((item) => {
    if (alertLevelCounts[item.alert_level] !== undefined) {
      alertLevelCounts[item.alert_level]++;
    }
    rootCauseCounts[item.root_cause] = (rootCauseCounts[item.root_cause] || 0) + 1;
    if (!rootCauseAlerts[item.root_cause]) {
      rootCauseAlerts[item.root_cause] = {
        alert_level: item.alert_level,
        alert_message: item.alert_message,
        recommendation: item.recommendation,
        count: 0,
      };
    }
    rootCauseAlerts[item.root_cause].count++;
  });

  return { alertLevelCounts, rootCauseCounts, rootCauseAlerts };
}

async function analyzeUnassigned(req, res, next) {
  try {
    const analyzed = await shardService.analyzeUnassignedShards();
    const { alertLevelCounts, rootCauseCounts, rootCauseAlerts } = buildAlertSummary(analyzed);
    res.json({
      success: true,
      summary: {
        total_unassigned: analyzed.length,
        alert_level_counts: alertLevelCounts,
        root_cause_counts: rootCauseCounts,
        root_cause_alerts: rootCauseAlerts,
      },
      data: analyzed,
    });
  } catch (err) {
    next(err);
  }
}

async function getShardSummary(req, res, next) {
  try {
    const summary = await shardService.getShardAllocationSummary();
    res.json({
      success: true,
      data: summary,
    });
  } catch (err) {
    next(err);
  }
}

async function getNodeStats(req, res, next) {
  try {
    const stats = await shardService.getNodeAllocationStats();
    res.json({
      success: true,
      data: stats,
    });
  } catch (err) {
    next(err);
  }
}

async function getFullReport(req, res, next) {
  try {
    const [summary, analyzed, nodeStats] = await Promise.all([
      shardService.getShardAllocationSummary(),
      shardService.analyzeUnassignedShards(),
      shardService.getNodeAllocationStats(),
    ]);

    const { alertLevelCounts, rootCauseCounts, rootCauseAlerts } = buildAlertSummary(analyzed);

    res.json({
      success: true,
      generated_at: new Date().toISOString(),
      cluster: summary.cluster,
      overview: {
        total_shards: summary.total_shards,
        shard_state_counts: summary.shard_state_counts,
        total_size: summary.total_size_formatted,
        total_docs: summary.total_docs,
        indices_with_unassigned: summary.indices_with_unassigned,
      },
      alert_summary: {
        total_unassigned: analyzed.length,
        alert_level_counts: alertLevelCounts,
        root_cause_counts: rootCauseCounts,
        root_cause_alerts: rootCauseAlerts,
      },
      unassigned_shards: {
        count: analyzed.length,
        details: analyzed,
      },
      nodes: nodeStats,
      indices: summary.indices,
    });
  } catch (err) {
    next(err);
  }
}

module.exports = {
  getClusterHealth,
  getClusterStats,
  getNodeAllocation,
  getShards,
  getAllocationExplain,
  getUnassignedShards,
  analyzeUnassigned,
  getShardSummary,
  getNodeStats,
  getFullReport,
};
