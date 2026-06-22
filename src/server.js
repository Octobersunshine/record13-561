const express = require('express');
const cors = require('cors');
const config = require('./config');
const shardRoutes = require('./routes/shardRoutes');

const app = express();

app.use(cors());
app.use(express.json());

app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  next();
});

app.get('/', (req, res) => {
  res.json({
    name: 'ES Shard Monitor API',
    version: '1.0.0',
    description: 'Elasticsearch 集群分片分配状态监控与未分配异常分片识别',
    endpoints: {
      health_check: 'GET /health',
      cluster_health: 'GET /api/cluster/health',
      cluster_stats: 'GET /api/cluster/stats',
      node_allocation: 'GET /api/cluster/nodes',
      node_stats: 'GET /api/cluster/nodes/stats',
      list_shards: 'GET /api/cluster/shards?index=&bytes=',
      shard_summary: 'GET /api/cluster/shards/summary',
      unassigned_shards: 'GET /api/cluster/shards/unassigned',
      analyze_unassigned: 'GET /api/cluster/shards/unassigned/analyze',
      allocation_explain: 'POST /api/cluster/shards/allocation-explain { index, shard, primary }',
      full_report: 'GET /api/cluster/report',
      relocate_settings: 'GET /api/cluster/relocate/settings',
      relocate_idle_nodes: 'GET /api/cluster/relocate/idle-nodes',
      relocate_candidates: 'GET /api/cluster/relocate/candidates?index=',
      relocate_execute: 'POST /api/cluster/relocate/execute { index, shard, from_node, to_node }',
      relocate_batch: 'POST /api/cluster/relocate/batch { operations: [{index, shard, from_node, to_node}] }',
      relocate_cancel: 'POST /api/cluster/relocate/cancel { index, shard, node }',
      relocate_throttle: 'POST /api/cluster/relocate/throttle { cluster_concurrent_rebalance, node_concurrent_recoveries }',
    },
  });
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
  });
});

app.use('/api/cluster', shardRoutes);

app.use((err, req, res, next) => {
  console.error('Error:', err.message);
  if (err.stack) {
    console.error(err.stack);
  }

  const statusCode = err.statusCode || err.meta?.statusCode || 500;
  const response = {
    success: false,
    error: err.message || 'Internal Server Error',
  };

  if (process.env.NODE_ENV === 'development') {
    response.stack = err.stack;
    if (err.meta) {
      response.meta = err.meta;
    }
  }

  res.status(statusCode).json(response);
});

app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: 'Not Found',
    path: req.path,
  });
});

app.listen(config.server.port, () => {
  console.log('========================================');
  console.log('   ES Shard Monitor API Server');
  console.log('========================================');
  console.log(`Server running on http://localhost:${config.server.port}`);
  console.log(`Elasticsearch Node: ${config.elasticsearch.node}`);
  console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
  console.log('========================================');
  console.log('');
  console.log('Available Endpoints:');
  console.log('  GET  /health                           - Health check');
  console.log('  GET  /api/cluster/health               - Cluster health');
  console.log('  GET  /api/cluster/stats                - Cluster stats');
  console.log('  GET  /api/cluster/nodes                - Node allocation');
  console.log('  GET  /api/cluster/nodes/stats          - Node stats');
  console.log('  GET  /api/cluster/shards               - List shards');
  console.log('  GET  /api/cluster/shards/summary       - Shard summary');
  console.log('  GET  /api/cluster/shards/unassigned    - Unassigned shards');
  console.log('  GET  /api/cluster/shards/unassigned/analyze - Analyze unassigned');
  console.log('  POST /api/cluster/shards/allocation-explain - Explain allocation');
  console.log('  GET  /api/cluster/report               - Full report');
  console.log('  GET  /api/cluster/relocate/settings    - Relocation settings');
  console.log('  GET  /api/cluster/relocate/idle-nodes  - Idle nodes');
  console.log('  GET  /api/cluster/relocate/candidates  - Migration candidates');
  console.log('  POST /api/cluster/relocate/execute     - Execute relocation');
  console.log('  POST /api/cluster/relocate/batch       - Batch relocation');
  console.log('  POST /api/cluster/relocate/cancel      - Cancel relocation');
  console.log('  POST /api/cluster/relocate/throttle    - Set throttle');
  console.log('');
});

process.on('SIGINT', () => {
  console.log('\nShutting down server gracefully...');
  process.exit(0);
});
