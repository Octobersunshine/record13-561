const client = require('../clients/elasticsearch');

const UNASSIGNED_REASON_SEVERITY = {
  'INDEX_CREATED': 'info',
  'CLUSTER_RECOVERED': 'info',
  'DANGLING_INDEX_IMPORTED': 'info',
  'NEW_INDEX_RESTORED': 'info',
  'EXISTING_INDEX_RESTORED': 'info',
  'REPLICA_ADDED': 'info',
  'ALLOCATION_FAILED': 'critical',
  'NODE_LEFT': 'warning',
  'REROUTE_CANCELLED': 'warning',
  'REINITIALIZED': 'warning',
  'REALLOCATED_REPLICA': 'warning',
  'PRIMARY_FAILED': 'critical',
  'FORCED_EMPTY_PRIMARY': 'critical',
  'MANUAL_ALLOCATION': 'info',
};

function getSeverity(reason) {
  return UNASSIGNED_REASON_SEVERITY[reason] || 'warning';
}

function formatBytes(bytes) {
  if (bytes === -1 || bytes === undefined || bytes === null) return 'N/A';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = bytes;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex++;
  }
  return `${size.toFixed(2)} ${units[unitIndex]}`;
}

async function getClusterHealth() {
  const { body } = await client.cluster.health();
  return body;
}

async function getClusterStats() {
  const { body } = await client.cluster.stats();
  return body;
}

async function getShardAllocation() {
  const { body } = await client.cat.allocation({
    format: 'json',
    v: true,
  });
  return body;
}

async function getShardAllocationDetails(params = {}) {
  const { body } = await client.cat.shards({
    format: 'json',
    v: true,
    ...params,
  });
  return body;
}

async function getAllocationExplain(index, shard, primary = false) {
  const { body } = await client.cluster.allocationExplain({
    body: {
      index,
      shard,
      primary,
    },
  });
  return body;
}

async function getUnassignedShards() {
  const shards = await getShardAllocationDetails();
  return shards.filter((shard) => shard.state === 'UNASSIGNED');
}

async function analyzeUnassignedShards() {
  const unassignedShards = await getUnassignedShards();
  const analyzed = [];

  for (const shard of unassignedShards) {
    const item = {
      index: shard.index,
      shard: parseInt(shard.shard, 10),
      prirep: shard.prirep,
      state: shard.state,
      unassigned_reason: shard['unassigned.reason'] || 'UNKNOWN',
      severity: getSeverity(shard['unassigned.reason']),
      docs: shard.docs ? parseInt(shard.docs, 10) : 0,
      store: shard.store || '0b',
      unassigned_at: shard['unassigned.at'] || null,
      unassigned_for: shard['unassigned.for'] || null,
    };

    try {
      const explain = await getAllocationExplain(
        shard.index,
        parseInt(shard.shard, 10),
        shard.prirep === 'p'
      );
      item.allocation_explain = {
        can_allocate: explain.can_allocate,
        allocate_explanation: explain.explanation,
        node_allocation_decisions: explain.node_allocation_decisions
          ? explain.node_allocation_decisions.map((decision) => ({
              node_id: decision.node_id,
              node_name: decision.node_name,
              can_allocate: decision.can_allocate,
              reasons: decision.deciders
                ? decision.deciders
                    .filter((d) => d.decision === 'NO')
                    .map((d) => ({
                      decider: d.decider,
                      explanation: d.explanation,
                    }))
                : [],
            }))
          : [],
      };
    } catch (err) {
      item.allocation_explain = {
        error: err.message,
      };
    }

    analyzed.push(item);
  }

  return analyzed;
}

async function getShardAllocationSummary() {
  const [health, shards, clusterStats] = await Promise.all([
    getClusterHealth(),
    getShardAllocationDetails(),
    getClusterStats(),
  ]);

  const stateCounts = {};
  const indexShardMap = {};
  let totalSize = 0;
  let totalDocs = 0;

  for (const shard of shards) {
    const state = shard.state;
    stateCounts[state] = (stateCounts[state] || 0) + 1;

    if (!indexShardMap[shard.index]) {
      indexShardMap[shard.index] = {
        total: 0,
        started: 0,
        unassigned: 0,
        initializing: 0,
        relocating: 0,
        size: 0,
        docs: 0,
      };
    }
    indexShardMap[shard.index].total++;
    if (state === 'STARTED') indexShardMap[shard.index].started++;
    else if (state === 'UNASSIGNED') indexShardMap[shard.index].unassigned++;
    else if (state === 'INITIALIZING') indexShardMap[shard.index].initializing++;
    else if (state === 'RELOCATING') indexShardMap[shard.index].relocating++;

    const storeBytes = shard.store ? parseStoreToBytes(shard.store) : 0;
    indexShardMap[shard.index].size += storeBytes;
    totalSize += storeBytes;

    if (shard.docs) {
      const docs = parseInt(shard.docs, 10);
      if (!isNaN(docs)) {
        indexShardMap[shard.index].docs += docs;
        totalDocs += docs;
      }
    }
  }

  const indices = Object.entries(indexShardMap)
    .map(([name, data]) => ({
      name,
      ...data,
      size_formatted: formatBytes(data.size),
      health_status:
        data.unassigned > 0 ? (data.started === 0 ? 'red' : 'yellow') : 'green',
    }))
    .sort((a, b) => b.unassigned - a.unassigned || b.size - a.size);

  const unassignedIndices = indices.filter((i) => i.unassigned > 0);

  return {
    cluster: {
      name: clusterStats.cluster_name,
      status: health.status,
      number_of_nodes: health.number_of_nodes,
      number_of_data_nodes: health.number_of_data_nodes,
      active_primary_shards: health.active_primary_shards,
      active_shards: health.active_shards,
      relocating_shards: health.relocating_shards,
      initializing_shards: health.initializing_shards,
      unassigned_shards: health.unassigned_shards,
      number_of_pending_tasks: health.number_of_pending_tasks,
      number_of_in_flight_fetch: health.number_of_in_flight_fetch,
      task_max_waiting_in_queue_millis: health.task_max_waiting_in_queue_millis,
    },
    shard_state_counts: stateCounts,
    total_shards: shards.length,
    total_size_formatted: formatBytes(totalSize),
    total_docs: totalDocs,
    indices_with_unassigned: unassignedIndices.length,
    indices,
  };
}

function parseStoreToBytes(storeStr) {
  if (!storeStr) return 0;
  const units = { b: 1, kb: 1024, mb: 1024 * 1024, gb: 1024 * 1024 * 1024, tb: 1024 ** 4 };
  const match = storeStr.toLowerCase().match(/^([\d.]+)\s*([kmgt]?b)$/);
  if (!match) return 0;
  const value = parseFloat(match[1]);
  const unit = match[2];
  return Math.round(value * units[unit]);
}

async function getNodeAllocationStats() {
  const nodes = await getShardAllocation();
  const [clusterStats] = await Promise.all([getClusterStats()]);

  const nodeStats = nodes.map((node) => ({
    node: node.node,
    ip: node.ip,
    shards: node.shards ? parseInt(node.shards, 10) : 0,
    disk_indices: node['disk.indices'],
    disk_used: node['disk.used'],
    disk_avail: node['disk.avail'],
    disk_total: node['disk.total'],
    disk_percent: node['disk.percent'],
    load_1m: node['load.1m'],
    load_5m: node['load.5m'],
    load_15m: node['load.15m'],
    cpu: node.cpu,
    memory_percent: node['memory.percent'],
  }));

  return {
    cluster_name: clusterStats.cluster_name,
    total_nodes: nodeStats.length,
    nodes: nodeStats,
  };
}

module.exports = {
  getClusterHealth,
  getClusterStats,
  getShardAllocation,
  getShardAllocationDetails,
  getAllocationExplain,
  getUnassignedShards,
  analyzeUnassignedShards,
  getShardAllocationSummary,
  getNodeAllocationStats,
};
