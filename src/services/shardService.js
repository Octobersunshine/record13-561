const client = require('../clients/elasticsearch');

const ROOT_CAUSE_PROFILES = {
  DISK_THRESHOLD: {
    alert_level: 'critical',
    alert_message: '磁盘水位超限：目标节点磁盘使用率已超过集群阈值，分片无法分配到该节点',
    recommendation: '1. 清理磁盘空间或扩容磁盘; 2. 调整 cluster.routing.allocation.disk.watermark.low/high 配置; 3. 使用 _cluster/reroute 强制分配（仅临时方案）',
  },
  NODE_LEFT: {
    alert_level: 'warning',
    alert_message: '节点离线：持有该分片数据的节点已离开集群，分片等待重新分配',
    recommendation: '1. 检查离线节点状态，尽快恢复; 2. 若节点无法恢复，使用 _cluster/reroute 分配副本提升为主分片; 3. 检查集群 minimum_master_nodes 配置是否合理',
  },
  NODE_LEFT_PRIMARY: {
    alert_level: 'critical',
    alert_message: '节点离线导致主分片丢失：持有主分片的节点已离开集群，数据存在不可用风险',
    recommendation: '1. 立即排查离线节点，优先恢复主分片所在节点; 2. 若节点永久下线，需通过 _cluster/reroute 接受数据丢失并提升副本; 3. 确认副本分片同步状态后再操作',
  },
  SHARD_LIMIT: {
    alert_level: 'warning',
    alert_message: '分片数上限：目标节点分片数已达到 cluster.routing.allocation.total_shards_per_node 上限',
    recommendation: '1. 调整 cluster.routing.allocation.total_shards_per_node 阈值; 2. 减少索引分片数; 3. 扩容数据节点以分散分片分布',
  },
  ALLOCATION_DISABLED: {
    alert_level: 'critical',
    alert_message: '分配已禁用：集群路由分配策略已被手动关闭，所有分片分配暂停',
    recommendation: '1. 检查 cluster.routing.allocation.enable 设置; 2. 执行 PUT _cluster/settings 恢复分配; 3. 确认是否因运维操作临时关闭，操作完成后需及时恢复',
  },
  FILTER_CONSTRAINT: {
    alert_level: 'info',
    alert_message: '分配过滤约束：当前索引的 allocation include/exclude/require 标签规则阻止了分片分配',
    recommendation: '1. 检查索引的 index.routing.allocation.* 配置; 2. 确认节点属性标签是否正确设置; 3. 修改过滤规则或添加匹配的节点属性',
  },
  REPLICA_NO_NODE: {
    alert_level: 'warning',
    alert_message: '副本无可用节点：集群中无足够的数据节点存放副本分片，副本无法分配',
    recommendation: '1. 扩容数据节点; 2. 降低索引副本数（仅临时方案）; 3. 检查节点是否因磁盘/内存压力被排除分配',
  },
  RECOVERY_FAILED: {
    alert_level: 'critical',
    alert_message: '分片恢复失败：分片在恢复过程中发生异常（IO 错误、损坏等），无法完成分配',
    recommendation: '1. 检查 ES 日志确认具体恢复失败原因; 2. 若为副本损坏，可删除后重新从主分片同步; 3. 若为主分片损坏，需评估数据丢失风险后重建索引',
  },
  PRIMARY_CORRUPTED: {
    alert_level: 'critical',
    alert_message: '主分片损坏：主分片数据损坏且无可用副本，数据面临丢失风险',
    recommendation: '1. 立即排查 ES 日志中 corruption 相关错误; 2. 若副本可用，提升副本为主分片; 3. 若无可用副本，需执行 _cluster/reroute 接受数据丢失; 4. 从备份恢复数据',
  },
  SAME_HOST: {
    alert_level: 'info',
    alert_message: '同宿主机约束：集群仅有一个数据节点或同属性节点，副本分片因 same_host 策略无法分配',
    recommendation: '1. 在不同宿主机上添加数据节点; 2. 如为测试环境，可临时关闭 cluster.routing.allocation.same_host 设置',
  },
  AWARENESS: {
    alert_level: 'warning',
    alert_message: '感知属性约束：节点感知（awareness）策略要求分片分布在不同属性值的节点上，但当前属性值对应的节点不足',
    recommendation: '1. 在缺失的 awareness 属性组中添加数据节点; 2. 检查 cluster.routing.allocation.awareness.attributes 配置; 3. 评估是否需要调整感知属性策略',
  },
  AWAITING_ALLOCATION: {
    alert_level: 'info',
    alert_message: '等待分配：新索引或新副本刚创建，正在等待集群分配到合适节点',
    recommendation: '1. 正常现象，分片会在短时间内完成分配; 2. 若长时间未分配，检查集群是否有足够的数据节点和磁盘空间',
  },
  UNKNOWN: {
    alert_level: 'warning',
    alert_message: '未知原因：无法通过 allocation explain 确定具体未分配原因',
    recommendation: '1. 检查 ES 日志获取更多线索; 2. 手动执行 _cluster/allocation/explain API 排查; 3. 检查集群配置和节点状态',
  },
};

function classifyRootCause(unassignedReason, prirep, explain) {
  if (explain && explain.error) {
    return buildResult('UNKNOWN', prirep);
  }

  const awaitingReasons = [
    'INDEX_CREATED',
    'CLUSTER_RECOVERED',
    'DANGLING_INDEX_IMPORTED',
    'NEW_INDEX_RESTORED',
    'EXISTING_INDEX_RESTORED',
    'REPLICA_ADDED',
    'MANUAL_ALLOCATION',
  ];
  if (awaitingReasons.includes(unassignedReason)) {
    return buildResult('AWAITING_ALLOCATION', prirep);
  }

  if (unassignedReason === 'NODE_LEFT') {
    const cause = prirep === 'p' ? 'NODE_LEFT_PRIMARY' : 'NODE_LEFT';
    return buildResult(cause, prirep);
  }

  if (unassignedReason === 'ALLOCATION_FAILED') {
    if (prirep === 'p') {
      return buildResult('PRIMARY_CORRUPTED', prirep);
    }
    return buildResult('RECOVERY_FAILED', prirep);
  }

  if (unassignedReason === 'PRIMARY_FAILED' || unassignedReason === 'FORCED_EMPTY_PRIMARY') {
    return buildResult('PRIMARY_CORRUPTED', prirep);
  }

  if (!explain || !explain.node_allocation_decisions) {
    return buildResult('UNKNOWN', prirep);
  }

  const deciderHits = collectDeciderNos(explain.node_allocation_decisions);

  if (deciderHits.disk_threshold && deciderHits.disk_threshold.length > 0) {
    const detail = deciderHits.disk_threshold[0].explanation;
    return buildResult('DISK_THRESHOLD', prirep, { disk_detail: detail });
  }

  if (deciderHits.enable_allocation && deciderHits.enable_allocation.length > 0) {
    return buildResult('ALLOCATION_DISABLED', prirep);
  }

  if (deciderHits.shard_limit && deciderHits.shard_limit.length > 0) {
    return buildResult('SHARD_LIMIT', prirep);
  }

  if (deciderHits.filter && deciderHits.filter.length > 0) {
    return buildResult('FILTER_CONSTRAINT', prirep);
  }

  if (deciderHits.same_host && deciderHits.same_host.length > 0) {
    return buildResult('SAME_HOST', prirep);
  }

  if (deciderHits.awareness && deciderHits.awareness.length > 0) {
    return buildResult('AWARENESS', prirep);
  }

  const noDeciderNames = Object.keys(deciderHits);
  if (noDeciderNames.length > 0) {
    const firstDecider = noDeciderNames[0];
    const firstExplanation = deciderHits[firstDecider][0].explanation;
    return buildResult('UNKNOWN', prirep, {
      raw_decider: firstDecider,
      raw_explanation: firstExplanation,
    });
  }

  if (explain.can_allocate === 'no' && explain.node_allocation_decisions.length === 0) {
    return buildResult('REPLICA_NO_NODE', prirep);
  }

  return buildResult('UNKNOWN', prirep);
}

function collectDeciderNos(nodeDecisions) {
  const hits = {};
  for (const decision of nodeDecisions) {
    if (!decision.deciders) continue;
    for (const d of decision.deciders) {
      if (d.decision === 'NO') {
        if (!hits[d.decider]) hits[d.decider] = [];
        hits[d.decider].push({
          node_name: decision.node_name,
          explanation: d.explanation,
        });
      }
    }
  }
  return hits;
}

function buildResult(causeKey, prirep, extra = {}) {
  const profile = ROOT_CAUSE_PROFILES[causeKey] || ROOT_CAUSE_PROFILES.UNKNOWN;
  let alertLevel = profile.alert_level;
  if (prirep === 'p' && alertLevel === 'warning') {
    alertLevel = 'critical';
  }
  return {
    root_cause: causeKey,
    alert_level: alertLevel,
    alert_message: profile.alert_message,
    recommendation: profile.recommendation,
    ...extra,
  };
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
    const prirep = shard.prirep;
    const unassignedReason = shard['unassigned.reason'] || 'UNKNOWN';

    const item = {
      index: shard.index,
      shard: parseInt(shard.shard, 10),
      prirep,
      state: shard.state,
      unassigned_reason: unassignedReason,
      docs: shard.docs ? parseInt(shard.docs, 10) : 0,
      store: shard.store || '0b',
      unassigned_at: shard['unassigned.at'] || null,
      unassigned_for: shard['unassigned.for'] || null,
    };

    let explain = null;

    try {
      explain = await getAllocationExplain(
        shard.index,
        parseInt(shard.shard, 10),
        prirep === 'p'
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

    const classification = classifyRootCause(unassignedReason, prirep, explain);
    item.root_cause = classification.root_cause;
    item.alert_level = classification.alert_level;
    item.alert_message = classification.alert_message;
    item.recommendation = classification.recommendation;
    if (classification.disk_detail) {
      item.disk_detail = classification.disk_detail;
    }
    if (classification.raw_decider) {
      item.raw_decider = classification.raw_decider;
      item.raw_explanation = classification.raw_explanation;
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
