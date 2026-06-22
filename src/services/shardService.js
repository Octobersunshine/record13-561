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

async function getNodeInfoMap() {
  const { body } = await client.nodes.info({
    filter_path: ['nodes.*.name', 'nodes.*.host', 'nodes.*.ip'],
  });
  const nodeMap = {};
  for (const [nodeId, info] of Object.entries(body.nodes || {})) {
    nodeMap[nodeId] = {
      nodeId,
      name: info.name,
      host: info.host,
      ip: info.ip,
    };
  }
  return nodeMap;
}

async function getRelocationSettings() {
  try {
    const { body } = await client.cluster.getSettings({
      include_defaults: true,
      filter_path: [
        'defaults.cluster.routing.allocation.cluster_concurrent_rebalance',
        'defaults.cluster.routing.allocation.node_concurrent_recoveries',
        'defaults.cluster.routing.allocation.node_initial_primaries_recoveries',
        'persistent.cluster.routing.allocation.cluster_concurrent_rebalance',
        'persistent.cluster.routing.allocation.node_concurrent_recoveries',
        'persistent.cluster.routing.allocation.node_initial_primaries_recoveries',
        'transient.cluster.routing.allocation.cluster_concurrent_rebalance',
        'transient.cluster.routing.allocation.node_concurrent_recoveries',
        'transient.cluster.routing.allocation.node_initial_primaries_recoveries',
      ],
    });
    return body;
  } catch (err) {
    return { error: err.message };
  }
}

function analyzeNodeLoad(nodeStatsList) {
  const validNodes = nodeStatsList.filter((n) =>
    n.disk_percent !== null && n.disk_percent !== undefined && n.disk_percent !== 'N/A'
  );

  if (validNodes.length === 0) {
    return { idle_nodes: [], busy_nodes: [], avg_shards: 0, avg_disk: 0 };
  }

  const totalShards = validNodes.reduce((sum, n) => sum + n.shards, 0);
  const totalDisk = validNodes.reduce((sum, n) => {
    const dp = parseFloat(n.disk_percent);
    return sum + (isNaN(dp) ? 0 : dp);
  }, 0);

  const avgShards = totalShards / validNodes.length;
  const avgDisk = totalDisk / validNodes.length;

  const idleNodes = validNodes.filter((n) => {
    const dp = parseFloat(n.disk_percent);
    return n.shards < avgShards * 0.8 && dp < avgDisk * 0.8 && dp < 70;
  }).sort((a, b) => a.shards - b.shards);

  const busyNodes = validNodes.filter((n) => {
    const dp = parseFloat(n.disk_percent);
    return n.shards > avgShards * 1.2 || dp > avgDisk * 1.2 || dp > 85;
  }).sort((a, b) => b.shards - a.shards);

  return {
    idle_nodes: idleNodes,
    busy_nodes: busyNodes,
    avg_shards: Math.round(avgShards * 100) / 100,
    avg_disk_percent: Math.round(avgDisk * 100) / 100,
  };
}

async function getIdleNodes() {
  const nodeAllocationStats = await getNodeAllocationStats();
  const [nodeInfoMap, shards] = await Promise.all([
    getNodeInfoMap(),
    getShardAllocationDetails(),
  ]);

  const loadAnalysis = analyzeNodeLoad(nodeAllocationStats.nodes);

  const shardsByNode = {};
  for (const s of shards) {
    if (s.node && s.node !== 'UNASSIGNED') {
      if (!shardsByNode[s.node]) shardsByNode[s.node] = [];
      shardsByNode[s.node].push({
        index: s.index,
        shard: parseInt(s.shard, 10),
        prirep: s.prirep,
        state: s.state,
        docs: s.docs ? parseInt(s.docs, 10) : 0,
        store: s.store,
      });
    }
  }

  const idleNodesWithDetails = loadAnalysis.idle_nodes.map((node) => {
    const dp = parseFloat(node.disk_percent);
    const utilizationScore = (node.shards / (loadAnalysis.avg_shards || 1)) * 0.4
      + (dp / 100) * 0.6;
    return {
      node_name: node.node,
      ip: node.ip,
      shards_count: node.shards,
      disk_percent: dp,
      disk_used: node.disk_used,
      disk_avail: node.disk_avail,
      disk_total: node.disk_total,
      load_1m: node.load_1m,
      load_5m: node.load_5m,
      load_15m: node.load_15m,
      cpu: node.cpu,
      memory_percent: node.memory_percent,
      utilization_score: Math.round(utilizationScore * 100) / 100,
      shards: shardsByNode[node.node] || [],
    };
  });

  const busyNodesWithDetails = loadAnalysis.busy_nodes.map((node) => {
    const dp = parseFloat(node.disk_percent);
    return {
      node_name: node.node,
      ip: node.ip,
      shards_count: node.shards,
      disk_percent: dp,
      disk_used: node.disk_used,
      disk_avail: node.disk_avail,
      disk_total: node.disk_total,
      shards: shardsByNode[node.node] || [],
    };
  });

  const nodeIdToName = {};
  for (const [nodeId, info] of Object.entries(nodeInfoMap)) {
    nodeIdToName[nodeId] = info.name;
    nodeIdToName[info.name] = nodeId;
  }

  return {
    cluster_name: nodeAllocationStats.cluster_name,
    baseline: {
      total_nodes: nodeAllocationStats.total_nodes,
      avg_shards_per_node: loadAnalysis.avg_shards,
      avg_disk_percent: loadAnalysis.avg_disk_percent,
    },
    idle_nodes: idleNodesWithDetails,
    busy_nodes: busyNodesWithDetails,
    node_id_name_map: nodeIdToName,
  };
}

async function getMigrationCandidates(index = null) {
  const [idleResult, shards, shardSummary] = await Promise.all([
    getIdleNodes(),
    getShardAllocationDetails(index ? { index } : {}),
    getShardAllocationSummary(),
  ]);

  const idleNodeNames = idleResult.idle_nodes.map((n) => n.node_name);
  const busyNodeNames = idleResult.busy_nodes.map((n) => n.node_name);

  const migratingShards = [];
  for (const shard of shards) {
    if (shard.state !== 'STARTED') continue;
    if (!busyNodeNames.includes(shard.node)) continue;
    if (shardSummary && shardSummary.indices) {
      const idxInfo = shardSummary.indices.find((i) => i.name === shard.index);
      if (idxInfo && idxInfo.health_status === 'red') continue;
    }
    migratingShards.push({
      index: shard.index,
      shard: parseInt(shard.shard, 10),
      prirep: shard.prirep,
      current_node: shard.node,
      docs: shard.docs ? parseInt(shard.docs, 10) : 0,
      store: shard.store,
    });
  }

  const candidates = migratingShards.map((shard) => ({
    ...shard,
    candidate_target_nodes: idleNodeNames.filter((name) => {
      const idxShards = shards.filter(
        (s) => s.index === shard.index && s.shard === shard.shard.toString()
      );
      return !idxShards.some((s) => s.node === name);
    }),
  })).filter((c) => c.candidate_target_nodes.length > 0);

  return {
    cluster_name: idleResult.cluster_name,
    baseline: idleResult.baseline,
    busy_nodes: idleResult.busy_nodes.map((n) => ({
      node_name: n.node_name,
      shards_count: n.shards_count,
      disk_percent: n.disk_percent,
    })),
    idle_nodes: idleResult.idle_nodes.map((n) => ({
      node_name: n.node_name,
      shards_count: n.shards_count,
      disk_percent: n.disk_percent,
      utilization_score: n.utilization_score,
    })),
    migration_candidates_count: candidates.length,
    migration_candidates: candidates,
  };
}

async function executeRelocation(index, shard, fromNode, toNode) {
  if (!index || shard === undefined || shard === null || !fromNode || !toNode) {
    throw new Error('缺少必要参数: index, shard, fromNode, toNode');
  }

  const shardNum = parseInt(shard, 10);
  if (isNaN(shardNum)) {
    throw new Error('shard 必须是数字');
  }

  if (fromNode === toNode) {
    throw new Error('源节点和目标节点不能相同');
  }

  const shards = await getShardAllocationDetails({ index });
  const targetShard = shards.find(
    (s) => s.index === index && s.shard === shardNum.toString() && s.node === fromNode
  );

  if (!targetShard) {
    throw new Error(`在节点 ${fromNode} 上未找到分片 ${index}[${shardNum}]`);
  }

  if (targetShard.state !== 'STARTED') {
    throw new Error(`分片状态为 ${targetShard.state}，仅 STARTED 状态的分片可迁移`);
  }

  if (targetShard.prirep === 'p') {
    const replicas = shards.filter(
      (s) => s.index === index && s.shard === shardNum.toString() && s.prirep === 'r'
    );
    const hasSyncReplica = replicas.some((s) => s.state === 'STARTED');
    if (!hasSyncReplica) {
      throw new Error('主分片迁移前需确保至少有一个同步的副本分片，建议先提升副本再迁移');
    }
  }

  const body = {
    commands: [
      {
        move: {
          index,
          shard: shardNum,
          from_node: fromNode,
          to_node: toNode,
        },
      },
    ],
  };

  const result = await client.cluster.reroute({
    body,
    retry_failed: true,
    metric: ['nodes', 'allocation'],
  });

  return {
    success: true,
    acknowledged: result.body.acknowledged,
    command: body.commands[0],
    state: result.body.state,
  };
}

async function executeBatchRelocation(operations) {
  if (!Array.isArray(operations) || operations.length === 0) {
    throw new Error('operations 必须是非空数组');
  }

  const commands = [];
  for (const op of operations) {
    if (!op.index || op.shard === undefined || !op.from_node || !op.to_node) {
      throw new Error(`操作参数不完整: ${JSON.stringify(op)}`);
    }
    if (op.from_node === op.to_node) {
      throw new Error(`源节点和目标节点不能相同: ${JSON.stringify(op)}`);
    }
    const shardNum = parseInt(op.shard, 10);
    if (isNaN(shardNum)) {
      throw new Error(`shard 必须是数字: ${op.shard}`);
    }
    commands.push({
      move: {
        index: op.index,
        shard: shardNum,
        from_node: op.from_node,
        to_node: op.to_node,
      },
    });
  }

  const result = await client.cluster.reroute({
    body: { commands },
    retry_failed: true,
  });

  return {
    success: true,
    acknowledged: result.body.acknowledged,
    commands_count: commands.length,
    commands,
    state: result.body.state,
  };
}

async function setRelocationThrottle(concurrentRebalance, concurrentRecoveries) {
  const body = { transient: {} };
  if (concurrentRebalance !== undefined && concurrentRebalance !== null) {
    const val = parseInt(concurrentRebalance, 10);
    if (isNaN(val) || val < 0 || val > 20) {
      throw new Error('cluster_concurrent_rebalance 必须是 0-20 之间的整数');
    }
    body.transient['cluster.routing.allocation.cluster_concurrent_rebalance'] = val.toString();
  }
  if (concurrentRecoveries !== undefined && concurrentRecoveries !== null) {
    const val = parseInt(concurrentRecoveries, 10);
    if (isNaN(val) || val < 0 || val > 20) {
      throw new Error('node_concurrent_recoveries 必须是 0-20 之间的整数');
    }
    body.transient['cluster.routing.allocation.node_concurrent_recoveries'] = val.toString();
  }

  if (Object.keys(body.transient).length === 0) {
    throw new Error('未提供任何限流参数');
  }

  const result = await client.cluster.putSettings({ body });

  return {
    success: true,
    acknowledged: result.body.acknowledged,
    applied_settings: body.transient,
  };
}

async function cancelRelocation(index, shard, node) {
  if (!index || shard === undefined || !node) {
    throw new Error('缺少必要参数: index, shard, node');
  }

  const shardNum = parseInt(shard, 10);
  if (isNaN(shardNum)) {
    throw new Error('shard 必须是数字');
  }

  const body = {
    commands: [
      {
        cancel: {
          index,
          shard: shardNum,
          node,
          allow_primary: true,
        },
      },
    ],
  };

  const result = await client.cluster.reroute({ body });

  return {
    success: true,
    acknowledged: result.body.acknowledged,
    command: body.commands[0],
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
  getIdleNodes,
  getMigrationCandidates,
  executeRelocation,
  executeBatchRelocation,
  setRelocationThrottle,
  cancelRelocation,
  getRelocationSettings,
};
