export interface ContainerStat {
  name: string;
  cpu: string;
  cpuCores?: string[];
  cpuLimit?: number;
  mem: string;
  cacheMem?: string;
}

export interface LatencySummary {
  avg: number;
  p50: number;
  p95: number;
  p99: number;
  max?: number;
}

export interface TelemetryPoint {
  time: string;
  timestamp: number;
  appLatency: number;
  queueLatency?: number;
  p95?: number;
  p99?: number;
  cpu: number;
  io: number;
  wait_tasks?: number;
  active_locks?: number;
  recordsModified: number;
  recordsFailed: number;
  recordsLate: number;
  recordsDropped?: number;
  recordsInKafka: number;
  lag: number;
  pipelineLatency?: number;
  pipelineP95?: number;
  actualRps?: number;
  targetRps?: number;
  slaRate?: number;
  successRate?: number;
  consumerE2eLatency?: number;
  consumerEnrichmentLatency?: number;
  consumerLag?: number;
}

export interface SystemLog {
  time: string;
  message: string;
  level: string;
  timestamp: number;
  seq?: number;
}

export interface SubscriberStats {
  numSubscribers: number;
  readySubscribers?: number;
  totalMessagesConsumed: number;
  consumedPerSec?: number;
  avgEnrichmentLatency: number;
  p95EnrichmentLatency: number;
  p99EnrichmentLatency: number;
  avgE2eLatency: number;
  p95E2eLatency?: number;
  enrichmentsFailed: number;
  enrichmentMisses?: number;
  skippedEvents?: number;
  consumerLag: number;
}

export type Phase = 'starting' | 'idle' | 'provisioning' | 'running' | 'paused' | 'draining' | 'cleaning';

export interface RunSummary {
  runId: string;
  approach: number;
  approachName: string;
  params: Record<string, unknown>;
  trackedTopic: string;
  elapsedSec: number;
  finalTargetRps: number;
  avgCommittedRps: number;
  recordsModified: number;
  recordsLate: number;
  recordsFailed: number;
  recordsDropped: number;
  recordsCommitted: number;
  recordsInKafka: number;
  lag: number;
  slaRate: number;
  successRate: number;
  appLatency: number;
  p50: number;
  p95: number;
  p99: number;
  maxLatency: number;
  queueLatency: number;
  pipelineLatency: LatencySummary;
  enrichmentMisses: number;
  partialWrites: number;
  subscriber?: {
    numSubscribers: number;
    consumed: number;
    failed: number;
    misses: number;
    enrichment: LatencySummary & { count: number };
    e2e: LatencySummary & { count: number };
  };
  flawAlert: string | null;
  finishedAt: string;
}

export interface VerifyResult {
  topic: string;
  messagesChecked: number;
  uniqueEvents: number;
  expected: number;
  missing: number;
  duplicates: number;
  keyOrderViolations: number;
  entitiesChecked: number;
  entitiesSpreadAcrossPartitions: number;
  ordered: boolean;
  tombstones: number;
  unparsable: number;
  complete: boolean;
}

export interface SimulationStats {
  runId?: string | null;
  phase: Phase;
  phaseDetail?: string;
  lastError?: string | null;
  elapsedSec?: number;
  isRunning: boolean;
  isPaused?: boolean;
  isProvisioning?: boolean;
  isDraining?: boolean;
  isCleaning?: boolean;
  approach: number | null;
  trackedTopic?: string | null;
  connector?: string | null;
  rps: number;
  issuedRps?: number;
  actualRps?: number;
  appLatency: number;
  queueLatency?: number;
  p50?: number;
  p95?: number;
  p99?: number;
  flawAlert: string | null;
  dbStats: { cpu: number; io: number; wait_tasks?: number; active_locks?: number; ldfSizeMB?: number; ple?: number; tempDbMB?: number; activeConnections?: number };
  containerStats: ContainerStat[];
  recordsModified: number;
  recordsFailed: number;
  recordsLate: number;
  recordsDropped?: number;
  recordsCommitted?: number;
  recordsInKafka: number;
  lag: number;
  pipelineLatency?: LatencySummary;
  captureLagMs?: number;
  enrichmentMisses?: number;
  smtLookupAvgMs?: number;
  logs?: SystemLog[];
  newLogs?: SystemLog[];
  subscriberStats?: SubscriberStats;
  lastRunSummary?: RunSummary | null;
}
