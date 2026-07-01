export interface ContainerStat {
  name: string;
  cpu: string;
  mem: string;
  cacheMem?: string;
}

export interface TelemetryPoint {
  time: string;
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
  recordsInKafka: number;
  lag: number;
  actualRps?: number;
}

export interface SystemLog {
  time: string;
  message: string;
  level: string;
  timestamp: number;
}

export interface SimulationStats {
  runId?: string;
  elapsedSec?: number;
  isRunning: boolean;
  approach: number;
  rps: number;
  appLatency: number;
  queueLatency?: number;
  p95?: number;
  p99?: number;
  flawAlert: string | null;
  dbStats: { cpu: number; io: number; wait_tasks?: number; active_locks?: number };
  containerStats: ContainerStat[];
  recordsModified: number;
  recordsFailed: number;
  recordsInKafka: number;
  lag: number;
  logs?: SystemLog[];
  newLogs?: SystemLog[];
}
