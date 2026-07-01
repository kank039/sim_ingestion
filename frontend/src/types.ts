export interface ContainerStat {
  name: string;
  cpu: string;
  mem: string;
  cacheMem?: string;
}

export interface TelemetryPoint {
  time: string;
  appLatency: number;
  cpu: number;
  io: number;
  recordsModified: number;
  recordsFailed: number;
  recordsInKafka: number;
  lag: number;
}

export interface SystemLog {
  time: string;
  message: string;
  level: string;
  timestamp: number;
}

export interface SimulationStats {
  isRunning: boolean;
  approach: number;
  rps: number;
  appLatency: number;
  flawAlert: string | null;
  dbStats: any; // Or specific shape like { cpu: number, io: number }
  containerStats: ContainerStat[];
  recordsModified: number;
  recordsFailed: number;
  recordsInKafka: number;
  lag: number;
  logs: SystemLog[];
}
