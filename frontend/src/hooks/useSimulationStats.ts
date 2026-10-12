import { useState, useEffect, useRef } from 'react';
import type { SimulationStats, TelemetryPoint, SystemLog } from '../types';
import { API_BASE } from '../constants';
import { outcomeRates } from '../metrics';

const MAX_LOGS = 500;
const MAX_HISTORY = 3600;

function toPoint(data: SimulationStats): TelemetryPoint {
  const rates = outcomeRates(data);
  return {
    time: new Date().toLocaleTimeString(),
    timestamp: Date.now(),
    appLatency: data.appLatency,
    queueLatency: data.queueLatency || 0,
    p95: data.p95 || 0,
    p99: data.p99 || 0,
    cpu: data.dbStats?.cpu || 0,
    io: data.dbStats?.io || 0,
    wait_tasks: data.dbStats?.wait_tasks || 0,
    active_locks: data.dbStats?.active_locks || 0,
    recordsModified: data.recordsModified || 0,
    recordsFailed: data.recordsFailed || 0,
    recordsLate: data.recordsLate || 0,
    recordsDropped: data.recordsDropped || 0,
    recordsInKafka: data.recordsInKafka || 0,
    lag: data.lag || 0,
    pipelineLatency: data.pipelineLatency?.avg || 0,
    pipelineP95: data.pipelineLatency?.p95 || 0,
    actualRps: data.actualRps || 0,
    targetRps: data.rps || 0,
    slaRate: rates.slaRate,
    successRate: rates.successRate,
    consumerE2eLatency: data.subscriberStats?.avgE2eLatency || 0,
    consumerEnrichmentLatency: data.subscriberStats?.avgEnrichmentLatency || 0,
    consumerLag: data.subscriberStats?.consumerLag || 0,
  };
}

export function useSimulationStats(addActionLog: (msg: string, level: string) => void) {
  const [stats, setStats] = useState<SimulationStats | null>(null);
  const [logs, setLogs] = useState<SystemLog[]>([]);
  const [history, setHistory] = useState<TelemetryPoint[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [errorCount, setErrorCount] = useState(0);
  const runIdRef = useRef<string | null | undefined>(undefined);
  const logRef = useRef(addActionLog);
  logRef.current = addActionLog;

  useEffect(() => {
    let eventSource: EventSource | null = null;
    let timeout: ReturnType<typeof setTimeout>;

    const connect = () => {
      eventSource = new EventSource(`${API_BASE}/stats/stream`);

      eventSource.onopen = () => {
        setIsConnected(true);
        setErrorCount(0);
      };

      eventSource.onmessage = (e) => {
        let data: SimulationStats;
        try {
          data = JSON.parse(e.data);
        } catch (err) {
          console.error('SSE parse error', err);
          return;
        }

        // A new run starts a fresh chart; the previous run's history stays until then.
        if (data.runId && runIdRef.current !== undefined && runIdRef.current !== data.runId && data.phase !== 'idle') {
          setHistory([]);
          logRef.current('New run detected, clearing chart history.', 'info');
        }
        if (data.runId) runIdRef.current = data.runId;
        else if (runIdRef.current === undefined) runIdRef.current = null;

        if (data.newLogs && data.newLogs.length > 0) {
          const incoming = data.newLogs;
          setLogs(prev => [...incoming, ...prev].slice(0, MAX_LOGS));
        }
        setStats(data);

        if (data.phase === 'running' || data.phase === 'paused' || data.phase === 'draining') {
          setHistory(prev => [...prev, toPoint(data)].slice(-MAX_HISTORY));
        }
      };

      eventSource.onerror = () => {
        setIsConnected(false);
        setErrorCount(prev => prev + 1);
        eventSource?.close();
        timeout = setTimeout(connect, 2000);
      };
    };

    connect();

    return () => {
      eventSource?.close();
      clearTimeout(timeout);
    };
  }, []);

  const clearLogs = async () => {
    try {
      const res = await fetch(`${API_BASE}/logs/clear`, { method: 'POST' });
      if (!res.ok) throw new Error(await res.text());
      setLogs([]);
      addActionLog('Logs cleared successfully.', 'info');
    } catch (e) {
      addActionLog(`Clear logs failed: ${String(e)}`, 'error');
    }
  };

  const [isReconnecting, setIsReconnecting] = useState(false);

  const reconnect = async () => {
    if (isReconnecting) return;
    try {
      setIsReconnecting(true);
      addActionLog('Attempting to reconnect services...', 'info');
      const res = await fetch(`${API_BASE}/system/reconnect`, { method: 'POST' });
      if (!res.ok) throw new Error(await res.text());
      addActionLog('Reconnect command sent successfully.', 'info');
    } catch (e) {
      addActionLog(`Reconnect failed: ${String(e)}`, 'error');
    } finally {
      setIsReconnecting(false);
    }
  };

  return { stats, logs, history, isConnected, errorCount, clearLogs, reconnect };
}
