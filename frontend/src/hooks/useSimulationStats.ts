import { useState, useEffect } from 'react';
import type { SimulationStats, TelemetryPoint } from '../types';

const API_BASE = 'http://localhost:3001/api';

export function useSimulationStats(addActionLog: (msg: string, level: string) => void) {
  const [stats, setStats] = useState<SimulationStats | null>(null);
  const [history, setHistory] = useState<TelemetryPoint[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [errorCount, setErrorCount] = useState(0);

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
        try {
          const data: SimulationStats = JSON.parse(e.data);
          
          setStats(prevStats => {
              if (prevStats?.runId !== data.runId) {
                  setHistory([]);
                  addActionLog('New run detected, clearing history.', 'info');
              }
              if (data.newLogs && data.newLogs.length > 0) {
                  data.logs = [...(prevStats?.logs || []), ...data.newLogs].slice(-500);
              } else {
                  data.logs = prevStats?.logs || [];
              }
              return data;
          });

          if (data.isRunning) {
            setHistory(prev => {
              const actualRps = prev.length > 0 ? Math.max(0, (data.recordsModified || 0) - prev[prev.length - 1].recordsModified) : 0;
              const newHistory = [...prev, {
                time: new Date().toLocaleTimeString(),
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
                recordsInKafka: data.recordsInKafka || 0,
                lag: data.lag || 0,
                actualRps
              }];
              return newHistory.slice(-3600);
            });
          }
        } catch(err) {
          console.error("SSE parse error", err);
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
      if (stats) setStats({ ...stats, logs: [] });
      addActionLog('Logs cleared successfully.', 'info');
    } catch(e) {
      addActionLog(`Clear logs failed: ${String(e)}`, 'error');
    }
  };

  const reconnect = async () => {
    try {
      addActionLog('Attempting to reconnect services...', 'info');
      const res = await fetch(`${API_BASE}/system/reconnect`, { method: 'POST' });
      if (!res.ok) throw new Error(await res.text());
      addActionLog('Reconnect command sent successfully.', 'info');
    } catch (e) {
      addActionLog(`Reconnect failed: ${String(e)}`, 'error');
    }
  };

  return { stats, history, isConnected, errorCount, clearLogs, reconnect };
}
