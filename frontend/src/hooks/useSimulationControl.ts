import { useState, useCallback, useRef } from 'react';
import { API_BASE } from '../constants';

/** Settings that can be changed while a run is active (sent to /simulate/update). */
export interface LiveSettings {
  rps: number;
  gradual: boolean;
  endRps: number;
  rampStepRps: number;
  timeoutMs: number;
  insertsOnly: boolean;
  insertWeight: number;
  updateWeight: number;
  deleteWeight: number;
}

async function post(path: string, body?: unknown) {
  const res = await fetch(`${API_BASE}${path}`, {
    method: 'POST',
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    let message = await res.text();
    try { message = JSON.parse(message).error || message; } catch { /* plain text */ }
    throw new Error(message);
  }
  return res.json().catch(() => ({}));
}

export function useSimulationControl(addActionLog: (msg: string, level: string) => void, initialApproach = 1, initialRps = 10) {
  const [approach, setApproach] = useState(initialApproach);
  const [rps, setRps] = useState(initialRps);
  const [endRps, setEndRps] = useState(5000);
  const [rampStepRps, setRampStepRps] = useState(50);
  const [timeoutMs, setTimeoutMs] = useState(3000);
  const [isGradual, setIsGradual] = useState(false);
  const [isCheckingHealth, setIsCheckingHealth] = useState(false);
  const [isInsertsOnly, setIsInsertsOnly] = useState(true);
  const [resetData, setResetData] = useState(true);

  const [cardinality, setCardinality] = useState(100);
  const [insertWeight, setInsertWeight] = useState(0.5);
  const [updateWeight, setUpdateWeight] = useState(0.3);
  const [deleteWeight, setDeleteWeight] = useState(0.2);
  const [numSubscribers, setNumSubscribers] = useState(5);

  const debounceRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  /** Applies live setting changes to the active run without restarting it. */
  const updateLive = useCallback((settings: LiveSettings, isRunning: boolean) => {
    if (!isRunning) return;
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(async () => {
      try {
        await post('/simulate/update', settings);
        addActionLog(`Updated run (target ${settings.rps} RPS${settings.gradual ? ` ramping to ${settings.endRps}` : ''}, timeout ${settings.timeoutMs}ms)`, 'info');
      } catch (err) {
        addActionLog(`Failed to update run: ${String(err)}`, 'error');
      }
    }, 250);
  }, [addActionLog]);

  const handleStartStop = async (isActive: boolean) => {
    try {
      if (isActive) {
        addActionLog('Stopping simulation (draining pipeline, then tearing down)...', 'info');
        await post('/simulate/stop');
      } else {
        addActionLog(`Starting Approach ${approach}: provisioning its pipeline...`, 'info');
        await post('/simulate/start', {
          approach, rps, gradual: isGradual, endRps, rampStepRps, timeoutMs, insertsOnly: isInsertsOnly,
          cardinality, insertWeight, updateWeight, deleteWeight, numSubscribers, resetData,
        });
      }
    } catch (e) {
      addActionLog(`Action failed: ${String(e)}`, 'error');
    }
  };

  const action = (path: string, start: string, done: string) => async () => {
    try {
      addActionLog(start, 'info');
      await post(path);
      addActionLog(done, 'info');
    } catch (e) {
      addActionLog(`${start.replace(/\.\.\.$/, '')} failed: ${String(e)}`, 'error');
    }
  };

  const handleClean = action('/simulate/clean', 'Cleaning: tearing down pipeline and truncating write tables...', 'System cleaned.');
  const handleCleanCdc = action('/simulate/clean-cdc', 'Disabling CDC on all simulator tables...', 'CDC change tables dropped.');
  const handleCleanKafka = action('/simulate/clean-kafka', 'Purging simulator Kafka topics...', 'Kafka topics purged.');
  const handlePause = action('/simulate/pause', 'Pausing simulation...', 'Simulation paused.');
  const handleResume = action('/simulate/resume', 'Resuming simulation...', 'Simulation resumed.');

  const handleHealthCheck = async () => {
    try {
      setIsCheckingHealth(true);
      addActionLog('Running system health checks...', 'info');
      const res = await fetch(`${API_BASE}/system/health-check`);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Health check failed');
      if (data.healthy) {
        addActionLog(`System is healthy and matches the expected ${data.expected ? `Approach ${data.expected.approach}` : 'idle'} state.`, 'info');
      } else {
        addActionLog(`System health degraded: ${data.errors.join('; ')}`, 'warn');
      }
    } catch (e) {
      addActionLog(`Health check failed: ${String(e)}`, 'error');
    } finally {
      setIsCheckingHealth(false);
    }
  };

  const handleInduceFailure = async () => {
    if (!window.confirm('Restart the Debezium container to simulate a hard CDC failure?')) return;
    await action('/system/induce-failure', 'Inducing hard failure...', 'Hard failure induced.')();
  };

  return {
    approach, setApproach,
    rps, setRps,
    endRps, setEndRps,
    rampStepRps, setRampStepRps,
    timeoutMs, setTimeoutMs,
    isGradual, setIsGradual,
    isCheckingHealth,
    resetData, setResetData,
    updateLive, handleStartStop, handlePause, handleResume, handleClean, handleHealthCheck, handleCleanCdc, handleCleanKafka,
    isInsertsOnly, setIsInsertsOnly,
    cardinality, setCardinality,
    insertWeight, setInsertWeight,
    updateWeight, setUpdateWeight,
    deleteWeight, setDeleteWeight,
    numSubscribers, setNumSubscribers,
    handleInduceFailure
  };
}
