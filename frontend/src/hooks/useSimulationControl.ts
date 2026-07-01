import { useState, useCallback, useRef } from 'react';

const API_BASE = 'http://localhost:3001/api';

export function useSimulationControl(addActionLog: (msg: string, level: string) => void, initialApproach = 1, initialRps = 10) {
  const [approach, setApproach] = useState(initialApproach);
  const [rps, setRps] = useState(initialRps);
  const [endRps, setEndRps] = useState(5000);
  const [timeoutMs, setTimeoutMs] = useState(3000);
  const [isGradual, setIsGradual] = useState(false);
  const [isCleaning, setIsCleaning] = useState(false);
  const [isInsertsOnly, setIsInsertsOnly] = useState(true);
  
  const [cardinality, setCardinality] = useState(100);
  const [insertWeight, setInsertWeight] = useState(0.5);
  const [updateWeight, setUpdateWeight] = useState(0.3);
  const [deleteWeight, setDeleteWeight] = useState(0.2);
  const [numSubscribers, setNumSubscribers] = useState(5);
  
  const debounceRef = useRef<ReturnType<typeof setTimeout>>();

  const updateSim = useCallback((newApproach: number, newRps: number, gradual: boolean, maxRps: number, isRunning: boolean, newTimeoutMs: number, insertsOnly: boolean, card: number, wIns: number, wUpd: number, wDel: number) => {
    if (!isRunning) return;
    if (debounceRef.current) clearTimeout(debounceRef.current);
    
    debounceRef.current = setTimeout(async () => {
      try {
        const res = await fetch(`${API_BASE}/simulate/start`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ approach: newApproach, rps: newRps, gradual, endRps: maxRps, timeoutMs: newTimeoutMs, insertsOnly, cardinality: card, insertWeight: wIns, updateWeight: wUpd, deleteWeight: wDel, numSubscribers })
        });
        if (!res.ok) throw new Error(await res.text());
        addActionLog(`Updated simulation (Approach ${newApproach}, RPS ${newRps})`, 'info');
      } catch (err) {
        addActionLog(`Failed to update simulation: ${String(err)}`, 'error');
      }
    }, 250);
  }, [addActionLog]);

  const handleStartStop = async (isRunning: boolean) => {
    try {
      if (isRunning) {
        addActionLog('Stopping simulation...', 'info');
        let res = await fetch(`${API_BASE}/simulate/stop`, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        
        setIsCleaning(true);
        addActionLog('Cleaning database...', 'info');
        res = await fetch(`${API_BASE}/simulate/clean`, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        setIsCleaning(false);
        addActionLog('Simulation stopped and cleaned successfully.', 'info');
      } else {
        addActionLog('Starting simulation...', 'info');
        const res = await fetch(`${API_BASE}/simulate/start`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ approach, rps, gradual: isGradual, endRps, timeoutMs, insertsOnly: isInsertsOnly, cardinality, insertWeight, updateWeight, deleteWeight, numSubscribers })
        });
        if (!res.ok) throw new Error(await res.text());
        addActionLog('Simulation started successfully.', 'info');
      }
    } catch (e) {
      setIsCleaning(false);
      addActionLog(`Action failed: ${String(e)}`, 'error');
    }
  };

  const handlePause = async () => {
    try {
      addActionLog('Pausing simulation...', 'info');
      const res = await fetch(`${API_BASE}/simulate/pause`, { method: 'POST' });
      if (!res.ok) throw new Error(await res.text());
      addActionLog('Simulation paused successfully.', 'info');
    } catch (e) {
      addActionLog(`Pause failed: ${String(e)}`, 'error');
    }
  };

  const handleResume = async () => {
    try {
      addActionLog('Resuming simulation...', 'info');
      const res = await fetch(`${API_BASE}/simulate/resume`, { method: 'POST' });
      if (!res.ok) throw new Error(await res.text());
      addActionLog('Simulation resumed successfully.', 'info');
    } catch (e) {
      addActionLog(`Resume failed: ${String(e)}`, 'error');
    }
  };

  const handleInduceFailure = async () => {
    if (window.confirm("Are you sure you want to forcefully restart the Debezium container to simulate a failure?")) {
      try {
        addActionLog('Inducing hard failure...', 'error');
        const res = await fetch(`${API_BASE}/system/induce-failure`, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        addActionLog('Hard failure induced.', 'info');
      } catch (e) {
        addActionLog(`Failure induction failed: ${String(e)}`, 'error');
      }
    }
  };

  return {
    approach, setApproach,
    rps, setRps,
    endRps, setEndRps,
    timeoutMs, setTimeoutMs,
    isGradual, setIsGradual,
    isCleaning,
    updateSim, handleStartStop, handlePause, handleResume,
    isInsertsOnly, setIsInsertsOnly,
    cardinality, setCardinality,
    insertWeight, setInsertWeight,
    updateWeight, setUpdateWeight,
    deleteWeight, setDeleteWeight,
    numSubscribers, setNumSubscribers,
    handleInduceFailure
  };
}
