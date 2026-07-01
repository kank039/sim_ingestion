import React from 'react';
import { Settings2, Play, Square, AlertTriangle } from 'lucide-react';
import { APPROACHES } from '../constants';



interface ControlPanelProps {
  approach: number;
  setApproach: (v: number) => void;
  rps: number;
  setRps: (v: number) => void;
  isGradual: boolean;
  setIsGradual: (v: boolean) => void;
  endRps: number;
  setEndRps: (v: number) => void;
  timeoutMs: number;
  setTimeoutMs: (v: number) => void;
  isRunning: boolean;
  isCleaning: boolean;
  isInsertsOnly: boolean;
  setIsInsertsOnly: (v: boolean) => void;
  cardinality: number;
  setCardinality: (v: number) => void;
  insertWeight: number;
  setInsertWeight: (v: number) => void;
  updateWeight: number;
  setUpdateWeight: (v: number) => void;
  deleteWeight: number;
  setDeleteWeight: (v: number) => void;
  numSubscribers: number;
  setNumSubscribers: (v: number) => void;
  updateSim: (approach: number, rps: number, gradual: boolean, endRps: number, isRunning: boolean, timeoutMs: number, insertsOnly: boolean, card: number, wIns: number, wUpd: number, wDel: number) => void;
  handleStartStop: (isRunning: boolean) => void;
  handlePause: () => void;
  handleInduceFailure: () => void;
}

export const ControlPanel: React.FC<ControlPanelProps> = ({
  approach, setApproach, rps, setRps, isGradual, setIsGradual,
  endRps, setEndRps, timeoutMs, setTimeoutMs, isRunning, isCleaning, updateSim, handleStartStop,
  isInsertsOnly, setIsInsertsOnly, handlePause, handleInduceFailure,
  cardinality, setCardinality, insertWeight, setInsertWeight, updateWeight, setUpdateWeight, deleteWeight, setDeleteWeight,
  numSubscribers, setNumSubscribers
}) => {
  const confirmStop = () => {
    if (isRunning) {
      if (window.confirm("Are you sure you want to stop and clean the database? This is destructive.")) {
        handleStartStop(true);
      }
    } else {
      handleStartStop(false);
    }
  };

  const onWeightChange = (type: 'insert' | 'update' | 'delete', val: number) => {
    let wIns = insertWeight, wUpd = updateWeight, wDel = deleteWeight;
    if (type === 'insert') wIns = val;
    if (type === 'update') wUpd = val;
    if (type === 'delete') wDel = val;
    
    const sum = wIns + wUpd + wDel;
    if (sum > 0) {
        wIns = wIns / sum;
        wUpd = wUpd / sum;
        wDel = wDel / sum;
    }
    
    setInsertWeight(wIns);
    setUpdateWeight(wUpd);
    setDeleteWeight(wDel);
    updateSim(approach, rps, isGradual, endRps, isRunning, timeoutMs, isInsertsOnly, cardinality, wIns, wUpd, wDel);
  };

  return (
    <div className="sidebar-control-panel">
      <div className="panel-header">
        <Settings2 size={20} />
        <h2>Control Panel</h2>
      </div>
      
      <div className="control-group">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.25rem' }}>
          <label style={{ margin: 0 }}>Load Generation (RPS)</label>
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.25rem', margin: 0, cursor: 'pointer', textTransform: 'none', letterSpacing: 'normal' }}>
            <input type="checkbox" aria-label="Gradual Ramp-Up" checked={isGradual} onChange={e => setIsGradual(e.target.checked)} /> Gradual Ramp-Up
          </label>
        </div>
        
        <div className="slider-container" style={{ marginBottom: isGradual ? '0.25rem' : '0' }}>
          <input 
            type="range" min="0" max="5000" step="50"
            aria-label="Starting RPS"
            value={rps} 
            onChange={(e) => {
              const v = Number(e.target.value);
              setRps(v);
              updateSim(approach, v, isGradual, endRps, isRunning, timeoutMs, isInsertsOnly, cardinality, insertWeight, updateWeight, deleteWeight);
            }}
          />
          <span className="slider-val">{rps} {isGradual && 'Start'}</span>
        </div>
        
        {isGradual && (
          <div className="slider-container">
            <input 
              type="range" min="0" max="5000" step="50"
              aria-label="Max RPS"
              value={endRps} 
              onChange={(e) => {
                const v = Number(e.target.value);
                setEndRps(v);
                updateSim(approach, rps, isGradual, v, isRunning, timeoutMs, isInsertsOnly, cardinality, insertWeight, updateWeight, deleteWeight);
              }}
            />
            <span className="slider-val">{endRps} Max</span>
          </div>
        )}
      </div>

      <div className="control-group">
        <label>Operation Timeout</label>
        <div className="slider-container">
          <input 
            type="range" min="300" max="5000" step="100"
            aria-label="Timeout Ms"
            value={timeoutMs} 
            onChange={(e) => {
              const v = Number(e.target.value);
              setTimeoutMs(v);
              updateSim(approach, rps, isGradual, endRps, isRunning, v, isInsertsOnly, cardinality, insertWeight, updateWeight, deleteWeight);
            }}
          />
          <span className="slider-val">{timeoutMs} ms</span>
        </div>
      </div>
      
      <div className="control-group">
        <label>Batch ID Cardinality</label>
        <select 
          value={cardinality} 
          onChange={e => {
            const v = Number(e.target.value);
            setCardinality(v);
            updateSim(approach, rps, isGradual, endRps, isRunning, timeoutMs, isInsertsOnly, v, insertWeight, updateWeight, deleteWeight);
          }}
          style={{ width: '100%', padding: '4px', background: '#333', color: '#fff', border: '1px solid #555' }}
        >
          <option value={100}>100 (High Contention)</option>
          <option value={10000}>10,000 (Medium Contention)</option>
          <option value={1000000}>1,000,000 (Low Contention)</option>
        </select>
      </div>

      <div className="control-group">
        <label style={{ display: 'flex', alignItems: 'center', gap: '0.25rem', margin: 0, cursor: 'pointer', textTransform: 'none', letterSpacing: 'normal' }}>
          <input 
            type="checkbox" 
            checked={isInsertsOnly} 
            onChange={e => {
              setIsInsertsOnly(e.target.checked);
              updateSim(approach, rps, isGradual, endRps, isRunning, timeoutMs, e.target.checked, cardinality, insertWeight, updateWeight, deleteWeight);
            }} 
          /> Inserts Only Mode
        </label>
        
        {!isInsertsOnly && (
            <div style={{ marginTop: '0.5rem', display: 'flex', gap: '0.25rem', fontSize: '0.75rem', flexDirection: 'column' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span>Insert:</span>
                    <input type="range" min="0" max="1" step="0.05" value={insertWeight} onChange={e => onWeightChange('insert', Number(e.target.value))} style={{width: '60px'}} />
                    <span>{Math.round(insertWeight * 100)}%</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span>Update:</span>
                    <input type="range" min="0" max="1" step="0.05" value={updateWeight} onChange={e => onWeightChange('update', Number(e.target.value))} style={{width: '60px'}} />
                    <span>{Math.round(updateWeight * 100)}%</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span>Delete:</span>
                    <input type="range" min="0" max="1" step="0.05" value={deleteWeight} onChange={e => onWeightChange('delete', Number(e.target.value))} style={{width: '60px'}} />
                    <span>{Math.round(deleteWeight * 100)}%</span>
                </div>
            </div>
        )}
      </div>

      <div className="control-group">
        <label>Architectural Approach</label>
        <div className="approach-selector">
          {APPROACHES.map(opt => (
            <button 
              key={opt.id}
              className={`approach-btn ${approach === opt.id ? 'active' : ''}`}
              onClick={() => {
                setApproach(opt.id);
                updateSim(opt.id, rps, isGradual, endRps, isRunning, timeoutMs, isInsertsOnly, cardinality, insertWeight, updateWeight, deleteWeight);
              }}
            >
              {opt.name}
            </button>
          ))}
        </div>
      </div>

      {approach === 5 && (
        <div className="control-group">
          <label>Number of Subscribers (Consumer Workers)</label>
          <div className="slider-container">
            <input 
              type="range" min="1" max="100" step="1"
              aria-label="Number of Subscribers"
              value={numSubscribers} 
              onChange={(e) => setNumSubscribers(Number(e.target.value))}
            />
            <span className="slider-val">{numSubscribers} workers</span>
          </div>
          <div style={{ fontSize: '0.7rem', color: '#999', marginTop: '0.25rem' }}>
            Each subscriber spins up a Kafka consumer worker that performs non-blocking multi-table JOINs for enrichment.
          </div>
        </div>
      )}

      <div style={{ display: 'flex', gap: '0.5rem' }}>
        <button 
          className={`action-btn ${isRunning ? 'stop' : 'start'} ${isCleaning ? 'cleaning' : ''}`} 
          style={{ flex: 1 }}
          onClick={confirmStop}
          disabled={isCleaning}
        >
          {isCleaning ? <><Settings2 size={16} className="spin" /> CLEANING...</> :
           isRunning ? <><Square size={16} /> STOP SIMULATION</> : 
           <><Play size={16} /> START SIMULATION</>}
        </button>

        {isRunning && (
          <button 
            className="action-btn stop"
            style={{ flex: 1, backgroundColor: '#f59e0b' }} // Orange color for pause
            onClick={() => handlePause()}
            disabled={isCleaning}
          >
            <Square size={16} /> PAUSE SIMULATION
          </button>
        )}
      </div>

      <div style={{ marginTop: '0.5rem' }}>
        <button
          className="action-btn"
          style={{ width: '100%', backgroundColor: 'var(--accent-red)', border: '1px solid #7f1d1d' }}
          onClick={handleInduceFailure}
          disabled={!isRunning || isCleaning || ![1,3,5].includes(approach)}
        >
          <AlertTriangle size={16} /> INDUCE HARD FAILURE (CDC)
        </button>
      </div>
    </div>
  );
};
