import React from 'react';
import { Settings2, Play, Square } from 'lucide-react';

const APPROACHES = [
  { id: 1, name: "Database Triggers (DBA Outbox)" },
  { id: 2, name: "Transactional Outbox (Recommended)" },
  { id: 3, name: "Stream-to-Stream Join (Flink)" },
  { id: 4, name: "JDBC SMT (Interceptor)" }
];

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
  isInsertsOnly: boolean;
  setIsInsertsOnly: (v: boolean) => void;
  updateSim: (approach: number, rps: number, gradual: boolean, endRps: number, isRunning: boolean, timeoutMs: number, insertsOnly: boolean) => void;
  handleStartStop: (isRunning: boolean) => void;
}

export const ControlPanel: React.FC<ControlPanelProps> = ({
  approach, setApproach, rps, setRps, isGradual, setIsGradual,
  endRps, setEndRps, timeoutMs, setTimeoutMs, isRunning, isCleaning, updateSim, handleStartStop,
  isInsertsOnly, setIsInsertsOnly
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
              updateSim(approach, v, isGradual, endRps, isRunning, timeoutMs, isInsertsOnly);
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
                updateSim(approach, rps, isGradual, v, isRunning, timeoutMs, isInsertsOnly);
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
              updateSim(approach, rps, isGradual, endRps, isRunning, v, isInsertsOnly);
            }}
          />
          <span className="slider-val">{timeoutMs} ms</span>
        </div>
      </div>

      <div className="control-group">
        <label style={{ display: 'flex', alignItems: 'center', gap: '0.25rem', margin: 0, cursor: 'pointer', textTransform: 'none', letterSpacing: 'normal' }}>
          <input 
            type="checkbox" 
            checked={isInsertsOnly} 
            onChange={e => {
              setIsInsertsOnly(e.target.checked);
              updateSim(approach, rps, isGradual, endRps, isRunning, timeoutMs, e.target.checked);
            }} 
          /> Inserts Only Mode
        </label>
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
                updateSim(opt.id, rps, isGradual, endRps, isRunning, timeoutMs, isInsertsOnly);
              }}
            >
              {opt.name}
            </button>
          ))}
        </div>
      </div>

      <button 
        className={`action-btn ${isRunning ? 'stop' : 'start'} ${isCleaning ? 'cleaning' : ''}`} 
        onClick={confirmStop}
        disabled={isCleaning}
      >
        {isCleaning ? <><Settings2 size={16} className="spin" /> CLEANING...</> :
         isRunning ? <><Square size={16} /> STOP SIMULATION</> : 
         <><Play size={16} /> START SIMULATION</>}
      </button>
    </div>
  );
};
