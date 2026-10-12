import React from 'react';
import { Settings2, Play, Square, AlertTriangle, Loader } from 'lucide-react';
import { APPROACHES } from '../constants';
import type { LiveSettings } from '../hooks/useSimulationControl';
import type { Phase } from '../types';

interface ControlPanelProps {
  phase: Phase;
  phaseDetail?: string;
  approach: number;
  setApproach: (v: number) => void;
  rps: number;
  setRps: (v: number) => void;
  isGradual: boolean;
  setIsGradual: (v: boolean) => void;
  endRps: number;
  setEndRps: (v: number) => void;
  rampStepRps: number;
  setRampStepRps: (v: number) => void;
  timeoutMs: number;
  setTimeoutMs: (v: number) => void;
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
  resetData: boolean;
  setResetData: (v: boolean) => void;
  updateLive: (settings: LiveSettings, isRunning: boolean) => void;
  handleStartStop: (isActive: boolean) => void;
  handlePause: () => void;
  handleResume: () => void;
  handleInduceFailure: () => void;
}

const selectStyle: React.CSSProperties = {
  width: '100%', padding: '0.5rem', backgroundColor: 'rgba(0, 0, 0, 0.2)', color: 'var(--text-main)',
  border: '1px solid var(--glass-border)', borderRadius: '6px', cursor: 'pointer', outline: 'none', fontSize: '0.85rem'
};

const hintStyle: React.CSSProperties = { fontSize: '0.7rem', color: '#999', marginTop: '0.25rem' };

export const ControlPanel: React.FC<ControlPanelProps> = (props) => {
  const {
    phase, phaseDetail, approach, setApproach, rps, setRps, isGradual, setIsGradual, endRps, setEndRps, rampStepRps, setRampStepRps,
    timeoutMs, setTimeoutMs, isInsertsOnly, setIsInsertsOnly, cardinality, setCardinality,
    insertWeight, setInsertWeight, updateWeight, setUpdateWeight, deleteWeight, setDeleteWeight,
    numSubscribers, setNumSubscribers, resetData, setResetData, updateLive, handleStartStop, handlePause, handleResume, handleInduceFailure,
  } = props;

  const isRunning = phase === 'running' || phase === 'paused';
  const isPaused = phase === 'paused';
  const isBusy = phase === 'provisioning' || phase === 'draining' || phase === 'cleaning' || phase === 'starting';
  // Settings that define the environment (approach, data shape, consumers) are fixed for a run.
  const runLocked = phase !== 'idle';

  const live = (overrides: Partial<LiveSettings>) => {
    updateLive({
      rps, gradual: isGradual, endRps, rampStepRps, timeoutMs, insertsOnly: isInsertsOnly,
      insertWeight, updateWeight, deleteWeight, ...overrides,
    }, isRunning);
  };

  const onWeightChange = (type: 'insert' | 'update' | 'delete', val: number) => {
    let wIns = insertWeight, wUpd = updateWeight, wDel = deleteWeight;
    if (type === 'insert') wIns = val;
    if (type === 'update') wUpd = val;
    if (type === 'delete') wDel = val;
    const sum = wIns + wUpd + wDel;
    if (sum > 0) {
      wIns /= sum;
      wUpd /= sum;
      wDel /= sum;
    }
    setInsertWeight(wIns);
    setUpdateWeight(wUpd);
    setDeleteWeight(wDel);
    live({ insertWeight: wIns, updateWeight: wUpd, deleteWeight: wDel });
  };

  return (
    <div className="sidebar-control-panel">
      <div className="panel-header">
        <Settings2 size={20} />
        <h2>Control Panel</h2>
      </div>

      <div className="control-group">
        <label>Architectural Approach</label>
        <div className="approach-selector">
          {APPROACHES.map(opt => (
            <button
              key={opt.id}
              className={`approach-btn ${approach === opt.id ? 'active' : ''}`}
              disabled={runLocked}
              title={runLocked ? 'Stop the current run to switch approach' : undefined}
              onClick={() => setApproach(opt.id)}
            >
              {opt.name}
            </button>
          ))}
        </div>
        {runLocked && <div style={hintStyle}>The approach is fixed for a run. Stop it to switch; the next run provisions a clean environment.</div>}
      </div>

      <div className="control-group">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.25rem' }}>
          <label style={{ margin: 0 }}>Load Generation (RPS)</label>
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.25rem', margin: 0, cursor: 'pointer', textTransform: 'none', letterSpacing: 'normal' }}>
            <input type="checkbox" aria-label="Gradual Ramp-Up" checked={isGradual} onChange={e => { setIsGradual(e.target.checked); live({ gradual: e.target.checked }); }} /> Gradual Ramp-Up
          </label>
        </div>

        <div className="slider-container" style={{ marginBottom: isGradual ? '0.25rem' : '0' }}>
          <input
            type="range" min="1" max="5000" step="1"
            aria-label="Starting RPS"
            value={rps}
            onChange={(e) => {
              const v = Number(e.target.value);
              setRps(v);
              live({ rps: v });
            }}
          />
          <span className="slider-val">{rps} {isGradual && 'Start'}</span>
        </div>

        {isGradual && (
          <>
            <div className="slider-container">
              <input
                type="range" min="1" max="20000" step="50"
                aria-label="Max RPS"
                value={endRps}
                onChange={(e) => {
                  const v = Number(e.target.value);
                  setEndRps(v);
                  live({ endRps: v });
                }}
              />
              <span className="slider-val">{endRps} Max</span>
            </div>
            <div className="slider-container">
              <input
                type="range" min="1" max="1000" step="1"
                aria-label="Ramp step"
                value={rampStepRps}
                onChange={(e) => {
                  const v = Number(e.target.value);
                  setRampStepRps(v);
                  live({ rampStepRps: v });
                }}
              />
              <span className="slider-val">+{rampStepRps}/s</span>
            </div>
          </>
        )}
      </div>

      <div className="control-group">
        <label>SLA Timeout</label>
        <div className="slider-container">
          <input
            type="range" min="100" max="5000" step="100"
            aria-label="Timeout Ms"
            value={timeoutMs}
            onChange={(e) => {
              const v = Number(e.target.value);
              setTimeoutMs(v);
              live({ timeoutMs: v });
            }}
          />
          <span className="slider-val">{timeoutMs} ms</span>
        </div>
        <div style={hintStyle}>Slower writes still commit but count as Late; operations queued longer than this are Dropped.</div>
      </div>

      <div className="control-group">
        <label>Batch ID Cardinality</label>
        <select
          value={cardinality}
          disabled={runLocked}
          onChange={e => setCardinality(Number(e.target.value))}
          style={selectStyle}
        >
          <option value={100}>100 (High Contention)</option>
          <option value={10000}>10,000 (Medium Contention)</option>
          <option value={1000000}>1,000,000 (Low Contention)</option>
        </select>
        <div style={hintStyle}>Reference data for every batch id is seeded before the run starts.</div>
      </div>

      <div className="control-group">
        <label style={{ display: 'flex', alignItems: 'center', gap: '0.25rem', margin: 0, cursor: 'pointer', textTransform: 'none', letterSpacing: 'normal' }}>
          <input
            type="checkbox"
            checked={isInsertsOnly}
            onChange={e => {
              setIsInsertsOnly(e.target.checked);
              live({ insertsOnly: e.target.checked });
            }}
          /> Inserts Only Mode
        </label>

        {!isInsertsOnly && (
          <div style={{ marginTop: '0.5rem', display: 'flex', gap: '0.25rem', fontSize: '0.75rem', flexDirection: 'column' }}>
            {([['insert', insertWeight], ['update', updateWeight], ['delete', deleteWeight]] as const).map(([type, value]) => (
              <div key={type} style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span style={{ textTransform: 'capitalize' }}>{type}:</span>
                <input type="range" min="0" max="1" step="0.05" value={value} onChange={e => onWeightChange(type, Number(e.target.value))} style={{ width: '60px' }} />
                <span>{Math.round(value * 100)}%</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {approach === 5 && (
        <div className="control-group">
          <label>Number of Subscribers (Consumer Workers)</label>
          <div className="slider-container">
            <input
              type="range" min="1" max="100" step="1"
              aria-label="Number of Subscribers"
              value={numSubscribers}
              disabled={runLocked}
              onChange={(e) => setNumSubscribers(Number(e.target.value))}
            />
            <span className="slider-val">{numSubscribers} subs</span>
          </div>
          <div style={hintStyle}>
            Each subscriber is its own Kafka consumer group that enriches every event with a multi-table JOIN.
          </div>
        </div>
      )}

      <div className="control-group">
        <label style={{ display: 'flex', alignItems: 'center', gap: '0.25rem', margin: 0, cursor: 'pointer', textTransform: 'none', letterSpacing: 'normal' }}>
          <input type="checkbox" checked={resetData} disabled={runLocked} onChange={e => setResetData(e.target.checked)} /> Reset write tables before run
        </label>
        <div style={hintStyle}>Starts every run from empty tables so results are comparable.</div>
      </div>

      {isBusy && (
        <div style={{ ...hintStyle, display: 'flex', gap: '0.4rem', alignItems: 'center', color: '#fbbf24', marginBottom: '0.5rem' }}>
          <Loader size={14} className="spin" /> {phase.toUpperCase()}{phaseDetail ? `: ${phaseDetail}` : ''}
        </div>
      )}

      <div style={{ display: 'flex', gap: '0.5rem' }}>
        <button
          className={`action-btn ${isRunning ? 'stop' : 'start'}`}
          style={{ flex: 1 }}
          onClick={() => handleStartStop(isRunning || phase === 'provisioning')}
          disabled={phase === 'draining' || phase === 'cleaning' || phase === 'starting'}
        >
          {isRunning || phase === 'provisioning'
            ? <><Square size={16} /> STOP SIMULATION</>
            : <><Play size={16} /> START SIMULATION</>}
        </button>

        {isRunning && (
          <button
            className="action-btn stop"
            style={{ flex: 1, backgroundColor: isPaused ? '#10b981' : '#f59e0b' }}
            onClick={() => isPaused ? handleResume() : handlePause()}
          >
            {isPaused ? <><Play size={16} /> RESUME</> : <><Square size={16} /> PAUSE</>}
          </button>
        )}
      </div>

      <div style={{ marginTop: '0.5rem' }}>
        <button
          className="action-btn"
          style={{ width: '100%', backgroundColor: 'rgba(220, 38, 38, 0.2)', color: '#fca5a5', border: '1px solid rgba(220, 38, 38, 0.5)' }}
          onClick={handleInduceFailure}
          disabled={!isRunning}
        >
          <AlertTriangle size={16} /> INDUCE HARD FAILURE (CDC)
        </button>
      </div>
    </div>
  );
};
