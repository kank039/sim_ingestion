import React, { useState, useEffect, useCallback } from 'react';
import { AlertTriangle, LayoutDashboard, Check, Download, FileText, Network, Trash2 } from 'lucide-react';
import './App.css';

import { useSimulationStats } from './hooks/useSimulationStats';
import { useSimulationControl } from './hooks/useSimulationControl';
import { APPROACHES } from './constants';

import { ControlPanel } from './components/ControlPanel';
import { SystemHealth } from './components/SystemHealth';
import { MetricsGrid } from './components/MetricsGrid';
import TelemetryChart from './components/TelemetryChart';
import { SystemLogs } from './components/SystemLogs';
import { RunSummaryModal } from './components/RunSummaryModal';
import { RunHistoryModal } from './components/RunHistoryModal';
import { DataFlowModal } from './components/DataFlowModal';
import type { SystemLog } from './types';



function App() {
  const [localLogs, setLocalLogs] = useState<SystemLog[]>([]);
  const addActionLog = useCallback((message: string, level = 'info') => {
    setLocalLogs(prev => [{ time: new Date().toLocaleTimeString(), message, level, timestamp: Date.now() }, ...prev].slice(0, 100));
  }, []);

  const { stats, history, isConnected, errorCount, clearLogs, reconnect } = useSimulationStats(addActionLog);
  const {
    approach, setApproach, rps, setRps, endRps, setEndRps,
    timeoutMs, setTimeoutMs,
    isGradual, setIsGradual, isCleaning, updateSim, handleStartStop, handlePause, handleResume, handleClean,
    isInsertsOnly, setIsInsertsOnly,
    cardinality, setCardinality,
    insertWeight, setInsertWeight,
    updateWeight, setUpdateWeight,
    deleteWeight, setDeleteWeight,
    numSubscribers, setNumSubscribers,
    handleInduceFailure
  } = useSimulationControl(addActionLog);

  const [chartSpeed, setChartSpeed] = useState<'fast' | 'normal' | 'slow'>('normal');

  // LocalStorage for Sidebar
  const [sidebarOpen, setSidebarOpen] = useState(() => {
    const saved = localStorage.getItem('sidebarOpen');
    return saved !== null ? JSON.parse(saved) : false;
  });
  const [visibleComponents, setVisibleComponents] = useState<Record<string, boolean>>(() => {
    const saved = localStorage.getItem('visibleComponents');
    return saved ? JSON.parse(saved) : {
      systemHealth: true, metrics: true, telemetry: true,
    };
  });

  useEffect(() => {
    localStorage.setItem('sidebarOpen', JSON.stringify(sidebarOpen));
  }, [sidebarOpen]);

  useEffect(() => {
    localStorage.setItem('visibleComponents', JSON.stringify(visibleComponents));
  }, [visibleComponents]);

  const [selectedChartMetrics, setSelectedChartMetrics] = useState<Record<string, boolean>>(() => {
    const saved = localStorage.getItem('selectedChartMetrics');
    return saved ? JSON.parse(saved) : {
      appLatency: true, cpu: true, io: true, recordsModified: false, recordsInKafka: false, lag: false, recordsFailed: false, recordsLate: false, successRate: false, consumerE2eLatency: true, consumerEnrichmentLatency: true
    };
  });

  useEffect(() => {
    localStorage.setItem('selectedChartMetrics', JSON.stringify(selectedChartMetrics));
  }, [selectedChartMetrics]);

  const toggleChartMetric = (id: string) => {
    setSelectedChartMetrics(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const toggleComponent = (id: string) => {
    setVisibleComponents(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const downloadCSV = () => {
    if (history.length === 0) return;
    const header = Object.keys(history[0]).join(',');
    const rows = history.map(obj => Object.values(obj).join(',')).join('\n');
    const csv = `${header}\n${rows}`;
    
    const approachData = APPROACHES.find(a => a.id === approach);
    const approachName = approachData ? approachData.name.replace(/[^a-zA-Z0-9]/g, '_') : 'unknown';
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const fileName = `sim_approach_${approach}_${approachName}_${timestamp}.csv`;

    const blob = new Blob([csv], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.setAttribute('href', url);
    a.setAttribute('download', fileName);
    a.click();
    window.URL.revokeObjectURL(url);
  };

  const currentStats = history.length > 0 
    ? history[history.length - 1] 
    : { time: '', timestamp: 0, appLatency: 0, cpu: 0, io: 0, recordsModified: 0, recordsFailed: 0, recordsLate: 0, recordsInKafka: 0, lag: 0 };

  const isRunning = stats?.isRunning || false;
  const isPaused = stats?.isPaused || false;
  const flawAlert = stats?.flawAlert || null;

  const [showSummaryModal, setShowSummaryModal] = useState(false);
  const [showHistoryModal, setShowHistoryModal] = useState(false);
  const [showLogsModal, setShowLogsModal] = useState(false);
  const [showFlowModal, setShowFlowModal] = useState(false);
  const [lastRunStats, setLastRunStats] = useState<any>(null);
  const [lastRunHistory, setLastRunHistory] = useState<any[]>([]);
  const prevIsRunning = React.useRef(isRunning);

  useEffect(() => {
    if (prevIsRunning.current && !isRunning) {
        if (stats && stats.elapsedSec && stats.elapsedSec > 0) {
            setLastRunStats(stats);
            setLastRunHistory(history);
            setShowSummaryModal(true);
        }
    }
    prevIsRunning.current = isRunning;
  }, [isRunning, stats, history]);

  const handleSaveRun = async (runData: any) => {
      const res = await fetch('http://localhost:3001/api/simulate/save-run', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(runData)
      });
      if (!res.ok) throw new Error(await res.text());
  };

  return (
    <div className="dashboard-container">
      {/* Toast for connection error */}
      {errorCount >= 3 && !isConnected && (
        <div className="connection-toast">
          <AlertTriangle size={16} /> Backend Connection Lost
        </div>
      )}

      <header className="glass-header">
        <div className="header-left">
          <button 
            className="icon-btn sidebar-toggle" 
            onClick={() => setSidebarOpen(!sidebarOpen)}
            title="Toggle Sidebar"
            aria-label="Toggle Sidebar"
          >
            <LayoutDashboard size={24} />
          </button>
          <div className="header-title-container">
            <h1>Data Ingestion & Enrichment Simulation</h1>
            <div className="header-approach">
              <span className="approach-label">Approach:</span>
              <span className="approach-name">{APPROACHES.find(a => a.id === approach)?.name}</span>
            </div>
            {stats?.elapsedSec !== undefined && (
              <div className="header-approach" style={{ marginLeft: '1rem' }}>
                <span className="approach-label">Elapsed:</span>
                <span className="approach-name">
                  {Math.floor(stats.elapsedSec / 60)}m {stats.elapsedSec % 60}s
                </span>
              </div>
            )}
          </div>
        </div>
        <div className="header-right">
          <button 
            className="icon-btn" 
            onClick={() => setShowFlowModal(true)} 
            title="View Data Flow Architecture"
            aria-label="View Data Flow Architecture"
          >
            <Network size={20} />
          </button>
          <button 
            className="icon-btn" 
            onClick={() => setShowLogsModal(true)} 
            title="View System Logs"
            aria-label="View System Logs"
          >
            <FileText size={20} />
          </button>
          <button 
            className="icon-btn" 
            onClick={() => setShowHistoryModal(true)} 
            title="View Run History"
            aria-label="View Run History"
          >
            <HistoryIcon size={20} />
          </button>
          <button 
            className="icon-btn" 
            onClick={downloadCSV} 
            title="Download Telemetry CSV"
            aria-label="Download CSV"
          >
            <Download size={20} />
          </button>
          <button 
            className={`icon-btn ${isCleaning ? 'cleaning' : ''}`} 
            onClick={handleClean} 
            title="Clean Database & Kafka"
            aria-label="Clean Database and Kafka"
            disabled={isRunning || isCleaning}
          >
            <Trash2 size={20} className={isCleaning ? 'spin' : ''} />
          </button>
          <div className="status-badge">
            <span style={{ 
              display: 'inline-block', width: 8, height: 8, borderRadius: '50%', 
              backgroundColor: isConnected ? '#10b981' : '#ef4444', marginRight: 8 
            }}></span>
            {isRunning ? (isPaused ? <span className="paused-indicator">PAUSED</span> : <span className="live-indicator">LIVE</span>) : <span className="paused-indicator">STOPPED</span>}
          </div>
        </div>
      </header>

      <div className="main-layout">
        <aside className={`sidebar-panel ${sidebarOpen ? 'open' : 'closed'}`}>
          <div className="sidebar-header">
            <h3>Settings & Components</h3>
          </div>
          <div className="sidebar-content">
            <ControlPanel 
              approach={approach} setApproach={setApproach}
              rps={rps} setRps={setRps}
              endRps={endRps} setEndRps={setEndRps}
              timeoutMs={timeoutMs} setTimeoutMs={setTimeoutMs}
              isGradual={isGradual} setIsGradual={setIsGradual}
              isRunning={isRunning} isCleaning={isCleaning} isPaused={isPaused}
              isInsertsOnly={isInsertsOnly} setIsInsertsOnly={setIsInsertsOnly}
              updateSim={updateSim} handleStartStop={handleStartStop} handlePause={handlePause} handleResume={handleResume}
              cardinality={cardinality} setCardinality={setCardinality}
              insertWeight={insertWeight} setInsertWeight={setInsertWeight}
              updateWeight={updateWeight} setUpdateWeight={setUpdateWeight}
              deleteWeight={deleteWeight} setDeleteWeight={setDeleteWeight}
              numSubscribers={numSubscribers} setNumSubscribers={setNumSubscribers}
              handleInduceFailure={handleInduceFailure}
            />
            
            <div className="sidebar-section-divider"></div>
            
            <div className="sidebar-components-list">
              {[
                { id: 'systemHealth', label: 'System Health' },
                { id: 'metrics', label: 'Metrics Grid' },
                { id: 'telemetry', label: 'Telemetry Chart' },
              ].map(({id, label}) => (
              <label key={id} className="component-checkbox">
                <input 
                  type="checkbox" 
                  checked={visibleComponents[id]} 
                  onChange={() => toggleComponent(id)} 
                  aria-label={`Toggle ${label}`}
                />
                <span className="checkbox-custom">
                  {visibleComponents[id] && <Check size={14} />}
                </span>
                {label}
              </label>
            ))}
            </div>
          </div>
          <div className="sidebar-footer">
            <p className="sidebar-hint">Select components to display on the dashboard.</p>
          </div>
        </aside>
        <main className="grid-area">
          {flawAlert && (
            <div className="flaw-alert">
              <AlertTriangle size={24} />
              <div className="flaw-content">
                <h3>Functional Flaw Detected</h3>
                <p>{flawAlert}</p>
              </div>
            </div>
          )}

          {approach === 3 && (
            <div className="flaw-alert info">
              <AlertTriangle size={24} />
              <div className="flaw-content">
                <h3>Flink State Limitation Warning</h3>
                <p>Requires holding <span title="invoice_batch state is held in memory (RocksDB). Joins may fail if data arrives out of order or after State TTL expires." style={{textDecoration: 'underline dotted', cursor: 'help'}}>state in memory</span>, which can fail on late data.</p>
              </div>
            </div>
          )}

          {approach === 5 && (
            <div className="flaw-alert info">
              <AlertTriangle size={24} />
              <div className="flaw-content">
                <h3>CDC Push + Consumer Enrichment Mode</h3>
                <p>{numSubscribers} subscriber workers will consume Kafka events and perform non-blocking <span title="(billing_record → invoice_batch → subscriber_plan → subscriber_usage → rate_schedule)" style={{textDecoration: 'underline dotted', cursor: 'help'}}>multi-table JOINs</span> with NOLOCK hints.</p>
              </div>
            </div>
          )}

          <div className={`dashboard-grid ${visibleComponents['metrics'] ? 'has-metrics' : ''} ${visibleComponents['systemHealth'] ? 'has-health' : ''} ${visibleComponents['telemetry'] ? 'has-telemetry' : ''}`}>
            {visibleComponents['metrics'] && (
              <div className="grid-item-metrics">
                <MetricsGrid 
                  currentStats={currentStats} 
                  selectedChartMetrics={selectedChartMetrics}
                  toggleChartMetric={toggleChartMetric}
                  subscriberStats={stats?.subscriberStats}
                />
              </div>
            )}
            
            {visibleComponents['systemHealth'] && (
              <div className="grid-item-health">
                <SystemHealth 
                  containerStats={stats?.containerStats || []}
                  reconnect={reconnect} 
                  isConnected={isConnected} 
                />
              </div>
            )}

            {visibleComponents['telemetry'] && (
              <div className="grid-item-telemetry">
                <TelemetryChart 
                  history={history} 
                  chartSpeed={chartSpeed} 
                  setChartSpeed={setChartSpeed} 
                  selectedChartMetrics={selectedChartMetrics}
                />
              </div>
            )}
          </div>

        </main>
      </div>

      {showLogsModal && (
        <div className="logs-modal-overlay" onClick={() => setShowLogsModal(false)}>
          <div className="logs-modal-content" onClick={e => e.stopPropagation()}>
            <SystemLogs 
              logs={[...(stats?.logs || []), ...localLogs].sort((a, b) => b.timestamp - a.timestamp)} 
              clearLogs={() => {
                setLocalLogs([]);
                clearLogs();
              }} 
            />
          </div>
        </div>
      )}
      
      {showFlowModal && (
        <DataFlowModal 
          approach={approach}
          currentStats={currentStats}
          subscriberStats={stats?.subscriberStats}
          onClose={() => setShowFlowModal(false)}
        />
      )}

      {showSummaryModal && lastRunStats && (
        <RunSummaryModal 
          stats={lastRunStats} 
          history={lastRunHistory} 
          onClose={() => setShowSummaryModal(false)} 
          onSave={handleSaveRun}
        />
      )}

      {showHistoryModal && (
        <RunHistoryModal onClose={() => setShowHistoryModal(false)} />
      )}
    </div>
  );
}

// Add HistoryIcon
const HistoryIcon = ({ size }: { size: number }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="12" cy="12" r="10"></circle>
    <polyline points="12 6 12 12 16 14"></polyline>
  </svg>
);

export default App;
