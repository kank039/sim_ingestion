import React, { useState, useEffect, useCallback } from 'react';
import { AlertTriangle, LayoutDashboard, Check, Download, FileText, Network, Trash2, Activity, Eraser, Wind } from 'lucide-react';
import './App.css';

import { useSimulationStats } from './hooks/useSimulationStats';
import { useSimulationControl } from './hooks/useSimulationControl';
import { API_BASE, approachName } from './constants';

import { ControlPanel } from './components/ControlPanel';
import { SystemHealth } from './components/SystemHealth';
import { MetricsGrid } from './components/MetricsGrid';
import TelemetryChart from './components/TelemetryChart';
import { SystemLogs } from './components/SystemLogs';
import { RunSummaryModal } from './components/RunSummaryModal';
import { RunHistoryModal } from './components/RunHistoryModal';
import { DataFlowModal } from './components/DataFlowModal';
import type { SystemLog, RunSummary, TelemetryPoint } from './types';



function App() {
  const [localLogs, setLocalLogs] = useState<SystemLog[]>([]);
  const addActionLog = useCallback((message: string, level = 'info') => {
    setLocalLogs(prev => [{ time: new Date().toLocaleTimeString(), message, level, timestamp: Date.now() }, ...prev].slice(0, 100));
  }, []);

  const { stats, logs: serverLogs, history, isConnected, errorCount, clearLogs, reconnect } = useSimulationStats(addActionLog);
  const {
    approach, setApproach, rps, setRps, endRps, setEndRps, rampStepRps, setRampStepRps,
    timeoutMs, setTimeoutMs, resetData, setResetData,
    isGradual, setIsGradual, isCheckingHealth, updateLive, handleStartStop, handlePause, handleResume, handleClean, handleHealthCheck, handleCleanCdc, handleCleanKafka,
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
      appLatency: true, actualRps: true, pipelineLatency: true, cpu: true, io: false, recordsModified: false, recordsInKafka: false, lag: true, recordsFailed: false, recordsLate: false, recordsDropped: false, slaRate: false, successRate: false, consumerE2eLatency: true, consumerEnrichmentLatency: true, consumerLag: false
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

  const phase = stats?.phase ?? 'starting';
  const isIdle = phase === 'idle';
  const isCleaning = phase === 'cleaning';
  const flawAlert = stats?.flawAlert || null;
  // While a run exists, show the approach the server is actually running, not the local selection.
  const displayedApproach = !isIdle && stats?.approach ? stats.approach : approach;

  const downloadCSV = () => {
    if (history.length === 0) return;
    const header = Object.keys(history[0]).join(',');
    const rows = history.map(obj => Object.values(obj).join(',')).join('\n');
    const csv = `${header}\n${rows}`;
    
    const safeName = approachName(displayedApproach).replace(/[^a-zA-Z0-9]/g, '_');
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const fileName = `sim_approach_${displayedApproach}_${safeName}_${timestamp}.csv`;

    const blob = new Blob([csv], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.setAttribute('href', url);
    a.setAttribute('download', fileName);
    a.click();
    window.URL.revokeObjectURL(url);
  };

  const currentStats: TelemetryPoint = history.length > 0
    ? history[history.length - 1]
    : { time: '', timestamp: 0, appLatency: 0, cpu: 0, io: 0, recordsModified: 0, recordsFailed: 0, recordsLate: 0, recordsDropped: 0, recordsInKafka: 0, lag: 0 };

  const [showSummaryModal, setShowSummaryModal] = useState(false);
  const [showHistoryModal, setShowHistoryModal] = useState(false);
  const [showLogsModal, setShowLogsModal] = useState(false);
  const [showFlowModal, setShowFlowModal] = useState(false);
  const [lastRunSummary, setLastRunSummary] = useState<RunSummary | null>(null);
  const [lastRunHistory, setLastRunHistory] = useState<TelemetryPoint[]>([]);
  const seenSummaryRef = React.useRef<string | null | undefined>(undefined);

  // The server publishes a summary once a run has drained; show it once per run
  // (but not for a summary that already existed when the page loaded).
  const summaryId = stats?.lastRunSummary?.runId ?? null;
  useEffect(() => {
    if (!stats) return;
    if (seenSummaryRef.current === undefined) {
      seenSummaryRef.current = summaryId;
      return;
    }
    if (summaryId && summaryId !== seenSummaryRef.current) {
      seenSummaryRef.current = summaryId;
      setLastRunSummary(stats.lastRunSummary ?? null);
      setLastRunHistory(history);
      setShowSummaryModal(true);
    }
  }, [summaryId, stats, history]);

  const handleSaveRun = async (runData: unknown) => {
      const res = await fetch(`${API_BASE}/simulate/save-run`, {
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
              <span className="approach-name">{approachName(displayedApproach)}</span>
            </div>
            {!isIdle && stats?.elapsedSec !== undefined && (
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
            title="Tear down pipeline and truncate write tables"
            aria-label="Clean Database"
            disabled={!isIdle}
          >
            <Trash2 size={20} className={isCleaning ? 'spin' : ''} />
          </button>
          <button 
            className={`icon-btn ${isCheckingHealth ? 'cleaning' : ''}`} 
            onClick={handleHealthCheck} 
            title="System Health Check"
            aria-label="System Health Check"
            disabled={isCheckingHealth || phase === 'starting'}
          >
            <Activity size={20} className={isCheckingHealth ? 'spin' : ''} />
          </button>
          <button 
            className="icon-btn" 
            onClick={handleCleanCdc} 
            title="Disable CDC on all simulator tables (drops change tables)"
            aria-label="Clear CDC Tables"
            disabled={!isIdle}
          >
            <Eraser size={20} />
          </button>
          <button 
            className="icon-btn" 
            onClick={handleCleanKafka} 
            title="Purge simulator Kafka topics"
            aria-label="Clear Kafka Topics"
            disabled={!isIdle}
          >
            <Wind size={20} />
          </button>
          <div className="status-badge">
            <span style={{ 
              display: 'inline-block', width: 8, height: 8, borderRadius: '50%', 
              backgroundColor: isConnected ? '#10b981' : '#ef4444', marginRight: 8 
            }}></span>
            {phase === 'running' ? <span className="live-indicator">LIVE</span>
              : <span className="paused-indicator">{phase === 'idle' ? 'STOPPED' : phase.toUpperCase()}</span>}
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
              phase={phase} phaseDetail={stats?.phaseDetail}
              approach={displayedApproach} setApproach={setApproach}
              rps={rps} setRps={setRps}
              endRps={endRps} setEndRps={setEndRps}
              rampStepRps={rampStepRps} setRampStepRps={setRampStepRps}
              timeoutMs={timeoutMs} setTimeoutMs={setTimeoutMs}
              isGradual={isGradual} setIsGradual={setIsGradual}
              resetData={resetData} setResetData={setResetData}
              isInsertsOnly={isInsertsOnly} setIsInsertsOnly={setIsInsertsOnly}
              updateLive={updateLive} handleStartStop={handleStartStop} handlePause={handlePause} handleResume={handleResume}
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
          {stats?.lastError && isIdle && (
            <div className="flaw-alert">
              <AlertTriangle size={24} />
              <div className="flaw-content">
                <h3>Run could not start</h3>
                <p>{stats.lastError}</p>
              </div>
            </div>
          )}

          {flawAlert && !isIdle && (
            <div className="flaw-alert">
              <AlertTriangle size={24} />
              <div className="flaw-content">
                <h3>Pipeline Alert</h3>
                <p>{flawAlert}</p>
              </div>
            </div>
          )}

          {(phase === 'provisioning' || phase === 'draining') && (
            <div className="flaw-alert info">
              <AlertTriangle size={24} />
              <div className="flaw-content">
                <h3>{phase === 'provisioning' ? 'Provisioning approach environment' : 'Draining pipeline'}</h3>
                <p>{stats?.phaseDetail}</p>
              </div>
            </div>
          )}

          {displayedApproach === 3 && (
            <div className="flaw-alert info">
              <AlertTriangle size={24} />
              <div className="flaw-content">
                <h3>Flink State Limitation Warning</h3>
                <p>Requires holding <span title="invoice_batch state is held in memory (RocksDB). Joins may fail if data arrives out of order or after State TTL expires." style={{textDecoration: 'underline dotted', cursor: 'help'}}>state in memory</span>, which can fail on late data.</p>
              </div>
            </div>
          )}

          {displayedApproach === 5 && (
            <div className="flaw-alert info">
              <AlertTriangle size={24} />
              <div className="flaw-content">
                <h3>CDC Push + Consumer Enrichment Mode</h3>
                <p>{stats?.subscriberStats?.numSubscribers ?? numSubscribers} subscribers consume Kafka events and perform non-blocking <span title="(invoice_batch → subscriber_plan → billing_record / subscriber_usage → rate_schedule)" style={{textDecoration: 'underline dotted', cursor: 'help'}}>multi-table JOINs</span> with NOLOCK hints.</p>
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
                  approach={displayedApproach}
                  pipelineLatency={stats?.pipelineLatency}
                  enrichmentMisses={stats?.enrichmentMisses}
                  smtLookupAvgMs={stats?.smtLookupAvgMs}
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
              logs={[...serverLogs, ...localLogs].sort((a, b) => b.timestamp - a.timestamp)} 
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
          approach={displayedApproach}
          trackedTopic={stats?.trackedTopic}
          currentStats={currentStats}
          subscriberStats={stats?.subscriberStats}
          onClose={() => setShowFlowModal(false)}
        />
      )}

      {showSummaryModal && lastRunSummary && (
        <RunSummaryModal 
          summary={lastRunSummary} 
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
