import { useState, useEffect, useCallback } from 'react';
import { AlertTriangle, LayoutDashboard, Check, Download } from 'lucide-react';
import './App.css';

import { useSimulationStats } from './hooks/useSimulationStats';
import { useSimulationControl } from './hooks/useSimulationControl';

import { ControlPanel } from './components/ControlPanel';
import { SystemHealth } from './components/SystemHealth';
import { MetricsGrid } from './components/MetricsGrid';
import TelemetryChart from './components/TelemetryChart';
import { SystemLogs } from './components/SystemLogs';
import type { SystemLog } from './types';

const APPROACHES = [
  { id: 1, name: "Database Triggers (DBA Outbox)" },
  { id: 2, name: "Transactional Outbox (Recommended)" },
  { id: 3, name: "Stream-to-Stream Join (Flink)" },
  { id: 4, name: "JDBC SMT (Interceptor)" }
];

function App() {
  const [localLogs, setLocalLogs] = useState<SystemLog[]>([]);
  const addActionLog = useCallback((message: string, level = 'info') => {
    setLocalLogs(prev => [{ time: new Date().toLocaleTimeString(), message, level, timestamp: Date.now() }, ...prev].slice(0, 100));
  }, []);

  const { stats, history, isConnected, errorCount, clearLogs, reconnect } = useSimulationStats(addActionLog);
  const {
    approach, setApproach, rps, setRps, endRps, setEndRps,
    timeoutMs, setTimeoutMs,
    isGradual, setIsGradual, isCleaning, updateSim, handleStartStop
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
      systemHealth: true, metrics: true, telemetry: true, logs: true,
    };
  });

  useEffect(() => {
    localStorage.setItem('sidebarOpen', JSON.stringify(sidebarOpen));
  }, [sidebarOpen]);

  useEffect(() => {
    localStorage.setItem('visibleComponents', JSON.stringify(visibleComponents));
  }, [visibleComponents]);

  const toggleComponent = (id: string) => {
    setVisibleComponents(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const downloadCSV = () => {
    if (history.length === 0) return;
    const header = Object.keys(history[0]).join(',');
    const rows = history.map(obj => Object.values(obj).join(',')).join('\n');
    const csv = `${header}\n${rows}`;
    
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.setAttribute('href', url);
    a.setAttribute('download', 'telemetry_history.csv');
    a.click();
    window.URL.revokeObjectURL(url);
  };

  const currentStats = history.length > 0 
    ? history[history.length - 1] 
    : { time: '', appLatency: 0, cpu: 0, io: 0, recordsPushed: 0, recordsFailed: 0, recordsInKafka: 0, lag: 0 };

  const isRunning = stats?.isRunning || false;
  const flawAlert = stats?.flawAlert || null;

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
          </div>
        </div>
        
        <div className="header-right">
          <button 
            className="icon-btn" 
            onClick={downloadCSV} 
            title="Download Telemetry CSV"
            aria-label="Download CSV"
          >
            <Download size={20} />
          </button>
          <div className="status-badge">
            <span style={{ 
              display: 'inline-block', width: 8, height: 8, borderRadius: '50%', 
              backgroundColor: isConnected ? '#10b981' : '#ef4444', marginRight: 8 
            }}></span>
            {isRunning ? <span className="live-indicator">LIVE</span> : <span className="paused-indicator">PAUSED</span>}
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
              isRunning={isRunning} isCleaning={isCleaning}
              updateSim={updateSim} handleStartStop={handleStartStop}
            />
            
            <div className="sidebar-section-divider"></div>
            
            <div className="sidebar-components-list">
              {[
                { id: 'systemHealth', label: 'System Health' },
                { id: 'metrics', label: 'Metrics Grid' },
                { id: 'telemetry', label: 'Telemetry Chart' },
                { id: 'logs', label: 'System Logs' },
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
                <p>This approach requires holding `invoice_batch` state in memory (RocksDB). Joins may fail if data arrives out of order or after State TTL expires.</p>
              </div>
            </div>
          )}

          <div className="static-dashboard-grid">
            <div className="left-column">
              {visibleComponents['systemHealth'] && (
                <div className="grid-item-wrapper">
                  <SystemHealth 
                    containerStats={stats?.containerStats || []} 
                    reconnect={reconnect} 
                    isConnected={isConnected} 
                  />
                </div>
              )}
            </div>
            
            <div className="right-column">
              {visibleComponents['metrics'] && (
                <div className="grid-item-wrapper" style={{ flexShrink: 0 }}>
                  <MetricsGrid currentStats={currentStats} />
                </div>
              )}
              {visibleComponents['telemetry'] && (
                <div className="grid-item-wrapper" style={{ flex: '1 1 400px', minHeight: '400px' }}>
                  <TelemetryChart history={history} chartSpeed={chartSpeed} setChartSpeed={setChartSpeed} />
                </div>
              )}
            </div>

            {visibleComponents['logs'] && (
              <div className="grid-item-wrapper" style={{ gridColumn: '1 / -1', height: '300px' }}>
                <SystemLogs 
                  logs={[...(stats?.logs || []), ...localLogs].sort((a, b) => b.timestamp - a.timestamp)} 
                  clearLogs={() => {
                    setLocalLogs([]);
                    clearLogs();
                  }} 
                />
              </div>
            )}
          </div>
        </main>
      </div>
    </div>
  );
}

export default App;
