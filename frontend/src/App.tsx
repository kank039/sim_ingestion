import { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { Play, Square, AlertTriangle, Settings2, Database, Cpu, HardDrive, Clock, Activity } from 'lucide-react';
import './App.css';

const API_BASE = 'http://localhost:3001/api';

function App() {
  const [isRunning, setIsRunning] = useState(false);
  const [approach, setApproach] = useState(1);
  const [rps, setRps] = useState(10);
  const [endRps, setEndRps] = useState(5000);
  const [isGradual, setIsGradual] = useState(false);
  const [chartSpeed, setChartSpeed] = useState<'fast' | 'normal' | 'slow'>('normal');
  const [history, setHistory] = useState<any[]>([]);
  const [containerStats, setContainerStats] = useState<any[]>([]);
  const [flawAlert, setFlawAlert] = useState<string | null>(null);
  const [isCleaning, setIsCleaning] = useState(false);
  const [logs, setLogs] = useState<{time: string, message: string}[]>([]);

  useEffect(() => {
    const interval = setInterval(async () => {
      try {
        const res = await fetch(`${API_BASE}/stats`);
        const data = await res.json();
        setIsRunning(data.isRunning);
        if (data.isRunning && data.rps) setRps(data.rps); // update dynamically
        setFlawAlert(data.flawAlert);
        setContainerStats(data.containerStats || []);
        setLogs(data.logs || []);
        
        if (data.isRunning) {
          setHistory(prev => {
            const newHistory = [...prev, {
              time: new Date().toLocaleTimeString(),
              appLatency: data.appLatency,
              cpu: data.dbStats.cpu,
              io: data.dbStats.io,
              recordsPushed: data.recordsPushed || 0,
              recordsInKafka: data.recordsInKafka || 0,
              lag: data.lag || 0
            }];
            return newHistory.slice(-300); // Keep last 300 points for slow view
          });
        }
      } catch (e) {
        console.error(e);
      }
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  const handleStartStop = async () => {
    if (isRunning) {
      await fetch(`${API_BASE}/simulate/stop`, { method: 'POST' });
      setIsCleaning(true);
      await fetch(`${API_BASE}/simulate/clean`, { method: 'POST' });
      setIsCleaning(false);
    } else {
      await fetch(`${API_BASE}/simulate/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ approach, rps, gradual: isGradual, endRps })
      });
    }
  };

  const currentStats = history.length > 0 ? history[history.length - 1] : { appLatency: 0, cpu: 0, io: 0, recordsPushed: 0, recordsInKafka: 0, lag: 0 };

  const getChartData = () => {
    switch (chartSpeed) {
      case 'fast': return history.slice(-30);
      case 'normal': return history.slice(-60);
      case 'slow': return history.slice(-150);
      default: return history;
    }
  };

  return (
    <div className="dashboard-container">
      <header className="glass-header">
        <h1>Data Ingestion & Enrichment Simulation</h1>
        <div className="status-badge">
          {isRunning ? <span className="live-indicator">LIVE</span> : <span className="paused-indicator">PAUSED</span>}
        </div>
      </header>

      <main className="main-content">
        <aside className="control-panel glass-panel">
          <div className="panel-header">
            <Settings2 size={20} />
            <h2>Control Panel</h2>
          </div>
          
          <div className="control-group">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.25rem' }}>
              <label style={{ margin: 0 }}>Load Generation (RPS)</label>
              <label style={{ display: 'flex', alignItems: 'center', gap: '0.25rem', margin: 0, cursor: 'pointer', textTransform: 'none', letterSpacing: 'normal' }}>
                <input type="checkbox" checked={isGradual} onChange={e => setIsGradual(e.target.checked)} /> Gradual Ramp-Up
              </label>
            </div>
            
            <div className="slider-container" style={{ marginBottom: isGradual ? '0.25rem' : '0' }}>
              <input 
                type="range" min="0" max="5000" step="50"
                value={rps} 
                onChange={(e) => {
                  setRps(Number(e.target.value));
                  if (isRunning && !isGradual) fetch(`${API_BASE}/simulate/start`, {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ approach, rps: Number(e.target.value), gradual: isGradual, endRps })
                  });
                }}
              />
              <span className="slider-val">{rps} {isGradual && 'Start'}</span>
            </div>
            
            {isGradual && (
              <div className="slider-container">
                <input 
                  type="range" min="0" max="5000" step="50"
                  value={endRps} 
                  onChange={(e) => {
                    setEndRps(Number(e.target.value));
                    if (isRunning) fetch(`${API_BASE}/simulate/start`, {
                      method: 'POST', headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({ approach, rps, gradual: isGradual, endRps: Number(e.target.value) })
                    });
                  }}
                />
                <span className="slider-val">{endRps} Max</span>
              </div>
            )}
          </div>

          <div className="control-group">
            <label>Architectural Approach</label>
            <div className="approach-selector">
              {[
                { id: 1, name: "Database Triggers (DBA Outbox)" },
                { id: 2, name: "Transactional Outbox (Recommended)" },
                { id: 3, name: "Stream-to-Stream Join (Flink)" },
                { id: 4, name: "JDBC SMT (Interceptor)" }
              ].map(opt => (
                <button 
                  key={opt.id}
                  className={`approach-btn ${approach === opt.id ? 'active' : ''}`}
                  onClick={() => {
                    setApproach(opt.id);
                    if (isRunning) fetch(`${API_BASE}/simulate/start`, {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({ approach: opt.id, rps })
                    });
                  }}
                >
                  {opt.name}
                </button>
              ))}
            </div>
          </div>

          <button 
            className={`action-btn ${isRunning ? 'stop' : 'start'} ${isCleaning ? 'cleaning' : ''}`} 
            onClick={handleStartStop}
            disabled={isCleaning}
          >
            {isCleaning ? <><Settings2 size={16} className="spin" /> CLEANING...</> :
             isRunning ? <><Square size={16} /> STOP SIMULATION</> : 
             <><Play size={16} /> START SIMULATION</>}
          </button>

          <div className="system-health">
            <div className="panel-header" style={{marginBottom: '0.5rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
              <div style={{display: 'flex', alignItems: 'center', gap: '0.5rem'}}>
                <Activity size={20} />
                <h2 style={{margin: 0}}>System Health</h2>
              </div>
              <button 
                onClick={() => fetch(`${API_BASE}/system/reconnect`, { method: 'POST' })}
                style={{
                  background: 'rgba(255,255,255,0.1)', border: 'none', color: '#fff', 
                  padding: '4px 8px', borderRadius: '4px', cursor: 'pointer', fontSize: '0.75rem',
                  display: 'flex', alignItems: 'center', gap: '4px'
                }}>
                <Settings2 size={12} /> Reconnect
              </button>
            </div>
            {containerStats.length > 0 ? (
              <div className="health-list">
                {containerStats.map((stat, idx) => (
                  <div key={idx} className="health-item">
                    <div className="health-name">{stat.name}</div>
                    <div className="health-bars">
                      <div className="health-bar-container tooltip-trigger" title={`CPU: ${stat.cpu}`}>
                        <div className="health-label">CPU</div>
                        <div className="health-bar-track">
                          <div className="health-bar cpu" style={{ width: stat.cpu.replace('%','') + '%' }}></div>
                        </div>
                      </div>
                      <div className="health-bar-container tooltip-trigger" title={`MEM: ${stat.mem}`}>
                        <div className="health-label">MEM</div>
                        <div className="health-bar-track">
                          <div className="health-bar mem" style={{ width: stat.mem.replace('%','') + '%' }}></div>
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div style={{ color: 'var(--accent-red)', fontSize: '0.85rem', textAlign: 'center', padding: '1rem', fontStyle: 'italic', background: 'rgba(0,0,0,0.2)', borderRadius: '8px' }}>
                <AlertTriangle size={14} style={{ verticalAlign: 'middle', marginRight: '4px' }}/>
                Containers Disconnected
              </div>
            )}
          </div>
        </aside>

        <section className="visualization-area">
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

          <div className="stats-grid">
            <div className="stat-card glass-panel">
              <Clock className="stat-icon" />
              <div className="stat-value">{currentStats.appLatency} <span className="stat-unit">ms</span></div>
              <div className="stat-label">Application Latency</div>
            </div>
            <div className="stat-card glass-panel">
              <Cpu className="stat-icon" />
              <div className="stat-value">{currentStats.cpu} <span className="stat-unit">%</span></div>
              <div className="stat-label">SQL Server CPU</div>
            </div>
            <div className="stat-card glass-panel">
              <HardDrive className="stat-icon" />
              <div className="stat-value">{currentStats.io} <span className="stat-unit">MB/s</span></div>
              <div className="stat-label">SQL Server I/O</div>
            </div>
            <div className="stat-card glass-panel">
              <Database className="stat-icon" />
              <div className="stat-value">{currentStats.recordsPushed}</div>
              <div className="stat-label">Records Pushed</div>
            </div>
            <div className="stat-card glass-panel" style={{ color: 'var(--accent-blue)' }}>
              <Activity className="stat-icon" />
              <div className="stat-value">{currentStats.recordsInKafka}</div>
              <div className="stat-label">Records in Kafka</div>
            </div>
            <div className="stat-card glass-panel" style={{ color: currentStats.lag > 1000 ? 'var(--accent-red)' : 'var(--accent-green)' }}>
              <AlertTriangle className="stat-icon" />
              <div className="stat-value">{currentStats.lag}</div>
              <div className="stat-label">Pipeline Lag</div>
            </div>
          </div>

          <div className="charts-container glass-panel">
            <div className="chart-header">
              <h3>Performance Telemetry</h3>
              <div className="speed-toggles">
                <button className={chartSpeed === 'slow' ? 'active' : ''} onClick={() => setChartSpeed('slow')}>Slow</button>
                <button className={chartSpeed === 'normal' ? 'active' : ''} onClick={() => setChartSpeed('normal')}>Normal</button>
                <button className={chartSpeed === 'fast' ? 'active' : ''} onClick={() => setChartSpeed('fast')}>Fast</button>
              </div>
            </div>
            <div className="chart-wrapper">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={getChartData()}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                  <XAxis dataKey="time" stroke="#888" />
                  <YAxis stroke="#888" />
                  <Tooltip contentStyle={{ backgroundColor: '#111', border: '1px solid #333' }} />
                  <Legend />
                  <Line type="monotone" dataKey="appLatency" name="App Latency (ms)" stroke="#3b82f6" strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="cpu" name="CPU (%)" stroke="#ef4444" strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="io" name="I/O (MB/s)" stroke="#10b981" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        </section>

        <div className="logs-panel glass-panel">
          <div className="panel-header" style={{marginBottom: '0.5rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
            <div style={{display: 'flex', alignItems: 'center', gap: '0.5rem'}}>
              <Settings2 size={20} />
              <h2 style={{margin: 0}}>System Logs</h2>
            </div>
            <button 
              onClick={() => {
                fetch(`${API_BASE}/logs/clear`, { method: 'POST' });
                setLogs([]);
              }}
              style={{
                background: 'rgba(255,255,255,0.1)', border: 'none', color: '#fff', 
                padding: '4px 8px', borderRadius: '4px', cursor: 'pointer', fontSize: '0.75rem'
              }}>
              Clear
            </button>
          </div>
          <div className="logs-content">
            {logs.length === 0 ? <div className="log-line empty">No activity yet...</div> : 
              logs.map((log, i) => (
                <div key={i} className="log-line">
                  <span className="log-time">[{log.time}]</span> {log.message}
                </div>
              ))
            }
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;
