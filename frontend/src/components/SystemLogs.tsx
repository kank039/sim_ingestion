import React from 'react';
import { Settings2 } from 'lucide-react';
import type { SystemLog } from '../types';

interface SystemLogsProps {
  logs: SystemLog[];
  clearLogs: () => void;
}

export const SystemLogs: React.FC<SystemLogsProps> = ({ logs, clearLogs }) => {
  return (
    <div className="glass-panel grid-item-content logs-panel">
      <div className="panel-header" style={{marginBottom: '0.5rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
        <div style={{display: 'flex', alignItems: 'center', gap: '0.5rem'}}>
          <Settings2 size={20} />
          <h2 style={{margin: 0}}>System Logs</h2>
        </div>
        <button 
          onClick={clearLogs}
          style={{
            background: 'rgba(255,255,255,0.1)', border: 'none', color: '#fff', 
            padding: '4px 8px', borderRadius: '4px', cursor: 'pointer', fontSize: '0.75rem'
          }}>
          Clear
        </button>
      </div>
      <div className="logs-content" style={{ flex: 1, overflowY: 'auto' }}>
        {logs.length === 0 ? <div className="log-line empty">No activity yet...</div> : 
          logs.map((log, i) => (
            <div key={i} className={`log-line ${log.level === 'error' ? 'text-red-400' : ''}`}>
              <span className="log-time">[{log.time}]</span> {log.message}
            </div>
          ))
        }
      </div>
    </div>
  );
};
