import React from 'react';
import { Settings2 } from 'lucide-react';
import type { SystemLog } from '../types';

interface SystemLogsProps {
  logs: SystemLog[];
  clearLogs: () => void;
}

const getLogEmoji = (msg: string, level: string) => {
  if (level === 'error') return '❌';
  if (level === 'warn') return '⚠️';
  const lower = msg.toLowerCase();
  if (lower.includes('clean') || lower.includes('wipe')) return '🧹';
  if (lower.includes('database') || lower.includes('sql')) return '🗄️';
  if (lower.includes('kafka') || lower.includes('topic')) return '📨';
  if (lower.includes('success')) return '✅';
  if (lower.includes('start') || lower.includes('resumed')) return '▶️';
  if (lower.includes('stop')) return '🛑';
  if (lower.includes('paus')) return '⏸️';
  if (lower.includes('connect')) return '🔌';
  if (lower.includes('worker') || lower.includes('spawn')) return '👷';
  if (lower.includes('truncate') || lower.includes('delete')) return '🗑️';
  if (lower.includes('flaw') || lower.includes('race condition')) return '🚨';
  return '💬';
};

export const SystemLogs: React.FC<SystemLogsProps> = ({ logs, clearLogs }) => {
  return (
    <div className="glass-panel grid-item-content logs-panel" style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <div className="panel-header" style={{marginBottom: '0.5rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
        <div style={{display: 'flex', alignItems: 'center', gap: '0.5rem'}}>
          <Settings2 size={20} />
          <h2 style={{margin: 0}}>System Logs</h2>
        </div>
        <button 
          onClick={clearLogs}
          style={{
            background: 'rgba(255,255,255,0.1)', border: 'none', color: '#fff', 
            padding: '4px 8px', borderRadius: '4px', cursor: 'pointer', fontSize: '0.75rem',
            transition: 'background 0.2s'
          }}
          onMouseOver={(e) => e.currentTarget.style.background = 'rgba(255,255,255,0.2)'}
          onMouseOut={(e) => e.currentTarget.style.background = 'rgba(255,255,255,0.1)'}
        >
          Clear
        </button>
      </div>
      <div className="logs-content" style={{ flex: 1, overflowY: 'auto' }}>
        {logs.length === 0 ? <div className="log-line empty">No activity yet...</div> : 
          logs.map((log, i) => {
            const timeStr = log.timestamp ? new Date(log.timestamp).toLocaleTimeString() : log.time;
            return (
              <div key={i} className={`log-line ${log.level === 'error' ? 'text-red-400' : ''}`} style={{ display: 'flex', gap: '0.5rem', alignItems: 'flex-start', marginBottom: '4px' }}>
                <span className="log-time" style={{ whiteSpace: 'nowrap', opacity: 0.7 }}>[{timeStr}]</span>
                <span className="log-emoji" style={{ fontSize: '1.1em', lineHeight: 1 }}>{getLogEmoji(log.message, log.level)}</span>
                <span className="log-message" style={{ wordBreak: 'break-word' }}>{log.message}</span>
              </div>
            );
          })
        }
      </div>
    </div>
  );
};
