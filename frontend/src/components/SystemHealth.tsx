import React from 'react';
import { Activity, Settings2, AlertTriangle } from 'lucide-react';
import type { ContainerStat } from '../types';

interface SystemHealthProps {
  containerStats: ContainerStat[];

  reconnect: () => void;
  isConnected: boolean;
}

export const SystemHealth: React.FC<SystemHealthProps> = ({ containerStats, reconnect, isConnected }) => {
  return (
    <div className="glass-panel grid-item-content">
      <div className="panel-header" style={{marginBottom: '0.5rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
        <div style={{display: 'flex', alignItems: 'center', gap: '0.5rem'}}>
          <Activity size={20} />
          <h2 style={{margin: 0}}>System Health</h2>
          {!isConnected && <span style={{ color: 'var(--accent-red)', fontSize: '0.75rem' }}>● Disconnected</span>}
        </div>
        <button 
          onClick={reconnect}
          style={{
            background: 'rgba(255,255,255,0.1)', border: 'none', color: '#fff', 
            padding: '4px 8px', borderRadius: '4px', cursor: 'pointer', fontSize: '0.75rem',
            display: 'flex', alignItems: 'center', gap: '4px'
          }}>
          <Settings2 size={12} /> Reconnect
        </button>
      </div>
      
      {!isConnected && containerStats.length === 0 ? (
        <div className="health-list" style={{ flex: 1, overflowY: 'auto' }}>
          {[1,2,3,4].map((_, idx) => (
            <div key={idx} className="health-item skeleton" style={{ height: '40px', marginBottom: '8px', background: 'rgba(255,255,255,0.05)', borderRadius: '4px' }}></div>
          ))}
        </div>
      ) : containerStats.length > 0 ? (
        <div className="health-list" style={{ flex: 1, overflowY: 'auto' }}>
          {containerStats.map((stat, idx) => (
            <div key={idx} className="health-item">
              <div className="health-name">{stat.name}</div>
              <div className="health-bars">
                <div className="health-bar-container tooltip-trigger" title={`CPU: ${stat.cpu}${stat.cpuCores ? ` | Cores: ${stat.cpuCores.join(', ')}` : ''}`}>
                  <div className="health-label">CPU</div>
                  <div className="health-bar-track">
                    {stat.cpuCores && stat.cpuCores.length > 0 ? (
                      stat.cpuCores.map((coreUsage, i) => (
                        <div key={i} className={`health-bar cpu core-${i % 4}`} style={{ width: coreUsage, borderRight: i < stat.cpuCores!.length - 1 && coreUsage !== '0.00%' ? '1px solid rgba(0,0,0,0.2)' : 'none' }}></div>
                      ))
                    ) : (
                      <div className="health-bar cpu" style={{ width: stat.cpu.replace('%','') + '%' }}></div>
                    )}
                  </div>
                </div>
                <div className="health-bar-container tooltip-trigger" title={`MEM: ${stat.mem}${stat.cacheMem ? ` | CACHE: ${stat.cacheMem}` : ''}`}>
                  <div className="health-label">MEM</div>
                  <div className="health-bar-track">
                    <div className="health-bar mem" style={{ width: stat.mem.replace('%','') + '%' }}></div>
                    {stat.cacheMem && (
                      <div className="health-bar cache" style={{ width: stat.cacheMem.replace('%','') + '%' }}></div>
                    )}
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
  );
};
