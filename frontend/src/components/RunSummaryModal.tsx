import React, { useState } from 'react';
import { X, Save, Activity } from 'lucide-react';
import { APPROACHES } from '../constants';

interface RunSummaryModalProps {
  stats: any;
  history: any[];
  onClose: () => void;
  onSave: (data: any) => Promise<void>;
}

export const RunSummaryModal: React.FC<RunSummaryModalProps> = ({ stats, history, onClose, onSave }) => {
  const [isSaving, setIsSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  const totalRecords = (stats?.recordsModified || 0) + (stats?.recordsFailed || 0);
  const successRate = totalRecords > 0 
    ? ((stats.recordsModified / totalRecords) * 100).toFixed(2) 
    : '0.00';

  const handleSave = async () => {
    setIsSaving(true);
    const runData = {
      runId: stats?.runId || Date.now().toString(),
      timestamp: new Date().toISOString(),
      approach: stats?.approach,
      approachName: APPROACHES.find(a => a.id === stats?.approach)?.name,
      rps: stats?.rps,
      elapsedSec: stats?.elapsedSec,
      recordsModified: stats?.recordsModified,
      recordsFailed: stats?.recordsFailed,
      successRate,
      p95: stats?.p95,
      p99: stats?.p99,
      queueLatency: stats?.queueLatency,
      appLatency: stats?.appLatency,
      history
    };
    try {
      await onSave(runData);
      setSaved(true);
    } catch (e) {
      console.error(e);
      alert('Failed to save run');
    }
    setIsSaving(false);
  };

  return (
    <div style={{
      position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
      backgroundColor: 'rgba(0,0,0,0.8)', zIndex: 1000,
      display: 'flex', justifyContent: 'center', alignItems: 'center'
    }}>
      <div className="glass-panel" style={{ width: '500px', maxWidth: '90%', padding: '2rem', position: 'relative' }}>
        <button 
          onClick={onClose}
          style={{ position: 'absolute', top: '1rem', right: '1rem', background: 'none', border: 'none', color: '#fff', cursor: 'pointer' }}
        >
          <X size={24} />
        </button>
        
        <h2 style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginTop: 0 }}>
          <Activity size={24} color="var(--accent-blue)" /> Simulation Finished
        </h2>
        
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', margin: '2rem 0' }}>
          <div>
            <div style={{ color: '#aaa', fontSize: '0.85rem' }}>Approach</div>
            <div style={{ fontSize: '1.1rem', fontWeight: 'bold' }}>{APPROACHES.find(a => a.id === stats?.approach)?.name}</div>
          </div>
          <div>
            <div style={{ color: '#aaa', fontSize: '0.85rem' }}>Duration</div>
            <div style={{ fontSize: '1.1rem', fontWeight: 'bold' }}>{Math.floor((stats?.elapsedSec || 0) / 60)}m {(stats?.elapsedSec || 0) % 60}s</div>
          </div>
          <div>
            <div style={{ color: '#aaa', fontSize: '0.85rem' }}>Total Records</div>
            <div style={{ fontSize: '1.1rem', fontWeight: 'bold' }}>{totalRecords.toLocaleString()}</div>
          </div>
          <div>
            <div style={{ color: '#aaa', fontSize: '0.85rem' }}>Success Rate</div>
            <div style={{ fontSize: '1.1rem', fontWeight: 'bold', color: parseFloat(successRate) > 99 ? 'var(--accent-green)' : 'var(--accent-red)' }}>
              {successRate}%
            </div>
          </div>
          <div>
            <div style={{ color: '#aaa', fontSize: '0.85rem' }}>p95 Latency</div>
            <div style={{ fontSize: '1.1rem', fontWeight: 'bold' }}>{stats?.p95 || 0} ms</div>
          </div>
          <div>
            <div style={{ color: '#aaa', fontSize: '0.85rem' }}>p99 Latency</div>
            <div style={{ fontSize: '1.1rem', fontWeight: 'bold' }}>{stats?.p99 || 0} ms</div>
          </div>
        </div>

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '1rem' }}>
          <button 
            onClick={onClose}
            style={{ padding: '0.5rem 1rem', background: 'rgba(255,255,255,0.1)', color: '#fff', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
          >
            Dismiss
          </button>
          <button 
            onClick={handleSave}
            disabled={isSaving || saved}
            style={{ 
              padding: '0.5rem 1rem', 
              background: saved ? 'var(--accent-green)' : 'var(--accent-blue)', 
              color: '#fff', border: 'none', borderRadius: '4px', cursor: 'pointer',
              display: 'flex', alignItems: 'center', gap: '0.5rem'
            }}
          >
            {saved ? <><Check size={16} /> Saved</> : <><Save size={16} /> Save Run</>}
          </button>
        </div>
      </div>
    </div>
  );
};

const Check = ({ size }: { size: number }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <polyline points="20 6 9 17 4 12"></polyline>
  </svg>
);
