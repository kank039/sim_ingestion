import React, { useState, useEffect } from 'react';
import { X, History, Download } from 'lucide-react';

const API_BASE = 'http://localhost:3001/api';

interface RunHistoryModalProps {
  onClose: () => void;
}

export const RunHistoryModal: React.FC<RunHistoryModalProps> = ({ onClose }) => {
  const [runs, setRuns] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API_BASE}/results`)
      .then(res => res.json())
      .then(data => {
        setRuns(data);
        setLoading(false);
      })
      .catch(err => {
        console.error(err);
        setLoading(false);
      });
  }, []);

  return (
    <div style={{
      position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
      backgroundColor: 'rgba(0,0,0,0.8)', zIndex: 1000,
      display: 'flex', justifyContent: 'center', alignItems: 'center'
    }}>
      <div className="glass-panel" style={{ width: '800px', maxWidth: '90%', maxHeight: '90vh', padding: '2rem', position: 'relative', display: 'flex', flexDirection: 'column' }}>
        <button 
          onClick={onClose}
          style={{ position: 'absolute', top: '1rem', right: '1rem', background: 'none', border: 'none', color: '#fff', cursor: 'pointer' }}
        >
          <X size={24} />
        </button>
        
        <h2 style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginTop: 0 }}>
          <History size={24} color="var(--accent-blue)" /> Run History
        </h2>
        
        <div style={{ flex: 1, overflowY: 'auto', marginTop: '1rem' }}>
          {loading ? (
            <div>Loading...</div>
          ) : runs.length === 0 ? (
            <div style={{ color: '#aaa', textAlign: 'center', padding: '2rem' }}>No runs saved yet.</div>
          ) : (
            <table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'left' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
                  <th style={{ padding: '0.5rem' }}>Date</th>
                  <th style={{ padding: '0.5rem' }}>Approach</th>
                  <th style={{ padding: '0.5rem' }}>RPS</th>
                  <th style={{ padding: '0.5rem' }}>Duration</th>
                  <th style={{ padding: '0.5rem' }}>Total Recs</th>
                  <th style={{ padding: '0.5rem' }}>p95 Latency</th>
                  <th style={{ padding: '0.5rem' }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {runs.map(run => (
                  <tr key={run.runId} style={{ borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
                    <td style={{ padding: '0.5rem' }}>{new Date(run.timestamp).toLocaleString()}</td>
                    <td style={{ padding: '0.5rem' }}>{run.approach}</td>
                    <td style={{ padding: '0.5rem' }}>{run.rps}</td>
                    <td style={{ padding: '0.5rem' }}>{Math.floor(run.elapsedSec / 60)}m {run.elapsedSec % 60}s</td>
                    <td style={{ padding: '0.5rem' }}>{run.recordsModified.toLocaleString()}</td>
                    <td style={{ padding: '0.5rem' }}>{run.p95} ms</td>
                    <td style={{ padding: '0.5rem' }}>
                      <a href={`${API_BASE}/results/${run.runId}`} target="_blank" rel="noreferrer" style={{ color: 'var(--accent-blue)', textDecoration: 'none', display: 'flex', alignItems: 'center', gap: '4px' }}>
                        <Download size={14} /> JSON
                      </a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
};
