import React, { useState } from 'react';
import { X, Save, Activity, Check } from 'lucide-react';
import { API_BASE } from '../constants';
import type { RunSummary, TelemetryPoint, VerifyResult } from '../types';

interface RunSummaryModalProps {
  summary: RunSummary;
  history: TelemetryPoint[];
  onClose: () => void;
  onSave: (data: unknown) => Promise<void>;
}

const Stat: React.FC<{ label: string; value: React.ReactNode; color?: string }> = ({ label, value, color }) => (
  <div>
    <div style={{ color: '#aaa', fontSize: '0.85rem' }}>{label}</div>
    <div style={{ fontSize: '1.1rem', fontWeight: 'bold', color }}>{value}</div>
  </div>
);

const good = 'var(--accent-green)';
const bad = 'var(--accent-red)';

export const RunSummaryModal: React.FC<RunSummaryModalProps> = ({ summary, history, onClose, onSave }) => {
  const [isSaving, setIsSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [verification, setVerification] = useState<VerifyResult | null>(null);
  const [verifyError, setVerifyError] = useState<string | null>(null);
  const [isVerifying, setIsVerifying] = useState(false);

  const s = summary;
  const totalAttempted = s.recordsCommitted + s.recordsFailed + s.recordsDropped;

  const handleSave = async () => {
    setIsSaving(true);
    try {
      await onSave({
        runId: s.runId,
        timestamp: s.finishedAt,
        approach: s.approach,
        approachName: s.approachName,
        summary: s,
        verification,
        history,
      });
      setSaved(true);
    } catch (e) {
      console.error(e);
      alert(`Failed to save run: ${String(e)}`);
    }
    setIsSaving(false);
  };

  const handleVerify = async () => {
    setIsVerifying(true);
    setVerification(null);
    setVerifyError(null);
    try {
      const res = await fetch(`${API_BASE}/simulate/verify`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Verification failed');
      setVerification(data);
    } catch (e) {
      setVerifyError(String(e));
    }
    setIsVerifying(false);
  };

  const fmtDur = (sec: number) => `${Math.floor(sec / 60)}m ${sec % 60}s`;

  return (
    <div style={{
      position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
      backgroundColor: 'rgba(0,0,0,0.8)', zIndex: 1000,
      display: 'flex', justifyContent: 'center', alignItems: 'center'
    }}>
      <div className="glass-panel" style={{ width: '720px', maxWidth: '92%', maxHeight: '92vh', overflowY: 'auto', padding: '2rem', position: 'relative' }}>
        <button
          onClick={onClose}
          style={{ position: 'absolute', top: '1rem', right: '1rem', background: 'none', border: 'none', color: '#fff', cursor: 'pointer' }}
        >
          <X size={24} />
        </button>

        <h2 style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginTop: 0 }}>
          <Activity size={24} color="var(--accent-blue)" /> Run Finished
        </h2>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '1rem', margin: '1.5rem 0' }}>
          <Stat label="Approach" value={s.approachName} />
          <Stat label="Duration (excl. pauses)" value={fmtDur(s.elapsedSec)} />
          <Stat label="Avg committed RPS" value={`${s.avgCommittedRps} (target ${s.finalTargetRps})`} />

          <Stat label="Attempted" value={totalAttempted.toLocaleString()} />
          <Stat label="Committed (on time / late)" value={`${s.recordsModified.toLocaleString()} / ${s.recordsLate.toLocaleString()}`} />
          <Stat label="Failed / Dropped" value={`${s.recordsFailed} / ${s.recordsDropped}`} color={s.recordsFailed + s.recordsDropped > 0 ? bad : undefined} />

          <Stat label="SLA %" value={`${s.slaRate}%`} color={s.slaRate >= 99 ? good : bad} />
          <Stat label="Success %" value={`${s.successRate}%`} color={s.successRate >= 99 ? good : bad} />
          <Stat label="Final pipeline lag" value={s.lag.toLocaleString()} color={s.lag === 0 ? good : bad} />

          <Stat label="DB latency p50 / p95 / p99" value={`${s.p50} / ${s.p95} / ${s.p99} ms`} />
          <Stat label="Commit → topic p50 / p95" value={`${s.pipelineLatency.p50} / ${s.pipelineLatency.p95} ms`} />
          <Stat label="Events delivered" value={`${s.recordsInKafka.toLocaleString()}`} />

          {s.subscriber && (
            <>
              <Stat label="Subscriber enrichments" value={s.subscriber.consumed.toLocaleString()} />
              <Stat label="Enrichment p95" value={`${Math.round(s.subscriber.enrichment.p95)} ms`} />
              <Stat label="Commit → enriched p95" value={`${Math.round(s.subscriber.e2e.p95)} ms`} />
            </>
          )}
          {s.approach === 4 && <Stat label="SMT enrichment misses" value={s.enrichmentMisses} color={s.enrichmentMisses ? bad : good} />}
          {s.approach === 3 && <Stat label="Partial dual writes" value={s.partialWrites} color={s.partialWrites ? bad : good} />}
        </div>

        {s.flawAlert && (
          <div style={{ marginBottom: '1rem', color: '#fca5a5', fontSize: '0.9rem' }}>Alert during run: {s.flawAlert}</div>
        )}

        <div style={{ margin: '1rem 0', padding: '1rem', background: 'rgba(255,255,255,0.05)', borderRadius: '8px' }}>
          <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '1rem' }}>Delivery Verification</h3>
          <div style={{ fontSize: '0.8rem', color: '#aaa', marginBottom: '0.75rem' }}>
            Re-reads this run's events on <code>{s.trackedTopic}</code> and checks completeness, duplicate deliveries
            and per-key / per-entity ordering.
          </div>
          <button
            onClick={handleVerify}
            disabled={isVerifying}
            style={{
              padding: '0.5rem 1rem', background: 'var(--accent-blue)', color: '#fff', border: 'none', borderRadius: '4px',
              cursor: isVerifying ? 'not-allowed' : 'pointer', opacity: isVerifying ? 0.5 : 1
            }}
          >
            {isVerifying ? 'Running Verification...' : 'Run Verification'}
          </button>

          {verifyError && <div style={{ marginTop: '0.75rem', color: bad }}>{verifyError}</div>}
          {verification && (
            <div style={{ marginTop: '1rem', padding: '1rem', background: 'rgba(0,0,0,0.3)', borderRadius: '4px', fontSize: '0.9rem', display: 'grid', gap: '0.3rem' }}>
              <div><strong>Events checked:</strong> {verification.messagesChecked.toLocaleString()}{!verification.complete && ' (timed out before the end of the topic)'}</div>
              <div>
                <strong>Completeness:</strong>{' '}
                <span style={{ color: verification.missing === 0 ? good : bad }}>
                  {verification.missing === 0 ? `all ${verification.expected.toLocaleString()} committed changes delivered` : `${verification.missing.toLocaleString()} of ${verification.expected.toLocaleString()} missing`}
                </span>
              </div>
              <div>
                <strong>Duplicates:</strong>{' '}
                <span style={{ color: verification.duplicates === 0 ? good : bad }}>
                  {verification.duplicates === 0 ? '0 (each change delivered once)' : `${verification.duplicates} redelivered changes (at-least-once)`}
                </span>
              </div>
              <div>
                <strong>Per-key ordering:</strong>{' '}
                <span style={{ color: verification.keyOrderViolations === 0 ? good : bad }}>
                  {verification.keyOrderViolations === 0 ? 'preserved' : `${verification.keyOrderViolations} regressions`}
                </span>
              </div>
              <div>
                <strong>Per-entity ordering:</strong>{' '}
                <span style={{ color: verification.entitiesSpreadAcrossPartitions === 0 ? good : '#fbbf24' }}>
                  {verification.entitiesSpreadAcrossPartitions === 0
                    ? `guaranteed (${verification.entitiesChecked.toLocaleString()} entities, each on one partition)`
                    : `not guaranteed: ${verification.entitiesSpreadAcrossPartitions.toLocaleString()} of ${verification.entitiesChecked.toLocaleString()} entities have events on several partitions`}
                </span>
              </div>
            </div>
          )}
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
