import React from 'react';
import { Clock, Cpu, HardDrive, Database, Activity, AlertTriangle, XCircle, Percent, Users, Zap } from 'lucide-react';
import type { TelemetryPoint, SubscriberStats } from '../types';

interface MetricsGridProps {
  currentStats: TelemetryPoint;
  selectedChartMetrics?: Record<string, boolean>;
  toggleChartMetric?: (id: string) => void;
  subscriberStats?: SubscriberStats;
}

export const MetricsGrid: React.FC<MetricsGridProps> = ({ currentStats, selectedChartMetrics, toggleChartMetric, subscriberStats }) => {
  const totalRecords = (currentStats.recordsModified || 0) + (currentStats.recordsFailed || 0);
  const successRate = totalRecords > 0 
    ? ((currentStats.recordsModified / totalRecords) * 100).toFixed(2) 
    : '100.00';

  const renderCheckbox = (id: string) => {
    if (!selectedChartMetrics || !toggleChartMetric) return null;
    return (
      <div style={{ position: 'absolute', top: '8px', right: '8px' }}>
        <input 
          type="checkbox" 
          checked={selectedChartMetrics[id] || false} 
          onChange={() => toggleChartMetric(id)} 
          title="Show on chart"
          style={{ cursor: 'pointer' }}
        />
      </div>
    );
  };

  return (
    <div className="glass-panel stats-grid-container">
      <div className="stats-grid">
        <div className="stat-card" style={{ position: 'relative' }}>
          {renderCheckbox('actualRps')}
          <Activity className="stat-icon" style={{ color: 'var(--accent-green)' }} />
          <div className="stat-value">{currentStats.actualRps || 0}</div>
          <div className="stat-label">Actual RPS</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }}>
          {renderCheckbox('appLatency')}
          <Clock className="stat-icon" />
          <div className="stat-value">{currentStats.appLatency} <span className="stat-unit">ms</span></div>
          <div className="stat-label">DB Latency (Avg)</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }}>
          {renderCheckbox('queueLatency')}
          <Clock className="stat-icon" style={{ color: 'var(--accent-orange)' }} />
          <div className="stat-value">{currentStats.queueLatency || 0} <span className="stat-unit">ms</span></div>
          <div className="stat-label">Queue Latency</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }}>
          <div className="stat-value" style={{ fontSize: '1rem', marginTop: '0.5rem' }}>p95: {currentStats.p95 || 0}ms</div>
          <div className="stat-value" style={{ fontSize: '1rem', marginTop: '0.25rem' }}>p99: {currentStats.p99 || 0}ms</div>
          <div className="stat-label" style={{ marginTop: '0.5rem' }}>Percentiles</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }}>
          {renderCheckbox('cpu')}
          <Cpu className="stat-icon" />
          <div className="stat-value">{currentStats.cpu} <span className="stat-unit">%</span></div>
          <div className="stat-label">SQL Server CPU</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }}>
          {renderCheckbox('io')}
          <HardDrive className="stat-icon" />
          <div className="stat-value">{currentStats.io} <span className="stat-unit">MB/s</span></div>
          <div className="stat-label">SQL Server I/O</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }}>
          {renderCheckbox('wait_tasks')}
          <Database className="stat-icon" style={{ color: currentStats.wait_tasks && currentStats.wait_tasks > 10 ? 'var(--accent-red)' : 'inherit' }} />
          <div className="stat-value">{currentStats.wait_tasks || 0}</div>
          <div className="stat-label">DB Wait Tasks</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }}>
          {renderCheckbox('active_locks')}
          <Database className="stat-icon" />
          <div className="stat-value">{currentStats.active_locks || 0}</div>
          <div className="stat-label">Active DB Locks</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }}>
          {renderCheckbox('recordsModified')}
          <Database className="stat-icon" />
          <div className="stat-value">{currentStats.recordsModified}</div>
          <div className="stat-label">Records Modified</div>
        </div>
        <div className="stat-card" style={{ color: 'var(--accent-blue)', position: 'relative' }}>
          {renderCheckbox('recordsInKafka')}
          <Activity className="stat-icon" />
          <div className="stat-value">{currentStats.recordsInKafka}</div>
          <div className="stat-label">Records in Kafka</div>
        </div>
        <div className="stat-card" style={{ color: currentStats.lag > 1000 ? 'var(--accent-red)' : 'var(--accent-green)', position: 'relative' }}>
          {renderCheckbox('lag')}
          <AlertTriangle className="stat-icon" />
          <div className="stat-value">{currentStats.lag}</div>
          <div className="stat-label">Pipeline Lag</div>
        </div>
        <div className="stat-card" style={{ color: currentStats.recordsFailed > 0 ? 'var(--accent-red)' : 'inherit', position: 'relative' }}>
          {renderCheckbox('recordsFailed')}
          <XCircle className="stat-icon" />
          <div className="stat-value">{currentStats.recordsFailed || 0}</div>
          <div className="stat-label">Failed Records</div>
        </div>
        <div className="stat-card" style={{ color: parseFloat(successRate as string) < 99 ? 'var(--accent-red)' : 'var(--accent-green)', position: 'relative' }}>
          {renderCheckbox('successRate')}
          <Percent className="stat-icon" />
          <div className="stat-value">{successRate} <span className="stat-unit">%</span></div>
          <div className="stat-label">Success Rate</div>
        </div>
      </div>

      {subscriberStats && (
        <>
          <div style={{ padding: '0.75rem 1rem 0.25rem', fontSize: '0.8rem', textTransform: 'uppercase', letterSpacing: '0.1em', color: '#888', fontWeight: 600 }}>
            Subscriber / Consumer Metrics
          </div>
          <div className="stats-grid">
            <div className="stat-card" style={{ position: 'relative' }}>
              <Users className="stat-icon" style={{ color: 'var(--accent-blue)' }} />
              <div className="stat-value">{subscriberStats.numSubscribers}</div>
              <div className="stat-label">Active Subscribers</div>
            </div>
            <div className="stat-card" style={{ position: 'relative' }}>
              <Activity className="stat-icon" style={{ color: 'var(--accent-green)' }} />
              <div className="stat-value">{subscriberStats.totalMessagesConsumed.toLocaleString()}</div>
              <div className="stat-label">Messages Consumed</div>
            </div>
            <div className="stat-card" style={{ position: 'relative' }}>
              {renderCheckbox('consumerEnrichmentLatency')}
              <Zap className="stat-icon" style={{ color: '#a78bfa' }} />
              <div className="stat-value">{subscriberStats.avgEnrichmentLatency} <span className="stat-unit">ms</span></div>
              <div className="stat-label">Enrichment Latency (Avg)</div>
            </div>
            <div className="stat-card" style={{ position: 'relative' }}>
              <div className="stat-value" style={{ fontSize: '1rem', marginTop: '0.5rem' }}>p95: {subscriberStats.p95EnrichmentLatency}ms</div>
              <div className="stat-value" style={{ fontSize: '1rem', marginTop: '0.25rem' }}>p99: {subscriberStats.p99EnrichmentLatency}ms</div>
              <div className="stat-label" style={{ marginTop: '0.5rem' }}>Enrichment Percentiles</div>
            </div>
            <div className="stat-card" style={{ position: 'relative' }}>
              {renderCheckbox('consumerE2eLatency')}
              <Clock className="stat-icon" style={{ color: '#f472b6' }} />
              <div className="stat-value">{subscriberStats.avgE2eLatency} <span className="stat-unit">ms</span></div>
              <div className="stat-label">E2E Latency (CDC → Enrich)</div>
            </div>
            <div className="stat-card" style={{ color: subscriberStats.enrichmentsFailed > 0 ? 'var(--accent-red)' : 'inherit', position: 'relative' }}>
              <XCircle className="stat-icon" />
              <div className="stat-value">{subscriberStats.enrichmentsFailed}</div>
              <div className="stat-label">Enrichments Failed</div>
            </div>
          </div>
        </>
      )}
    </div>
  );
};
