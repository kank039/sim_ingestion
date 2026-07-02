import React from 'react';
import { Clock, Cpu, HardDrive, Database, Activity, AlertTriangle, XCircle, Percent, Users, Zap } from 'lucide-react';
import type { TelemetryPoint, SubscriberStats } from '../types';

const METRIC_DESCRIPTIONS: Record<string, string> = {
  actualRps: "What it is: The actual Requests Per Second being processed.\nImportance: Shows real throughput compared to target.",
  appLatency: "What it is: Average round-trip time for DB operations.\nImportance: Demonstrates direct impact of approach on primary DB write path.",
  queueLatency: "What it is: Time operations spend waiting in Node.js queue.\nImportance: High values indicate DB pool exhaustion or system saturation.",
  percentiles: "What it is: 95th and 99th percentile latencies.\nImportance: Crucial for understanding tail latency and worst-case performance.",
  cpu: "What it is: CPU utilization of Docker containers.\nImportance: Identifies which component is the bottleneck under load.",
  io: "What it is: SQL Server I/O utilization.\nImportance: Helps identify excessive disk buffering.",
  wait_tasks: "What it is: Number of DB wait tasks.\nImportance: Indicates DB lock contention or resource waits.",
  active_locks: "What it is: Number of active database locks.\nImportance: Shows transaction concurrency issues.",
  recordsModified: "What it is: Cumulative number of successful database mutations.\nImportance: Represents actual throughput.",
  recordsInKafka: "What it is: Total events captured by Debezium and committed to Kafka.\nImportance: Verifies events are flowing through streaming infrastructure.",
  lag: "What it is: Difference between Records Modified and Records in Kafka.\nImportance: Critical for evaluating CDC performance (stale data).",
  recordsFailed: "What it is: Number of operations that timed out or threw an error.\nImportance: High count indicates hard bottleneck.",
  successRate: "What it is: Percentage of successful requests.\nImportance: Direct indicator of system stability.",
  numSubscribers: "What it is: Number of active consumer workers.\nImportance: Shows horizontal scaling factor for Approach 5.",
  totalMessagesConsumed: "What it is: Total messages fully processed by consumers.\nImportance: Represents downstream throughput.",
  consumerEnrichmentLatency: "What it is: Time taken to execute multi-table JOINs against DB.\nImportance: Measures cost of doing read-side enrichment.",
  percentilesEnrichment: "What it is: 95th/99th percentile for enrichment latency.\nImportance: Shows worst-case read-side performance.",
  consumerE2eLatency: "What it is: Total time from capture to enrichment completion.\nImportance: Ultimate measure of freshness for downstream systems.",
  enrichmentsFailed: "What it is: Number of consumer enrichments that failed.\nImportance: Indicates read-side database errors or lock timeouts."
};

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
        <div className="stat-card" style={{ position: 'relative' }} title={METRIC_DESCRIPTIONS.actualRps}>
          {renderCheckbox('actualRps')}
          <Activity className="stat-icon" style={{ color: 'var(--accent-green)' }} />
          <div className="stat-value">{currentStats.actualRps || 0}</div>
          <div className="stat-label">Actual RPS</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }} title={METRIC_DESCRIPTIONS.appLatency}>
          {renderCheckbox('appLatency')}
          <Clock className="stat-icon" />
          <div className="stat-value">{currentStats.appLatency} <span className="stat-unit">ms</span></div>
          <div className="stat-label">DB Latency (Avg)</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }} title={METRIC_DESCRIPTIONS.queueLatency}>
          {renderCheckbox('queueLatency')}
          <Clock className="stat-icon" style={{ color: 'var(--accent-orange)' }} />
          <div className="stat-value">{currentStats.queueLatency || 0} <span className="stat-unit">ms</span></div>
          <div className="stat-label">Queue Latency</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }} title={METRIC_DESCRIPTIONS.percentiles}>
          <div className="stat-value" style={{ fontSize: '1rem', marginTop: '0.5rem' }}>p95: {currentStats.p95 || 0}ms</div>
          <div className="stat-value" style={{ fontSize: '1rem', marginTop: '0.25rem' }}>p99: {currentStats.p99 || 0}ms</div>
          <div className="stat-label" style={{ marginTop: '0.5rem' }}>Percentiles</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }} title={METRIC_DESCRIPTIONS.cpu}>
          {renderCheckbox('cpu')}
          <Cpu className="stat-icon" />
          <div className="stat-value">{currentStats.cpu} <span className="stat-unit">%</span></div>
          <div className="stat-label">SQL Server CPU</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }} title={METRIC_DESCRIPTIONS.io}>
          {renderCheckbox('io')}
          <HardDrive className="stat-icon" />
          <div className="stat-value">{currentStats.io} <span className="stat-unit">MB/s</span></div>
          <div className="stat-label">SQL Server I/O</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }} title={METRIC_DESCRIPTIONS.wait_tasks}>
          {renderCheckbox('wait_tasks')}
          <Database className="stat-icon" style={{ color: currentStats.wait_tasks && currentStats.wait_tasks > 10 ? 'var(--accent-red)' : 'inherit' }} />
          <div className="stat-value">{currentStats.wait_tasks || 0}</div>
          <div className="stat-label">DB Wait Tasks</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }} title={METRIC_DESCRIPTIONS.active_locks}>
          {renderCheckbox('active_locks')}
          <Database className="stat-icon" />
          <div className="stat-value">{currentStats.active_locks || 0}</div>
          <div className="stat-label">Active DB Locks</div>
        </div>
        <div className="stat-card" style={{ position: 'relative' }} title={METRIC_DESCRIPTIONS.recordsModified}>
          {renderCheckbox('recordsModified')}
          <Database className="stat-icon" />
          <div className="stat-value">{currentStats.recordsModified}</div>
          <div className="stat-label">Records Modified</div>
        </div>
        <div className="stat-card" style={{ color: 'var(--accent-blue)', position: 'relative' }} title={METRIC_DESCRIPTIONS.recordsInKafka}>
          {renderCheckbox('recordsInKafka')}
          <Activity className="stat-icon" />
          <div className="stat-value">{currentStats.recordsInKafka}</div>
          <div className="stat-label">Records in Kafka</div>
        </div>
        <div className="stat-card" style={{ color: currentStats.lag > 1000 ? 'var(--accent-red)' : 'var(--accent-green)', position: 'relative' }} title={METRIC_DESCRIPTIONS.lag}>
          {renderCheckbox('lag')}
          <AlertTriangle className="stat-icon" />
          <div className="stat-value">{currentStats.lag}</div>
          <div className="stat-label">Pipeline Lag</div>
        </div>
        <div className="stat-card" style={{ color: currentStats.recordsFailed > 0 ? 'var(--accent-red)' : 'inherit', position: 'relative' }} title={METRIC_DESCRIPTIONS.recordsFailed}>
          {renderCheckbox('recordsFailed')}
          <XCircle className="stat-icon" />
          <div className="stat-value">{currentStats.recordsFailed || 0}</div>
          <div className="stat-label">Failed Records</div>
        </div>
        <div className="stat-card" style={{ color: parseFloat(successRate as string) < 99 ? 'var(--accent-red)' : 'var(--accent-green)', position: 'relative' }} title={METRIC_DESCRIPTIONS.successRate}>
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
            <div className="stat-card" style={{ position: 'relative' }} title={METRIC_DESCRIPTIONS.numSubscribers}>
              <Users className="stat-icon" style={{ color: 'var(--accent-blue)' }} />
              <div className="stat-value">{subscriberStats.numSubscribers}</div>
              <div className="stat-label">Active Subscribers</div>
            </div>
            <div className="stat-card" style={{ position: 'relative' }} title={METRIC_DESCRIPTIONS.totalMessagesConsumed}>
              <Activity className="stat-icon" style={{ color: 'var(--accent-green)' }} />
              <div className="stat-value">{subscriberStats.totalMessagesConsumed.toLocaleString()}</div>
              <div className="stat-label">Messages Consumed</div>
            </div>
            <div className="stat-card" style={{ position: 'relative' }} title={METRIC_DESCRIPTIONS.consumerEnrichmentLatency}>
              {renderCheckbox('consumerEnrichmentLatency')}
              <Zap className="stat-icon" style={{ color: '#a78bfa' }} />
              <div className="stat-value">{subscriberStats.avgEnrichmentLatency} <span className="stat-unit">ms</span></div>
              <div className="stat-label">Enrichment Latency (Avg)</div>
            </div>
            <div className="stat-card" style={{ position: 'relative' }} title={METRIC_DESCRIPTIONS.percentilesEnrichment}>
              <div className="stat-value" style={{ fontSize: '1rem', marginTop: '0.5rem' }}>p95: {subscriberStats.p95EnrichmentLatency}ms</div>
              <div className="stat-value" style={{ fontSize: '1rem', marginTop: '0.25rem' }}>p99: {subscriberStats.p99EnrichmentLatency}ms</div>
              <div className="stat-label" style={{ marginTop: '0.5rem' }}>Enrichment Percentiles</div>
            </div>
            <div className="stat-card" style={{ position: 'relative' }} title={METRIC_DESCRIPTIONS.consumerE2eLatency}>
              {renderCheckbox('consumerE2eLatency')}
              <Clock className="stat-icon" style={{ color: '#f472b6' }} />
              <div className="stat-value">{subscriberStats.avgE2eLatency} <span className="stat-unit">ms</span></div>
              <div className="stat-label">E2E Latency (CDC → Enrich)</div>
            </div>
            <div className="stat-card" style={{ color: subscriberStats.enrichmentsFailed > 0 ? 'var(--accent-red)' : 'inherit', position: 'relative' }} title={METRIC_DESCRIPTIONS.enrichmentsFailed}>
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
