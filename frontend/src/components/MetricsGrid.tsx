import React, { useState, useEffect } from 'react';
import { Clock, Cpu, HardDrive, Database, Activity, AlertTriangle, XCircle, Percent, Users, Zap, GripHorizontal } from 'lucide-react';
import type { TelemetryPoint, SubscriberStats } from '../types';
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
} from '@dnd-kit/core';
import type { DragEndEvent } from '@dnd-kit/core';
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  rectSortingStrategy,
  useSortable
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';

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
  recordsFailed: "What it is: Number of operations that threw an error.\nImportance: High count indicates hard bottleneck.",
  recordsLate: "What it is: Number of operations that timed out.\nImportance: High count indicates processing delays.",
  successRate: "What it is: Percentage of requests that were successfully processed.\nImportance: Direct indicator of system stability.",
  slaRate: "What it is: Percentage of successful requests that met the timeout SLA.\nImportance: Indicator of system performance and compliance.",
  numSubscribers: "What it is: Number of active consumer workers.\nImportance: Shows horizontal scaling factor for Approach 5.",
  totalMessagesConsumed: "What it is: Total messages fully processed by consumers.\nImportance: Represents downstream throughput.",
  consumerEnrichmentLatency: "What it is: Time taken to execute multi-table JOINs against DB.\nImportance: Measures cost of doing read-side enrichment.",
  percentilesEnrichment: "What it is: 95th/99th percentile for enrichment latency.\nImportance: Shows worst-case read-side performance.",
  consumerE2eLatency: "What it is: Total time from capture to enrichment completion.\nImportance: Ultimate measure of freshness for downstream systems.",
  enrichmentsFailed: "What it is: Number of consumer enrichments that failed.\nImportance: Indicates read-side database errors or lock timeouts."
};

const DEFAULT_MAIN_ORDER = [
  'actualRps', 'appLatency', 'queueLatency', 'percentiles', 'cpu', 'io', 
  'wait_tasks', 'active_locks', 'recordsModified', 'recordsInKafka', 
  'lag', 'recordsFailed', 'recordsLate', 'slaRate', 'successRate'
];

const DEFAULT_SUBSCRIBER_ORDER = [
  'numSubscribers', 'totalMessagesConsumed', 'consumerEnrichmentLatency', 
  'percentilesEnrichment', 'consumerE2eLatency', 'enrichmentsFailed'
];

interface MetricsGridProps {
  currentStats: TelemetryPoint;
  selectedChartMetrics?: Record<string, boolean>;
  toggleChartMetric?: (id: string) => void;
  subscriberStats?: SubscriberStats;
}

const SortableMetricCard = ({ id, content, title, renderCheckbox, cardStyle }: any) => {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id });

  const style = {
    ...cardStyle,
    transform: CSS.Transform.toString(transform),
    transition,
    zIndex: isDragging ? 10 : 1,
    opacity: isDragging ? 0.8 : 1,
    position: 'relative' as const,
    cursor: 'default',
  };

  return (
    <div
      ref={setNodeRef}
      style={style}
      className="stat-card"
      title={title}
    >
      <div 
        {...attributes} 
        {...listeners} 
        style={{ position: 'absolute', top: '8px', left: '8px', cursor: 'grab', opacity: 0.3, zIndex: 2 }}
        className="drag-handle"
      >
        <GripHorizontal size={16} />
      </div>
      {renderCheckbox(id)}
      {content}
    </div>
  );
};

export const MetricsGrid: React.FC<MetricsGridProps> = ({ currentStats, selectedChartMetrics, toggleChartMetric, subscriberStats }) => {
  const totalRecords = (currentStats.recordsModified || 0) + (currentStats.recordsFailed || 0) + (currentStats.recordsLate || 0);
  const slaRate = totalRecords > 0 
    ? ((currentStats.recordsModified / totalRecords) * 100).toFixed(2) 
    : '100.00';
  const successRate = totalRecords > 0 
    ? (((currentStats.recordsModified + currentStats.recordsLate) / totalRecords) * 100).toFixed(2) 
    : '100.00';

  const renderCheckbox = (id: string) => {
    if (!selectedChartMetrics || !toggleChartMetric) return null;
    // Don't render checkbox if it's not a chartable metric (percentiles)
    if (id === 'percentiles' || id === 'percentilesEnrichment') return null;
    
    return (
      <div style={{ position: 'absolute', top: '8px', right: '8px', zIndex: 2 }}>
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

  const mainConfigs: Record<string, any> = {
    actualRps: {
      content: (
        <>
          <Activity className="stat-icon" style={{ color: 'var(--accent-green)' }} />
          <div className="stat-value">{currentStats.actualRps || 0}</div>
          <div className="stat-label">Actual RPS</div>
        </>
      ),
      style: {}
    },
    appLatency: {
      content: (
        <>
          <Clock className="stat-icon" />
          <div className="stat-value">{currentStats.appLatency} <span className="stat-unit">ms</span></div>
          <div className="stat-label">DB Latency (Avg)</div>
        </>
      ),
      style: {}
    },
    queueLatency: {
      content: (
        <>
          <Clock className="stat-icon" style={{ color: 'var(--accent-orange)' }} />
          <div className="stat-value">{currentStats.queueLatency || 0} <span className="stat-unit">ms</span></div>
          <div className="stat-label">Queue Latency</div>
        </>
      ),
      style: {}
    },
    percentiles: {
      content: (
        <>
          <div className="stat-value" style={{ fontSize: '1rem', marginTop: '0.5rem' }}>p95: {currentStats.p95 || 0}ms</div>
          <div className="stat-value" style={{ fontSize: '1rem', marginTop: '0.25rem' }}>p99: {currentStats.p99 || 0}ms</div>
          <div className="stat-label" style={{ marginTop: '0.5rem' }}>Percentiles</div>
        </>
      ),
      style: {}
    },
    cpu: {
      content: (
        <>
          <Cpu className="stat-icon" />
          <div className="stat-value">{currentStats.cpu} <span className="stat-unit">%</span></div>
          <div className="stat-label">SQL Server CPU <span style={{ fontSize: '0.75em', opacity: 0.7 }}>(1m)</span></div>
        </>
      ),
      style: {}
    },
    io: {
      content: (
        <>
          <HardDrive className="stat-icon" />
          <div className="stat-value">{currentStats.io} <span className="stat-unit">MB/s</span></div>
          <div className="stat-label">SQL Server I/O</div>
        </>
      ),
      style: {}
    },
    wait_tasks: {
      content: (
        <>
          <Database className="stat-icon" style={{ color: currentStats.wait_tasks && currentStats.wait_tasks > 10 ? 'var(--accent-red)' : 'inherit' }} />
          <div className="stat-value">{currentStats.wait_tasks || 0}</div>
          <div className="stat-label">DB Wait Tasks</div>
        </>
      ),
      style: {}
    },
    active_locks: {
      content: (
        <>
          <Database className="stat-icon" />
          <div className="stat-value">{currentStats.active_locks || 0}</div>
          <div className="stat-label">Active DB Locks</div>
        </>
      ),
      style: {}
    },
    recordsModified: {
      content: (
        <>
          <Database className="stat-icon" />
          <div className="stat-value">{currentStats.recordsModified}</div>
          <div className="stat-label">Records Modified</div>
        </>
      ),
      style: {}
    },
    recordsInKafka: {
      content: (
        <>
          <Activity className="stat-icon" />
          <div className="stat-value">{currentStats.recordsInKafka}</div>
          <div className="stat-label">Records in Kafka</div>
        </>
      ),
      style: { color: 'var(--accent-blue)' }
    },
    lag: {
      content: (
        <>
          <AlertTriangle className="stat-icon" />
          <div className="stat-value">{currentStats.lag}</div>
          <div className="stat-label">Pipeline Lag</div>
        </>
      ),
      style: { color: currentStats.lag > 1000 ? 'var(--accent-red)' : 'var(--accent-green)' }
    },
    recordsFailed: {
      content: (
        <>
          <XCircle className="stat-icon" />
          <div className="stat-value">{currentStats.recordsFailed || 0}</div>
          <div className="stat-label">Failed Records</div>
        </>
      ),
      style: { color: currentStats.recordsFailed > 0 ? 'var(--accent-red)' : 'inherit' }
    },
    recordsLate: {
      content: (
        <>
          <Clock className="stat-icon" />
          <div className="stat-value">{currentStats.recordsLate || 0}</div>
          <div className="stat-label">Late Records</div>
        </>
      ),
      style: { color: (currentStats.recordsLate || 0) > 0 ? '#fbbf24' : 'inherit' }
    },
    slaRate: {
      content: (
        <>
          <Percent className="stat-icon" />
          <div className="stat-value">{slaRate} <span className="stat-unit">%</span></div>
          <div className="stat-label">SLA %</div>
        </>
      ),
      style: { color: parseFloat(slaRate as string) < 99 ? 'var(--accent-red)' : 'var(--accent-green)' }
    },
    successRate: {
      content: (
        <>
          <Percent className="stat-icon" />
          <div className="stat-value">{successRate} <span className="stat-unit">%</span></div>
          <div className="stat-label">Success %</div>
        </>
      ),
      style: { color: parseFloat(successRate as string) < 99 ? 'var(--accent-red)' : 'var(--accent-green)' }
    }
  };

  const subscriberConfigs: Record<string, any> = subscriberStats ? {
    numSubscribers: {
      content: (
        <>
          <Users className="stat-icon" style={{ color: 'var(--accent-blue)' }} />
          <div className="stat-value">{subscriberStats.numSubscribers}</div>
          <div className="stat-label">Active Subscribers</div>
        </>
      ),
      style: {}
    },
    totalMessagesConsumed: {
      content: (
        <>
          <Activity className="stat-icon" style={{ color: 'var(--accent-green)' }} />
          <div className="stat-value">{subscriberStats.totalMessagesConsumed.toLocaleString()}</div>
          <div className="stat-label">Messages Consumed</div>
        </>
      ),
      style: {}
    },
    consumerEnrichmentLatency: {
      content: (
        <>
          <Zap className="stat-icon" style={{ color: '#a78bfa' }} />
          <div className="stat-value">{subscriberStats.avgEnrichmentLatency} <span className="stat-unit">ms</span></div>
          <div className="stat-label">Enrichment Latency (Avg)</div>
        </>
      ),
      style: {}
    },
    percentilesEnrichment: {
      content: (
        <>
          <div className="stat-value" style={{ fontSize: '1rem', marginTop: '0.5rem' }}>p95: {subscriberStats.p95EnrichmentLatency}ms</div>
          <div className="stat-value" style={{ fontSize: '1rem', marginTop: '0.25rem' }}>p99: {subscriberStats.p99EnrichmentLatency}ms</div>
          <div className="stat-label" style={{ marginTop: '0.5rem' }}>Enrichment Percentiles</div>
        </>
      ),
      style: {}
    },
    consumerE2eLatency: {
      content: (
        <>
          <Clock className="stat-icon" style={{ color: '#f472b6' }} />
          <div className="stat-value">{subscriberStats.avgE2eLatency} <span className="stat-unit">ms</span></div>
          <div className="stat-label">E2E Latency (CDC → Enrich)</div>
        </>
      ),
      style: {}
    },
    enrichmentsFailed: {
      content: (
        <>
          <XCircle className="stat-icon" />
          <div className="stat-value">{subscriberStats.enrichmentsFailed}</div>
          <div className="stat-label">Enrichments Failed</div>
        </>
      ),
      style: { color: subscriberStats.enrichmentsFailed > 0 ? 'var(--accent-red)' : 'inherit' }
    }
  } : {};

  // Load saved order from localStorage or fallback to default
  const [mainOrder, setMainOrder] = useState<string[]>(() => {
    const saved = localStorage.getItem('metricsGridMainOrder');
    if (saved) {
      try {
        const parsed = JSON.parse(saved);
        if (Array.isArray(parsed) && parsed.length === DEFAULT_MAIN_ORDER.length) {
          return parsed;
        }
      } catch (e) {}
    }
    return DEFAULT_MAIN_ORDER;
  });

  const [subscriberOrder, setSubscriberOrder] = useState<string[]>(() => {
    const saved = localStorage.getItem('metricsGridSubscriberOrder');
    if (saved) {
      try {
        const parsed = JSON.parse(saved);
        if (Array.isArray(parsed) && parsed.length === DEFAULT_SUBSCRIBER_ORDER.length) {
          return parsed;
        }
      } catch (e) {}
    }
    return DEFAULT_SUBSCRIBER_ORDER;
  });

  useEffect(() => {
    localStorage.setItem('metricsGridMainOrder', JSON.stringify(mainOrder));
  }, [mainOrder]);

  useEffect(() => {
    localStorage.setItem('metricsGridSubscriberOrder', JSON.stringify(subscriberOrder));
  }, [subscriberOrder]);

  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: {
        distance: 5,
      },
    }),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  const handleDragEndMain = (event: DragEndEvent) => {
    const { active, over } = event;
    if (over && active.id !== over.id) {
      setMainOrder((items) => {
        const oldIndex = items.indexOf(active.id as string);
        const newIndex = items.indexOf(over.id as string);
        return arrayMove(items, oldIndex, newIndex);
      });
    }
  };

  const handleDragEndSubscriber = (event: DragEndEvent) => {
    const { active, over } = event;
    if (over && active.id !== over.id) {
      setSubscriberOrder((items) => {
        const oldIndex = items.indexOf(active.id as string);
        const newIndex = items.indexOf(over.id as string);
        return arrayMove(items, oldIndex, newIndex);
      });
    }
  };

  return (
    <div className="glass-panel stats-grid-container">
      <DndContext 
        sensors={sensors}
        collisionDetection={closestCenter}
        onDragEnd={handleDragEndMain}
      >
        <div className="stats-grid">
          <SortableContext 
            items={mainOrder}
            strategy={rectSortingStrategy}
          >
            {mainOrder.map((id) => {
              const config = mainConfigs[id];
              if (!config) return null;
              return (
                <SortableMetricCard
                  key={id}
                  id={id}
                  title={METRIC_DESCRIPTIONS[id]}
                  content={config.content}
                  cardStyle={config.style}
                  renderCheckbox={renderCheckbox}
                />
              );
            })}
          </SortableContext>
        </div>
      </DndContext>

      {subscriberStats && (
        <>
          <div style={{ padding: '0.75rem 1rem 0.25rem', fontSize: '0.8rem', textTransform: 'uppercase', letterSpacing: '0.1em', color: '#888', fontWeight: 600 }}>
            Subscriber / Consumer Metrics
          </div>
          <DndContext 
            sensors={sensors}
            collisionDetection={closestCenter}
            onDragEnd={handleDragEndSubscriber}
          >
            <div className="stats-grid">
              <SortableContext 
                items={subscriberOrder}
                strategy={rectSortingStrategy}
              >
                {subscriberOrder.map((id) => {
                  const config = subscriberConfigs[id];
                  if (!config) return null;
                  return (
                    <SortableMetricCard
                      key={id}
                      id={id}
                      title={METRIC_DESCRIPTIONS[id]}
                      content={config.content}
                      cardStyle={config.style}
                      renderCheckbox={renderCheckbox}
                    />
                  );
                })}
              </SortableContext>
            </div>
          </DndContext>
        </>
      )}
    </div>
  );
};
