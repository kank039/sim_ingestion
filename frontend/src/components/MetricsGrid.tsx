import React, { useState, useEffect } from 'react';
import { Clock, Cpu, HardDrive, Database, Activity, AlertTriangle, XCircle, Percent, Users, Zap, GripHorizontal, Ban, Timer, Search } from 'lucide-react';
import type { TelemetryPoint, SubscriberStats, LatencySummary } from '../types';
import { outcomeRates } from '../metrics';
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
  actualRps: "What it is: Operations committed per second (5s rolling), next to the target rate.\nImportance: Shows real throughput compared to target.",
  appLatency: "What it is: Average DB execution time of a write operation (this second).\nImportance: Demonstrates direct impact of approach on primary DB write path.",
  queueLatency: "What it is: Time operations spend waiting in Node.js queue.\nImportance: High values indicate DB pool exhaustion or system saturation.",
  percentiles: "What it is: True 95th/99th percentile DB latency across all writer threads (merged histograms).\nImportance: Crucial for understanding tail latency and worst-case performance.",
  pipelineLatency: "What it is: Time from DB commit (CDC source timestamp) until the event is on the approach's output topic (5s rolling).\nImportance: Freshness of the stream each approach produces.",
  recordsDropped: "What it is: Operations the load generator shed before reaching the DB (queue full or waited longer than the SLA).\nImportance: The generator could not push the target rate through.",
  smtLookup: "What it is: JDBC SMT lookup time inside Kafka Connect, and events whose invoice lookup found nothing.\nImportance: Cost and correctness of synchronous enrichment.",
  cpu: "What it is: CPU utilization of Docker containers.\nImportance: Identifies which component is the bottleneck under load.",
  io: "What it is: SQL Server I/O utilization.\nImportance: Helps identify excessive disk buffering.",
  wait_tasks: "What it is: Number of DB wait tasks.\nImportance: Indicates DB lock contention or resource waits.",
  active_locks: "What it is: Number of active database locks.\nImportance: Shows transaction concurrency issues.",
  recordsModified: "What it is: Cumulative number of successful database mutations.\nImportance: Represents actual throughput.",
  recordsInKafka: "What it is: Change events of this run delivered on the approach's output topic (tombstones excluded).\nImportance: Verifies events are flowing through streaming infrastructure.",
  lag: "What it is: Committed operations whose event has not reached the output topic yet.\nImportance: Critical for evaluating CDC performance (stale data).",
  recordsFailed: "What it is: Number of operations that threw an error.\nImportance: High count indicates hard bottleneck.",
  recordsLate: "What it is: Operations that committed but took longer than the SLA timeout.\nImportance: High count indicates processing delays.",
  successRate: "What it is: Share of all attempted operations that committed (on time or late).\nImportance: Direct indicator of system stability.",
  slaRate: "What it is: Share of all attempted operations that committed within the SLA timeout.\nImportance: Indicator of system performance and compliance.",
  numSubscribers: "What it is: Number of active consumer workers.\nImportance: Shows horizontal scaling factor for Approach 5.",
  totalMessagesConsumed: "What it is: Total messages fully processed by consumers.\nImportance: Represents downstream throughput.",
  consumerEnrichmentLatency: "What it is: Time taken to execute multi-table JOINs against DB.\nImportance: Measures cost of doing read-side enrichment.",
  percentilesEnrichment: "What it is: 95th/99th percentile for enrichment latency.\nImportance: Shows worst-case read-side performance.",
  consumerE2eLatency: "What it is: Time from DB commit to enrichment completed by a subscriber.\nImportance: Ultimate measure of freshness for downstream systems.",
  consumerLag: "What it is: Events on the topic not yet processed, summed over all subscribers.\nImportance: Shows whether consumers keep up.",
  enrichmentsFailed: "What it is: Number of consumer enrichments that failed.\nImportance: Indicates read-side database errors or lock timeouts."
};

const DEFAULT_MAIN_ORDER = [
  'actualRps', 'appLatency', 'queueLatency', 'percentiles', 'pipelineLatency', 'cpu', 'io',
  'wait_tasks', 'active_locks', 'recordsModified', 'recordsInKafka',
  'lag', 'recordsFailed', 'recordsLate', 'recordsDropped', 'slaRate', 'successRate', 'smtLookup'
];

const DEFAULT_SUBSCRIBER_ORDER = [
  'numSubscribers', 'totalMessagesConsumed', 'consumerEnrichmentLatency',
  'percentilesEnrichment', 'consumerE2eLatency', 'consumerLag', 'enrichmentsFailed'
];

/** Keeps the user's saved card order but adds cards introduced since it was saved. */
function loadOrder(key: string, defaults: string[]): string[] {
  try {
    const saved = JSON.parse(localStorage.getItem(key) || 'null');
    if (Array.isArray(saved)) {
      const known = saved.filter((id: unknown): id is string => typeof id === 'string' && defaults.includes(id));
      return [...known, ...defaults.filter(id => !known.includes(id))];
    }
  } catch {
    // fall through to defaults
  }
  return defaults;
}

interface MetricsGridProps {
  currentStats: TelemetryPoint;
  selectedChartMetrics?: Record<string, boolean>;
  toggleChartMetric?: (id: string) => void;
  subscriberStats?: SubscriberStats;
  approach?: number;
  pipelineLatency?: LatencySummary;
  enrichmentMisses?: number;
  smtLookupAvgMs?: number;
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

export const MetricsGrid: React.FC<MetricsGridProps> = ({ currentStats, selectedChartMetrics, toggleChartMetric, subscriberStats, approach, pipelineLatency, enrichmentMisses, smtLookupAvgMs }) => {
  const rates = outcomeRates(currentStats);
  const slaRate = rates.slaRate.toFixed(2);
  const successRate = rates.successRate.toFixed(2);

  const renderCheckbox = (id: string) => {
    if (!selectedChartMetrics || !toggleChartMetric) return null;
    // Don't render checkbox if it's not a chartable metric (percentiles)
    if (id === 'percentiles' || id === 'percentilesEnrichment' || id === 'smtLookup') return null;
    
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
          <div className="stat-value">{currentStats.actualRps || 0} <span className="stat-unit">/ {currentStats.targetRps || 0}</span></div>
          <div className="stat-label">Actual / Target RPS</div>
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
          <div className="stat-label" style={{ marginTop: '0.5rem' }}>DB Latency Percentiles</div>
        </>
      ),
      style: {}
    },
    pipelineLatency: {
      content: (
        <>
          <Timer className="stat-icon" style={{ color: '#f472b6' }} />
          <div className="stat-value">{pipelineLatency?.avg ?? 0} <span className="stat-unit">ms</span></div>
          <div className="stat-label">Commit → Topic (p95 {pipelineLatency?.p95 ?? 0}ms)</div>
        </>
      ),
      style: {}
    },
    recordsDropped: {
      content: (
        <>
          <Ban className="stat-icon" />
          <div className="stat-value">{currentStats.recordsDropped || 0}</div>
          <div className="stat-label">Dropped (shed)</div>
        </>
      ),
      style: { color: (currentStats.recordsDropped || 0) > 0 ? 'var(--accent-red)' : 'inherit' }
    },
    cpu: {
      content: (
        <>
          <Cpu className="stat-icon" />
          <div className="stat-value">{currentStats.cpu} <span className="stat-unit">%</span></div>
          <div className="stat-label">SQL Server CPU <span style={{ fontSize: '0.75em', opacity: 0.7 }}>(1m avg)</span></div>
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
          <div className="stat-label">Committed On Time</div>
        </>
      ),
      style: {}
    },
    recordsInKafka: {
      content: (
        <>
          <Activity className="stat-icon" />
          <div className="stat-value">{currentStats.recordsInKafka}</div>
          <div className="stat-label">Events Delivered</div>
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
    },
    ...(approach === 4 ? {
      smtLookup: {
        content: (
          <>
            <Search className="stat-icon" style={{ color: '#a78bfa' }} />
            <div className="stat-value">{smtLookupAvgMs ?? 0} <span className="stat-unit">ms</span></div>
            <div className="stat-label">SMT Lookup · {enrichmentMisses ?? 0} misses</div>
          </>
        ),
        style: { color: (enrichmentMisses ?? 0) > 0 ? 'var(--accent-red)' : 'inherit' }
      }
    } : {})
  };

  const subscriberConfigs: Record<string, any> = subscriberStats ? {
    numSubscribers: {
      content: (
        <>
          <Users className="stat-icon" style={{ color: 'var(--accent-blue)' }} />
          <div className="stat-value">{subscriberStats.readySubscribers ?? subscriberStats.numSubscribers} <span className="stat-unit">/ {subscriberStats.numSubscribers}</span></div>
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
          <div className="stat-label">E2E Latency (Commit → Enriched)</div>
        </>
      ),
      style: {}
    },
    consumerLag: {
      content: (
        <>
          <AlertTriangle className="stat-icon" />
          <div className="stat-value">{subscriberStats.consumerLag.toLocaleString()}</div>
          <div className="stat-label">Consumer Lag (events)</div>
        </>
      ),
      style: { color: subscriberStats.consumerLag > 1000 ? 'var(--accent-red)' : 'inherit' }
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
  const [mainOrder, setMainOrder] = useState<string[]>(() => loadOrder('metricsGridMainOrder', DEFAULT_MAIN_ORDER));
  const [subscriberOrder, setSubscriberOrder] = useState<string[]>(() => loadOrder('metricsGridSubscriberOrder', DEFAULT_SUBSCRIBER_ORDER));

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
