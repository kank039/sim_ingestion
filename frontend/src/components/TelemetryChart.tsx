import React, { useMemo } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Brush } from 'recharts';
import type { TelemetryPoint } from '../types';

interface TelemetryChartProps {
  history: TelemetryPoint[];
  chartSpeed: 'fast' | 'normal' | 'slow';
  setChartSpeed: (speed: 'fast' | 'normal' | 'slow') => void;
  selectedChartMetrics?: Record<string, boolean>;
}

const CustomTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    return (
      <div style={{ 
        backgroundColor: 'rgba(17, 17, 17, 0.95)', 
        border: '1px solid #444', 
        padding: '12px', 
        borderRadius: '8px', 
        color: '#fff', 
        fontSize: '12px',
        boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.5)',
        zIndex: 1000
      }}>
        <p style={{ margin: '0 0 10px 0', fontWeight: 'bold', borderBottom: '1px solid #444', paddingBottom: '6px' }}>
          {`Time: ${label}`}
        </p>
        <div style={{ 
          display: 'grid', 
          gridTemplateColumns: payload.length > 5 ? '1fr 1fr' : '1fr', 
          columnGap: '20px',
          rowGap: '6px'
        }}>
          {payload.map((entry: any, index: number) => (
            <div key={`item-${index}`} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '15px' }}>
              <span style={{ color: entry.color, whiteSpace: 'nowrap' }}>{entry.name}</span>
              <span style={{ fontWeight: 'bold' }}>{entry.value}</span>
            </div>
          ))}
        </div>
      </div>
    );
  }
  return null;
};

const TelemetryChart: React.FC<TelemetryChartProps> = ({ history, chartSpeed, setChartSpeed, selectedChartMetrics }) => {
  const chartData = useMemo(() => {
    switch (chartSpeed) {
      case 'fast': return history.slice(-30);
      case 'normal': return history.slice(-60);
      case 'slow': return history.slice(-150);
      default: return history;
    }
  }, [history, chartSpeed]);

  return (
    <div className="glass-panel grid-item-content charts-container">
      <div className="chart-header">
        <h3>Performance Telemetry</h3>
        <div className="speed-toggles">
          <button className={chartSpeed === 'slow' ? 'active' : ''} onClick={() => setChartSpeed('slow')}>Slow</button>
          <button className={chartSpeed === 'normal' ? 'active' : ''} onClick={() => setChartSpeed('normal')}>Normal</button>
          <button className={chartSpeed === 'fast' ? 'active' : ''} onClick={() => setChartSpeed('fast')}>Fast</button>
        </div>
      </div>
      <div className="chart-wrapper">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
            <XAxis dataKey="time" stroke="#888" />
            <YAxis stroke="#888" />
            <Tooltip content={<CustomTooltip />} />
            <Legend />
            {(!selectedChartMetrics || selectedChartMetrics['appLatency']) && <Line type="monotone" dataKey="appLatency" name="App Latency (ms)" stroke="#3b82f6" strokeWidth={2} dot={false} isAnimationActive={false} />}
            {selectedChartMetrics && selectedChartMetrics['actualRps'] && <Line type="monotone" dataKey="actualRps" name="Actual RPS" stroke="#10b981" strokeWidth={2} dot={false} isAnimationActive={false} />}
            {selectedChartMetrics && selectedChartMetrics['queueLatency'] && <Line type="monotone" dataKey="queueLatency" name="Queue Latency (ms)" stroke="#f97316" strokeWidth={2} dot={false} isAnimationActive={false} />}
            {(!selectedChartMetrics || selectedChartMetrics['cpu']) && <Line type="monotone" dataKey="cpu" name="CPU (%)" stroke="#ef4444" strokeWidth={2} dot={false} isAnimationActive={false} />}
            {(!selectedChartMetrics || selectedChartMetrics['io']) && <Line type="monotone" dataKey="io" name="I/O (MB/s)" stroke="#10b981" strokeWidth={2} dot={false} isAnimationActive={false} />}
            {selectedChartMetrics && selectedChartMetrics['wait_tasks'] && <Line type="monotone" dataKey="wait_tasks" name="DB Wait Tasks" stroke="#eab308" strokeWidth={2} dot={false} isAnimationActive={false} />}
            {selectedChartMetrics && selectedChartMetrics['active_locks'] && <Line type="monotone" dataKey="active_locks" name="Active DB Locks" stroke="#ef4444" strokeWidth={2} dot={false} isAnimationActive={false} />}
            {selectedChartMetrics && selectedChartMetrics['recordsModified'] && <Line type="monotone" dataKey="recordsModified" name="Records Modified" stroke="#a855f7" strokeWidth={2} dot={false} isAnimationActive={false} />}
            {selectedChartMetrics && selectedChartMetrics['recordsInKafka'] && <Line type="monotone" dataKey="recordsInKafka" name="Records in Kafka" stroke="#ec4899" strokeWidth={2} dot={false} isAnimationActive={false} />}
            {selectedChartMetrics && selectedChartMetrics['lag'] && <Line type="monotone" dataKey="lag" name="Pipeline Lag" stroke="#f59e0b" strokeWidth={2} dot={false} isAnimationActive={false} />}
            {selectedChartMetrics && selectedChartMetrics['recordsFailed'] && <Line type="monotone" dataKey="recordsFailed" name="Failed Records" stroke="#dc2626" strokeWidth={2} dot={false} isAnimationActive={false} />}
            {selectedChartMetrics && selectedChartMetrics['successRate'] && <Line type="monotone" dataKey="successRate" name="Success Rate (%)" stroke="#8b5cf6" strokeWidth={2} dot={false} isAnimationActive={false} />}
            {selectedChartMetrics && selectedChartMetrics['consumerE2eLatency'] && <Line type="monotone" dataKey="consumerE2eLatency" name="Consumer E2E Latency (ms)" stroke="#f472b6" strokeWidth={2} dot={false} isAnimationActive={false} />}
            {selectedChartMetrics && selectedChartMetrics['consumerEnrichmentLatency'] && <Line type="monotone" dataKey="consumerEnrichmentLatency" name="Consumer Enrichment Latency (ms)" stroke="#a78bfa" strokeWidth={2} dot={false} isAnimationActive={false} />}
            <Brush dataKey="time" height={30} stroke="#3b82f6" />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default React.memo(TelemetryChart);
