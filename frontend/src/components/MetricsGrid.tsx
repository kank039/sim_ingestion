import React from 'react';
import { Clock, Cpu, HardDrive, Database, Activity, AlertTriangle, XCircle, Percent } from 'lucide-react';
import type { TelemetryPoint } from '../types';

interface MetricsGridProps {
  currentStats: TelemetryPoint;
}

export const MetricsGrid: React.FC<MetricsGridProps> = ({ currentStats }) => {
  const totalRecords = (currentStats.recordsPushed || 0) + (currentStats.recordsFailed || 0);
  const successRate = totalRecords > 0 
    ? ((currentStats.recordsPushed / totalRecords) * 100).toFixed(2) 
    : '100.00';

  return (
    <div className="glass-panel stats-grid-container">
      <div className="stats-grid">
        <div className="stat-card">
          <Clock className="stat-icon" />
          <div className="stat-value">{currentStats.appLatency} <span className="stat-unit">ms</span></div>
          <div className="stat-label">Application Latency</div>
        </div>
        <div className="stat-card">
          <Cpu className="stat-icon" />
          <div className="stat-value">{currentStats.cpu} <span className="stat-unit">%</span></div>
          <div className="stat-label">SQL Server CPU</div>
        </div>
        <div className="stat-card">
          <HardDrive className="stat-icon" />
          <div className="stat-value">{currentStats.io} <span className="stat-unit">MB/s</span></div>
          <div className="stat-label">SQL Server I/O</div>
        </div>
        <div className="stat-card">
          <Database className="stat-icon" />
          <div className="stat-value">{currentStats.recordsPushed}</div>
          <div className="stat-label">Records Pushed</div>
        </div>
        <div className="stat-card" style={{ color: 'var(--accent-blue)' }}>
          <Activity className="stat-icon" />
          <div className="stat-value">{currentStats.recordsInKafka}</div>
          <div className="stat-label">Records in Kafka</div>
        </div>
        <div className="stat-card" style={{ color: currentStats.lag > 1000 ? 'var(--accent-red)' : 'var(--accent-green)' }}>
          <AlertTriangle className="stat-icon" />
          <div className="stat-value">{currentStats.lag}</div>
          <div className="stat-label">Pipeline Lag</div>
        </div>
        <div className="stat-card" style={{ color: currentStats.recordsFailed > 0 ? 'var(--accent-red)' : 'inherit' }}>
          <XCircle className="stat-icon" />
          <div className="stat-value">{currentStats.recordsFailed || 0}</div>
          <div className="stat-label">Failed Records</div>
        </div>
        <div className="stat-card" style={{ color: parseFloat(successRate as string) < 99 ? 'var(--accent-red)' : 'var(--accent-green)' }}>
          <Percent className="stat-icon" />
          <div className="stat-value">{successRate} <span className="stat-unit">%</span></div>
          <div className="stat-label">Success Rate</div>
        </div>
      </div>
    </div>
  );
};
