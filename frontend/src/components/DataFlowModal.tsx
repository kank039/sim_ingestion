import React from 'react';
import { Activity, X } from 'lucide-react';
import type { TelemetryPoint, SubscriberStats } from '../types';
import { MermaidDiagram } from './MermaidDiagram';

interface DataFlowModalProps {
  approach: number;
  currentStats: TelemetryPoint;
  subscriberStats?: SubscriberStats;
  onClose: () => void;
}

export const DataFlowModal: React.FC<DataFlowModalProps> = ({ approach, currentStats, subscriberStats, onClose }) => {
  const totalRecords = (currentStats.recordsModified || 0) + (currentStats.recordsFailed || 0) + (currentStats.recordsLate || 0);
  const slaRate = totalRecords > 0 
    ? ((currentStats.recordsModified / totalRecords) * 100).toFixed(2) 
    : '100.00';
  const successRate = totalRecords > 0 
    ? (((currentStats.recordsModified + currentStats.recordsLate) / totalRecords) * 100).toFixed(2) 
    : '100.00';

  const appLabel = `Simulator App<br/>RPS: ${currentStats.actualRps || 0}<br/>SLA: ${slaRate}%<br/>Success: ${successRate}%<br/>App Latency: ${currentStats.appLatency}ms`;
  const dbLabel = `SQL Server<br/>Modified: ${currentStats.recordsModified || 0}<br/>CPU (1m): ${currentStats.cpu}%<br/>I/O: ${currentStats.io} MB/s<br/>Wait Tasks: ${currentStats.wait_tasks || 0}<br/>Locks: ${currentStats.active_locks || 0}`;
  const kafkaLabel = `Kafka Topic<br/>Records: ${currentStats.recordsInKafka || 0}<br/>Lag: ${currentStats.lag || 0}`;
  
  let subsLabel = `Node.js Consumer Workers`;
  if (subscriberStats) {
      subsLabel += `<br/>Active Subs: ${subscriberStats.numSubscribers}<br/>Msgs Consumed: ${subscriberStats.totalMessagesConsumed.toLocaleString()}<br/>Avg Enrich: ${subscriberStats.avgEnrichmentLatency}ms<br/>Avg E2E: ${subscriberStats.avgE2eLatency}ms<br/>Fails: ${subscriberStats.enrichmentsFailed}`;
  }

  const appClass = parseFloat(successRate) < 99 ? 'appError' : 'app';
  const dbClass = (currentStats.recordsFailed > 0 || (currentStats.wait_tasks || 0) > 10) ? 'dbError' : 'db';
  const kafkaClass = (currentStats.lag || 0) > 1000 ? 'kafkaError' : 'kafka';
  const subsClass = (subscriberStats && subscriberStats.enrichmentsFailed > 0) ? 'workersError' : 'workers';

  const classDefs = `
    classDef default fill:#1f2937,stroke:#4b5563,color:#f9fafb,stroke-width:2px;
    classDef app fill:#1e3a8a,stroke:#3b82f6,color:#eff6ff,stroke-width:2px;
    classDef appError fill:#7f1d1d,stroke:#ef4444,color:#fef2f2,stroke-width:2px;
    classDef db fill:#92400e,stroke:#f59e0b,color:#fffbeb,stroke-width:2px;
    classDef dbError fill:#7f1d1d,stroke:#ef4444,color:#fef2f2,stroke-width:2px;
    classDef kafka fill:#064e3b,stroke:#10b981,color:#ecfdf5,stroke-width:2px;
    classDef kafkaError fill:#7f1d1d,stroke:#ef4444,color:#fef2f2,stroke-width:2px;
    classDef workers fill:#4c1d95,stroke:#8b5cf6,color:#f5f3ff,stroke-width:2px;
    classDef workersError fill:#7f1d1d,stroke:#ef4444,color:#fef2f2,stroke-width:2px;
    classDef processing fill:#831843,stroke:#f43f5e,color:#fff1f2,stroke-width:2px;
  `;

  let chart = '';

  if (approach === 1) {
    chart = `flowchart TD
${classDefs}
    App["${appLabel}"]:::${appClass} -->|"1. Insert/Update/Delete"| DB_Main[("billing_record<br/>${dbLabel}")]:::${dbClass}
    DB_Main -->|"2. Trigger Execution"| DB_Outbox[("outbox_events")]:::${dbClass}
    CDC["Debezium CDC"]:::processing -->|"3. Tail Log"| DB_Outbox
    CDC -->|"4. Push Event"| Kafka[["${kafkaLabel}"]]:::${kafkaClass}
    `;
  } else if (approach === 2) {
    chart = `flowchart LR
${classDefs}
    App["${appLabel}"]:::${appClass} -->|"1. Begin Transaction"| DB
    subgraph DB ["${dbLabel}"]
        DB_Main[("billing_record")]:::${dbClass}
        DB_Outbox[("outbox_events")]:::${dbClass}
    end
    class DB ${dbClass};
    App -->|"2a. Write Domain Entity"| DB_Main
    App -->|"2b. Write Event Payload"| DB_Outbox
    App -->|"3. Commit Transaction"| DB
    CDC["Debezium CDC"]:::processing -->|"4. Capture Change"| DB_Outbox
    CDC -->|"5. Publish Event"| Kafka[["${kafkaLabel}"]]:::${kafkaClass}
    `;
  } else if (approach === 3) {
    chart = `flowchart TD
${classDefs}
    App["${appLabel}"]:::${appClass} -->|"1a. Write Domain"| DB[("billing_record<br/>${dbLabel}")]:::${dbClass}
    App -->|"1b. Write Enrichment Req"| DB2[("enrichment_requests")]:::${dbClass}
    
    CDC1["Debezium CDC"]:::processing -->|"2a. Capture billing"| DB
    CDC2["Debezium CDC"]:::processing -->|"2b. Capture enrichment"| DB2
    
    CDC1 -->|"3a. Publish"| Topic1[["Kafka: billing_events"]]:::${kafkaClass}
    CDC2 -->|"3b. Publish"| Topic2[["Kafka: enrichment_events"]]:::${kafkaClass}
    
    Topic1 --> Flink["Flink JobManager/TaskManager"]:::processing
    Topic2 --> Flink
    
    Flink -->|"4. Stateful Stream Join"| FlinkState[("RocksDB State")]:::processing
    Flink -->|"5. Output Enriched Event"| OutTopic[["${kafkaLabel}"]]:::${kafkaClass}
    `;
  } else if (approach === 4) {
    chart = `flowchart TD
${classDefs}
    App["${appLabel}"]:::${appClass} -->|"1a. Write Domain"| DB[("billing_record<br/>${dbLabel}")]:::${dbClass}
    CDC["Debezium CDC"]:::processing -->|"2. Capture Change"| DB
    
    subgraph KafkaConnect ["Kafka Connect"]
        CDC
        SMT["JDBC SMT Interceptor"]:::processing
    end
    
    CDC -->|"3. Route Event to SMT"| SMT
    SMT -->|"4. Synchronous JDBC Lookup"| DB2[("invoice_batch etc")]:::${dbClass}
    DB2 -->|"5. Return Lookup Data"| SMT
    SMT -->|"6. Publish Enriched Event"| Kafka[["${kafkaLabel}"]]:::${kafkaClass}
    `;
  } else if (approach === 5) {
    chart = `flowchart TD
${classDefs}
    App["${appLabel}"]:::${appClass} -->|"1. Write Domain"| DB[("billing_record<br/>${dbLabel}")]:::${dbClass}
    CDC["Debezium CDC"]:::processing -->|"2. Capture Change"| DB
    CDC -->|"3. Publish Raw Event"| Kafka[["${kafkaLabel}"]]:::${kafkaClass}
    
    subgraph NodeWorkers ["${subsLabel}"]
        CW1["Consumer Worker 1"]:::${subsClass}
        CW2["Consumer Worker 2"]:::${subsClass}
    end
    class NodeWorkers ${subsClass};
    
    Kafka -->|"4. Consume Raw Event"| NodeWorkers
    CW1 -->|"5. Multi-Table JOIN (NOLOCK)"| DB_ReadReplica[("SQL Server / Read Replica")]:::${dbClass}
    CW2 -->|"5. Multi-Table JOIN (NOLOCK)"| DB_ReadReplica
    
    NodeWorkers -->|"6. Process Enriched Data"| Destination["..."]:::default
    `;
  }

  return (
    <div className="logs-modal-overlay" onClick={onClose}>
      <div className="logs-modal-content flow-modal-content" onClick={e => e.stopPropagation()} style={{ maxWidth: '1000px', width: '90%' }}>
        <div className="panel-header" style={{ justifyContent: 'space-between', borderBottom: '1px solid var(--glass-border)', paddingBottom: '1rem', marginBottom: '1.5rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
            <Activity className="stat-icon" style={{ color: 'var(--accent-blue)', margin: 0 }} />
            <h2 style={{ fontSize: '1.2rem' }}>Data Flow Architecture - Approach {approach}</h2>
          </div>
          <button className="icon-btn" onClick={onClose} title="Close">
            <X size={24} />
          </button>
        </div>
        
        <div className="data-flow-container" style={{ overflowY: 'auto', flex: 1, backgroundColor: 'rgba(0,0,0,0.2)', borderRadius: '8px' }}>
            <MermaidDiagram chart={chart} />
        </div>
      </div>
    </div>
  );
};
