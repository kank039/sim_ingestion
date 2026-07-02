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
  const totalRecords = (currentStats.recordsModified || 0) + (currentStats.recordsFailed || 0);
  const successRate = totalRecords > 0 
    ? ((currentStats.recordsModified / totalRecords) * 100).toFixed(2) 
    : '100.00';

  const appLabel = `Simulator App<br/>RPS: ${currentStats.actualRps || 0}<br/>Success: ${successRate}%<br/>App Latency: ${currentStats.appLatency}ms`;
  const dbLabel = `SQL Server<br/>Modified: ${currentStats.recordsModified || 0}<br/>CPU: ${currentStats.cpu}%<br/>I/O: ${currentStats.io} MB/s<br/>Wait Tasks: ${currentStats.wait_tasks || 0}<br/>Locks: ${currentStats.active_locks || 0}`;
  const kafkaLabel = `Kafka Topic<br/>Records: ${currentStats.recordsInKafka || 0}<br/>Lag: ${currentStats.lag || 0}`;
  
  let subsLabel = `Node.js Consumer Workers`;
  if (subscriberStats) {
      subsLabel += `<br/>Active Subs: ${subscriberStats.numSubscribers}<br/>Msgs Consumed: ${subscriberStats.totalMessagesConsumed.toLocaleString()}<br/>Avg Enrich: ${subscriberStats.avgEnrichmentLatency}ms<br/>Avg E2E: ${subscriberStats.avgE2eLatency}ms<br/>Fails: ${subscriberStats.enrichmentsFailed}`;
  }

  let chart = '';

  if (approach === 1) {
    chart = `flowchart TD
    App["${appLabel}"] -->|"1. Insert/Update/Delete"| DB_Main[("billing_record<br/>${dbLabel}")]
    DB_Main -->|"2. Trigger Execution"| DB_Outbox[("outbox_events")]
    CDC["Debezium CDC"] -->|"3. Tail Log"| DB_Outbox
    CDC -->|"4. Push Event"| Kafka[["${kafkaLabel}"]]
    `;
  } else if (approach === 2) {
    chart = `flowchart LR
    App["${appLabel}"] -->|"1. Begin Transaction"| DB
    subgraph DB ["${dbLabel}"]
        DB_Main[("billing_record")]
        DB_Outbox[("outbox_events")]
    end
    App -->|"2a. Write Domain Entity"| DB_Main
    App -->|"2b. Write Event Payload"| DB_Outbox
    App -->|"3. Commit Transaction"| DB
    CDC["Debezium CDC"] -->|"4. Capture Change"| DB_Outbox
    CDC -->|"5. Publish Event"| Kafka[["${kafkaLabel}"]]
    `;
  } else if (approach === 3) {
    chart = `flowchart TD
    App["${appLabel}"] -->|"1a. Write Domain"| DB[("billing_record<br/>${dbLabel}")]
    App -->|"1b. Write Enrichment Req"| DB2[("enrichment_requests")]
    
    CDC1["Debezium CDC"] -->|"2a. Capture billing"| DB
    CDC2["Debezium CDC"] -->|"2b. Capture enrichment"| DB2
    
    CDC1 -->|"3a. Publish"| Topic1[["Kafka: billing_events"]]
    CDC2 -->|"3b. Publish"| Topic2[["Kafka: enrichment_events"]]
    
    Topic1 --> Flink["Flink JobManager/TaskManager"]
    Topic2 --> Flink
    
    Flink -->|"4. Stateful Stream Join"| FlinkState[("RocksDB State")]
    Flink -->|"5. Output Enriched Event"| OutTopic[["${kafkaLabel}"]]
    `;
  } else if (approach === 4) {
    chart = `flowchart TD
    App["${appLabel}"] -->|"1a. Write Domain"| DB[("billing_record<br/>${dbLabel}")]
    CDC["Debezium CDC"] -->|"2. Capture Change"| DB
    
    subgraph KafkaConnect ["Kafka Connect"]
        CDC
        SMT["JDBC SMT Interceptor"]
    end
    
    CDC -->|"3. Route Event to SMT"| SMT
    SMT -->|"4. Synchronous JDBC Lookup"| DB2[("invoice_batch etc")]
    DB2 -->|"5. Return Lookup Data"| SMT
    SMT -->|"6. Publish Enriched Event"| Kafka[["${kafkaLabel}"]]
    `;
  } else if (approach === 5) {
    chart = `flowchart TD
    App["${appLabel}"] -->|"1. Write Domain"| DB[("billing_record<br/>${dbLabel}")]
    CDC["Debezium CDC"] -->|"2. Capture Change"| DB
    CDC -->|"3. Publish Raw Event"| Kafka[["${kafkaLabel}"]]
    
    subgraph NodeWorkers ["${subsLabel}"]
        CW1["Consumer Worker 1"]
        CW2["Consumer Worker 2"]
    end
    
    Kafka -->|"4. Consume Raw Event"| NodeWorkers
    CW1 -->|"5. Multi-Table JOIN (NOLOCK)"| DB_ReadReplica[("SQL Server / Read Replica")]
    CW2 -->|"5. Multi-Table JOIN (NOLOCK)"| DB_ReadReplica
    
    NodeWorkers -->|"6. Process Enriched Data"| Destination["..."]
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
