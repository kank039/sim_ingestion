/** Backend base URL. Override with VITE_API_BASE (e.g. when the simulator runs on another host). */
export const API_BASE: string = import.meta.env.VITE_API_BASE || 'http://localhost:3001/api';

export const APPROACHES = [
  { id: 1, name: "Database Triggers (DBA Outbox)" },
  { id: 2, name: "Transactional Outbox (Recommended)" },
  { id: 3, name: "Stream-to-Stream Join (Flink)" },
  { id: 4, name: "JDBC SMT (Interceptor)" },
  { id: 5, name: "CDC Push + Consumer Enrichment (Multi-JOIN)" }
];

export const approachName = (id: number | null | undefined) => APPROACHES.find(a => a.id === id)?.name ?? 'Unknown';
