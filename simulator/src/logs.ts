export type LogLevel = 'info' | 'warn' | 'error';

export interface SystemLog { time: string; message: string; level: LogLevel; timestamp: number; seq: number }

const MAX_LOGS = 500;
let logs: SystemLog[] = [];
let seq = 0;

export function addLog(message: string, level: LogLevel = 'info') {
    const entry: SystemLog = { time: new Date().toLocaleTimeString(), message, level, timestamp: Date.now(), seq: ++seq };
    logs.unshift(entry);
    if (logs.length > MAX_LOGS) logs.length = MAX_LOGS;
    const line = `[${level}] ${message}`;
    if (level === 'error') console.error(line); else console.log(line);
}

/** Newest first. */
export function getLogs(): SystemLog[] {
    return logs;
}

/** Logs strictly newer than `afterSeq`, newest first. Sequence numbers never collide (unlike timestamps). */
export function getLogsAfter(afterSeq: number): SystemLog[] {
    const out: SystemLog[] = [];
    for (const l of logs) {
        if (l.seq <= afterSeq) break;
        out.push(l);
    }
    return out;
}

export function latestLogSeq(): number {
    return seq;
}

export function clearLogs() {
    logs = [];
}
