import path from 'path';
import { Worker } from 'worker_threads';

/**
 * Spawns a worker from either the compiled .js (production) or the .ts source (ts-node dev mode).
 */
export function spawnWorker(name: string, workerData?: unknown): Worker {
    const isTs = __filename.endsWith('.ts');
    const file = path.join(__dirname, `${name}${isTs ? '.ts' : '.js'}`);
    return new Worker(file, {
        workerData,
        execArgv: isTs ? ['-r', 'ts-node/register/transpile-only'] : [],
    });
}

/** Sends `stop`, waits for `doneType` (or the timeout), then terminates. Never leaves a thread behind. */
export async function stopWorker(worker: Worker, doneType: string, timeoutMs = 10_000): Promise<void> {
    await new Promise<void>(resolve => {
        const timer = setTimeout(resolve, timeoutMs);
        const onMsg = (m: any) => {
            if (m?.type === doneType) {
                clearTimeout(timer);
                worker.off('message', onMsg);
                resolve();
            }
        };
        worker.on('message', onMsg);
        try { worker.postMessage({ type: 'stop' }); } catch { clearTimeout(timer); resolve(); }
    });
    await worker.terminate();
}
