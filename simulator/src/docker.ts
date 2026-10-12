import http from 'http';
import { config } from './config';

interface DockerResponse { status: number; body: Buffer }

function dockerRequest(method: string, path: string, body?: unknown, timeoutMs = 10_000): Promise<DockerResponse> {
    return new Promise((resolve, reject) => {
        const payload = body === undefined ? undefined : Buffer.from(JSON.stringify(body));
        const req = http.request({
            socketPath: config.dockerSocket,
            path,
            method,
            headers: payload ? { 'Content-Type': 'application/json', 'Content-Length': payload.length } : undefined,
        }, (res) => {
            const chunks: Buffer[] = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => resolve({ status: res.statusCode || 0, body: Buffer.concat(chunks) }));
            res.on('error', reject);
        });
        req.setTimeout(timeoutMs, () => req.destroy(new Error(`Docker API ${method} ${path} timed out`)));
        req.on('error', reject);
        if (payload) req.write(payload);
        req.end();
    });
}

async function dockerJson<T>(method: string, path: string, body?: unknown, timeoutMs?: number): Promise<T> {
    const res = await dockerRequest(method, path, body, timeoutMs);
    if (res.status >= 400) throw new Error(`Docker API ${method} ${path} -> ${res.status}: ${res.body.toString().slice(0, 300)}`);
    return (res.body.length ? JSON.parse(res.body.toString()) : undefined) as T;
}

export interface ContainerSummary { Id: string; Names: string[]; State: string }

export const containerName = (c: ContainerSummary) => (c.Names[0] || '').replace(/^\//, '');

export function listContainers(): Promise<ContainerSummary[]> {
    return dockerJson<ContainerSummary[]>('GET', '/containers/json');
}

export function containerStatsOnce(id: string): Promise<any> {
    // one-shot avoids the ~1s wait docker otherwise uses to fill precpu_stats; we diff samples ourselves.
    return dockerJson<any>('GET', `/containers/${id}/stats?stream=false&one-shot=true`);
}

export function inspectContainer(idOrName: string): Promise<any> {
    return dockerJson<any>('GET', `/containers/${encodeURIComponent(idOrName)}/json`);
}

export async function restartContainer(idOrName: string): Promise<void> {
    const res = await dockerRequest('POST', `/containers/${encodeURIComponent(idOrName)}/restart?t=10`, undefined, 60_000);
    if (res.status !== 204) throw new Error(`Restart failed (${res.status}): ${res.body.toString().slice(0, 300)}`);
}

/** Docker multiplexes stdout/stderr with 8-byte frame headers when Tty is false. */
export function demuxDockerStream(buf: Buffer): string {
    let out = '';
    let i = 0;
    while (i + 8 <= buf.length) {
        const size = buf.readUInt32BE(i + 4);
        out += buf.subarray(i + 8, i + 8 + size).toString();
        i += 8 + size;
    }
    if (i === 0 && buf.length > 0) return buf.toString(); // not multiplexed
    return out;
}

/** Runs a command inside a container and returns its exit code and combined output. */
export async function execInContainer(container: string, cmd: string[], timeoutMs = 180_000): Promise<{ exitCode: number; output: string }> {
    const exec = await dockerJson<{ Id: string }>('POST', `/containers/${encodeURIComponent(container)}/exec`, {
        AttachStdout: true, AttachStderr: true, Tty: false, Cmd: cmd,
    });
    const started = await dockerRequest('POST', `/exec/${exec.Id}/start`, { Detach: false, Tty: false }, timeoutMs);
    const info = await dockerJson<{ ExitCode: number | null }>('GET', `/exec/${exec.Id}/json`);
    return { exitCode: info.ExitCode ?? -1, output: demuxDockerStream(started.body) };
}
