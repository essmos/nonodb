export type Connection = {
  endpoint: string;
  token: string;
  tenant: string;
  keyId: string;
  key: string;
};

export class ApiError extends Error {
  code: string;
  status: number;
  constructor(code: string, message: string, status = 0) {
    super(message);
    this.code = code;
    this.status = status;
  }
}

export type ApiTiming = {
  op: string;
  ms: number;
  ok: boolean;
  at: number;
};

type TimingListener = (timing: ApiTiming) => void;
let timingListener: TimingListener | null = null;

export function subscribeTiming(listener: TimingListener): () => void {
  timingListener = listener;
  return () => {
    if (timingListener === listener) timingListener = null;
  };
}

function reportTiming(timing: ApiTiming): void {
  timingListener?.(timing);
}

const STORAGE_KEY = 'nonodb.gui.connection';
const PROFILES_KEY = 'nonodb.gui.profiles';

export type ConnectionProfile = {
  name: string;
  connection: Connection;
};

export function defaultConnection(): Connection {
  return {
    endpoint: window.location.origin,
    token: '',
    tenant: '',
    keyId: '',
    key: '',
  };
}

export function loadConnection(): Connection {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return defaultConnection();
    return { ...defaultConnection(), ...JSON.parse(raw) } as Connection;
  } catch {
    return defaultConnection();
  }
}

export function saveConnection(connection: Connection): void {
  sessionStorage.setItem(STORAGE_KEY, JSON.stringify(connection));
}

export function clearConnection(): void {
  sessionStorage.removeItem(STORAGE_KEY);
}

export function loadProfiles(): ConnectionProfile[] {
  try {
    return JSON.parse(localStorage.getItem(PROFILES_KEY) ?? '[]') as ConnectionProfile[];
  } catch {
    return [];
  }
}

export function saveProfiles(profiles: ConnectionProfile[]): void {
  localStorage.setItem(PROFILES_KEY, JSON.stringify(profiles));
}

function headers(connection: Connection): Record<string, string> {
  const out: Record<string, string> = { 'content-type': 'application/json' };
  if (connection.token) out.authorization = `Bearer ${connection.token}`;
  if (connection.tenant) out['x-nonodb-tenant'] = connection.tenant;
  if (connection.keyId) out['x-nonodb-key-id'] = connection.keyId;
  if (connection.key) out['x-nonodb-key'] = connection.key;
  return out;
}

function endpointFor(connection: Connection, path: string): string {
  const endpoint = connection.endpoint.trim().replace(/\/+$/, '');
  return `${endpoint}${path}`;
}

export async function exec<T = unknown>(
  connection: Connection,
  body: Record<string, unknown>,
): Promise<T> {
  const started = performance.now();
  const op = typeof body.op === 'string' ? body.op : 'request';
  try {
    const response = await fetch(endpointFor(connection, '/v1/exec'), {
      method: 'POST',
      headers: headers(connection),
      body: JSON.stringify(body),
    });
    let payload: any = null;
    try {
      payload = await response.json();
    } catch {
      throw new ApiError('invalid_response', `HTTP ${response.status}`, response.status);
    }
    if (!response.ok || payload?.ok === false) {
      const error = payload?.error ?? {};
      throw new ApiError(
        error.code ?? 'http_error',
        error.message ?? `HTTP ${response.status}`,
        response.status,
      );
    }
    const serverMs = Number(response.headers.get('x-nonodb-server-ms'));
    reportTiming({
      op,
      ms: Number.isFinite(serverMs) && serverMs > 0 ? serverMs : performance.now() - started,
      ok: true,
      at: Date.now(),
    });
    return payload.result as T;
  } catch (error) {
    reportTiming({
      op,
      ms: performance.now() - started,
      ok: false,
      at: Date.now(),
    });
    throw error;
  }
}

export async function health(connection: Connection): Promise<unknown> {
  const response = await fetch(endpointFor(connection, '/healthz'));
  if (!response.ok) throw new ApiError('http_error', `HTTP ${response.status}`, response.status);
  return response.json();
}

export async function metrics(connection: Connection): Promise<Record<string, unknown>> {
  const response = await fetch(endpointFor(connection, '/metrics'), {
    headers: headers(connection),
  });
  if (!response.ok) throw new ApiError('http_error', `HTTP ${response.status}`, response.status);
  const payload = await response.json();
  return (payload?.result ?? payload) as Record<string, unknown>;
}

export function errorText(error: unknown): string {
  if (error instanceof ApiError) return `${error.code}: ${error.message}`;
  if (error instanceof Error) return error.message;
  return String(error);
}

export async function watchStream(
  connection: Connection,
  body: Record<string, unknown>,
  onEvent: (event: Record<string, unknown>) => void,
  signal: AbortSignal,
): Promise<void> {
  const response = await fetch(endpointFor(connection, '/v1/watch'), {
    method: 'POST',
    headers: { ...headers(connection), accept: 'application/x-ndjson' },
    body: JSON.stringify(body),
    signal,
  });
  if (!response.ok || !response.body) {
    throw new ApiError('stream_error', `HTTP ${response.status}`, response.status);
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    for (;;) {
      const newline = buffer.indexOf('\n');
      if (newline < 0) break;
      const line = buffer.slice(0, newline).trim();
      buffer = buffer.slice(newline + 1);
      if (!line) continue;
      let item: any;
      try {
        item = JSON.parse(line);
      } catch {
        continue;
      }
      if (item.heartbeat || item.stream) continue;
      if (item.error) {
        throw new ApiError(
          item.error.code ?? 'stream_error',
          item.error.message ?? 'stream error',
        );
      }
      onEvent(item as Record<string, unknown>);
    }
  }
}
