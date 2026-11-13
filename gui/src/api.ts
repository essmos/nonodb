export type Connection = {
  endpoint: string;
  database: string;
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
    database: 'default',
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

function scopedPath(connection: Connection, path: string): string {
  const database = (connection.database ?? 'default').trim();
  if (!database || database === 'default') return path;
  return `/${encodeURIComponent(database)}${path}`;
}

export async function exec<T = unknown>(
  connection: Connection,
  body: Record<string, unknown>,
): Promise<T> {
  const started = performance.now();
  const op = typeof body.op === 'string' ? body.op : 'request';
  try {
    const response = await fetch(endpointFor(connection, scopedPath(connection, '/v1/exec')), {
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


export type AuthPrincipal = {
  name: string;
  scopes: string[];
  tokens: number;
  certificates: number;
};

export type AuthToken = {
  kid: string;
  principal: string;
  disabled: boolean;
};

export type AuthCertificate = {
  sha256: string;
  principal: string;
  disabled: boolean;
};

export type AuthSummary = {
  generation: number;
  principals: AuthPrincipal[];
  tokens: AuthToken[];
  certificates: AuthCertificate[];
};

async function authRequest<T>(
  connection: Connection,
  path: string,
  method = 'GET',
  body?: Record<string, unknown>,
): Promise<T> {
  const response = await fetch(endpointFor(connection, path), {
    method,
    headers: headers(connection),
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
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
  return payload?.result as T;
}

export function authSummary(connection: Connection): Promise<AuthSummary> {
  return authRequest<AuthSummary>(connection, '/v1/auth');
}

export function createPrincipal(
  connection: Connection,
  name: string,
  scopes: string[],
): Promise<AuthPrincipal> {
  return authRequest<AuthPrincipal>(connection, '/v1/auth/principals', 'POST', {
    name,
    scopes,
  });
}

export function updatePrincipal(
  connection: Connection,
  name: string,
  scopes: string[],
): Promise<AuthPrincipal> {
  return authRequest<AuthPrincipal>(
    connection,
    `/v1/auth/principals/${encodeURIComponent(name)}`,
    'PUT',
    { scopes },
  );
}

export function deletePrincipal(connection: Connection, name: string): Promise<unknown> {
  return authRequest(
    connection,
    `/v1/auth/principals/${encodeURIComponent(name)}`,
    'DELETE',
  );
}

export function createAuthToken(
  connection: Connection,
  principal: string,
  kid?: string,
): Promise<{ kid: string; principal: string; token: string }> {
  return authRequest(connection, '/v1/auth/tokens', 'POST', {
    principal,
    ...(kid ? { kid } : {}),
  });
}

export function updateAuthToken(
  connection: Connection,
  kid: string,
  disabled: boolean,
): Promise<AuthToken> {
  return authRequest<AuthToken>(
    connection,
    `/v1/auth/tokens/${encodeURIComponent(kid)}`,
    'PATCH',
    { disabled },
  );
}

export function createCertificate(
  connection: Connection,
  sha256: string,
  principal: string,
): Promise<AuthCertificate> {
  return authRequest<AuthCertificate>(connection, '/v1/auth/certificates', 'POST', {
    sha256,
    principal,
  });
}

export function updateCertificate(
  connection: Connection,
  sha256: string,
  disabled: boolean,
): Promise<AuthCertificate> {
  return authRequest<AuthCertificate>(
    connection,
    `/v1/auth/certificates/${encodeURIComponent(sha256)}`,
    'PATCH',
    { disabled },
  );
}

export function collectionMetrics(
  connection: Connection,
  collection: string,
): Promise<Record<string, unknown>> {
  return exec<Record<string, unknown>>(connection, {
    op: 'get_collection_metrics',
    coll: collection,
  });
}

export type CollectionStorageStats = {
  collection: string;
  records: number;
  data_bytes: number;
  index_bytes: number;
  indexes: number;
  history_bytes: number;
  ttl_bytes: number;
  total_bytes: number;
  requests_last_minute: number;
  avg_requests_per_second: number;
};

export type DatabaseStorageStats = {
  database: string;
  map_size: number;
  used_bytes: number;
  free_bytes: number;
  page_size: number;
  total_records: number;
  collection_bytes: number;
  collections: CollectionStorageStats[];
};

export function databaseStats(connection: Connection): Promise<DatabaseStorageStats> {
  return exec<DatabaseStorageStats>(connection, { op: 'database_stats' });
}

export type BackupDestination =
  | { kind: 'local'; path: string }
  | {
      kind: 's3';
      endpoint: string;
      bucket: string;
      region: string;
      prefix: string;
      access_key_env: string;
      secret_key_env: string;
      session_token_env?: string;
    }
  | {
      kind: 'restic';
      repository: string;
      password_file: string;
      binary: string;
    };

export type BackupEncryption = {
  kind: 'age';
  recipient_file: string;
};

export type BackupJob = {
  id: string;
  database: string;
  databases?: string[];
  enabled: boolean;
  every_minutes: number;
  destination: BackupDestination;
  encryption?: BackupEncryption;
  keep_last: number;
  verify: boolean;
  compression?: 'none' | 'gzip';
  exclude_collections?: string[];
};

export type BackupJobState = {
  last_run_unix: number;
  next_run_unix: number;
  last_status: string;
  last_error?: string | null;
  last_size_bytes: number;
  last_duration_ms: number;
  last_destination: string;
  running: boolean;
};

export type BackupItem = {
  job: BackupJob;
  state: BackupJobState;
};

export type BackupsView = {
  config_path: string;
  staging_path: string;
  key_dir: string;
  default_identity_file: string;
  default_recipient_file: string;
  default_restic_password_file: string;
  default_local_path: string;
  default_restic_repository: string;
  jobs: BackupItem[];
};

async function backupRequest<T>(
  connection: Connection,
  path: string,
  method = 'GET',
  body?: Record<string, unknown>,
): Promise<T> {
  const response = await fetch(endpointFor(connection, path), {
    method,
    headers: headers(connection),
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
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
  return payload?.result as T;
}

export function backupsList(connection: Connection): Promise<BackupsView> {
  return backupRequest<BackupsView>(connection, '/v1/backups');
}

export function saveBackupJob(connection: Connection, job: BackupJob): Promise<BackupItem> {
  return backupRequest<BackupItem>(connection, '/v1/backups/jobs', 'POST', job as unknown as Record<string, unknown>);
}

export function deleteBackupJob(connection: Connection, id: string): Promise<unknown> {
  return backupRequest(connection, `/v1/backups/jobs/${encodeURIComponent(id)}`, 'DELETE');
}

export function runBackupJob(connection: Connection, id: string): Promise<unknown> {
  return backupRequest(connection, `/v1/backups/jobs/${encodeURIComponent(id)}/run`, 'POST');
}

export type BackupKey = {
  identity_file: string;
  recipient_file: string;
  recipient: string;
};

export function generateBackupKey(
  connection: Connection,
  identity_file: string,
  recipient_file: string,
): Promise<BackupKey> {
  return backupRequest<BackupKey>(connection, '/v1/backups/keygen', 'POST', {
    identity_file,
    recipient_file,
  });
}

export function generateBackupSecret(
  connection: Connection,
  path: string,
): Promise<{ path: string }> {
  return backupRequest<{ path: string }>(connection, '/v1/backups/secret', 'POST', { path });
}

export type ImportSource =
  | { kind: 'local'; path: string }
  | {
      kind: 's3';
      endpoint: string;
      bucket: string;
      region: string;
      prefix: string;
      access_key_env: string;
      secret_key_env: string;
      session_token_env?: string;
    };

export type DiscoveredImport = {
  location: string;
  manifest: {
    version: number;
    created_at_ms: number;
    source: string;
    job_id?: string;
    excluded_collections?: string[];
    data_file: string;
    data_size: number;
    sha256: string;
    plain_sha256?: string;
    plain_size?: number;
    encryption?: { kind: string };
    compression?: { kind: string };
  };
  info?: string | null;
};

export type ImportStatus = {
  id: string;
  status: string;
  source: string;
  location: string;
  target_database: string;
  error?: string | null;
  bytes: number;
  started_at_ms: number;
  finished_at_ms?: number | null;
};

export type ImportOverview = {
  roots: string[];
  default_root?: string | null;
  jobs: ImportStatus[];
};

export function importOverview(connection: Connection): Promise<ImportOverview> {
  return backupRequest<ImportOverview>(connection, '/v1/import');
}

export function browseImports(
  connection: Connection,
  source: ImportSource,
): Promise<{ backups: DiscoveredImport[] }> {
  return backupRequest<{ backups: DiscoveredImport[] }>(connection, '/v1/import/browse', 'POST', {
    source,
  });
}

export function runImport(
  connection: Connection,
  source: ImportSource,
  location: string,
  target_database: string,
): Promise<ImportStatus> {
  return backupRequest<ImportStatus>(connection, '/v1/import/run', 'POST', {
    source,
    location,
    target_database,
  });
}

export function importStatus(connection: Connection, id: string): Promise<ImportStatus> {
  return backupRequest<ImportStatus>(connection, `/v1/import/status/${encodeURIComponent(id)}`);
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
  const response = await fetch(endpointFor(connection, scopedPath(connection, '/v1/watch')), {
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

export async function sessionDefaults(): Promise<{ token: string }> {
  try {
    const response = await fetch('/gui/session', { headers: { accept: 'application/json' } });
    if (!response.ok) return { token: '' };
    const payload = await response.json();
    return { token: typeof payload?.token === 'string' ? payload.token : '' };
  } catch {
    return { token: '' };
  }
}
