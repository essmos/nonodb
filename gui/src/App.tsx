import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { json as jsonLanguage } from '@codemirror/lang-json';
import { sql as sqlLanguage } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';
import { autocompletion, type CompletionContext } from '@codemirror/autocomplete';
import QueryBuilder from './QueryBuilder';
import EventingPage from './EventingPage';
import {
  applyFirewall,
  authSummary,
  backupsList,
  clusterStatus,
  testProgress,
  browseImports,
  clearConnection,
  collectionMetrics as fetchCollectionMetrics,
  createAuthToken,
  createCertificate,
  createPrincipal,
  createReplicaPairing,
  databaseStats,
  disableFirewall,
  defaultConnection,
  deleteBackupJob,
  deletePrincipal,
  disconnectReplication,
  errorText,
  exec,
  forgetReplicaAck,
  generateBackupKey,
  generateBackupSecret,
  health,
  importOverview,
  importStatus,
  demoteReplication,
  promoteReplication,
  listReplicaPairings,
  networkInfo,
  pauseReplication,
  replicationChanges,
  replicationStatus,
  resumeReplication,
  revokeReplicaPairing,
  runImport,
  updateReplicationSource,
  loadConnection,
  loadProfiles,
  metrics,
  saveConnection,
  saveFirewall,
  saveProfiles,
  saveBackupJob,
  runBackupJob,
  sessionDefaults,
  type AuthSummary,
  type BackupItem,
  type BackupJob,
  type BackupsView,
  type ClusterStatus,
  type TestProgress,
  type ConnectionProfile,
  type DiscoveredImport,
  type ImportSource,
  type ImportStatus,
  type ImportOverview,
  type PairingView,
  type ReplicationChangeTxn,
  type NetworkInfo,
  type ReplicationStatus,
  type DatabaseStorageStats,
  subscribeTiming,
  updateAuthToken,
  updateCertificate,
  updatePrincipal,
  watchStream,
  type ApiTiming,
  type Connection,
} from './api';

type Doc = Record<string, unknown>;

type CollectionTab = 'documents' | 'indexes' | 'events' | 'search' | 'aggregate' | 'schema' | 'metrics';

type TimeSeriesSettings = {
  time_field: string;
  meta_field?: string | null;
  granularity: string;
  bucket_max_docs?: number;
  bucket_max_bytes?: number;
  compression?: string;
  expire_after_seconds?: number | null;
};

type TimeSeriesStats = {
  buckets: number;
  sealed_buckets: number;
  open_buckets: number;
  series: number;
  rows: number;
  raw_bytes: number;
  encoded_bytes: number;
  open_bytes: number;
  deleted_rows: number;
};

type IndexInfo = {
  field: string;
  fields: string[];
  unique: boolean;
  include?: string[];
  format?: string;
  geo?: boolean;
};

type Revision = {
  rev: number;
  op: string;
  at: number;
  deleted: boolean;
  doc?: Doc | null;
};

type MapInfo = {
  map_size: number;
  used_bytes: number;
  free_bytes: number;
  page_size: number;
};

function formatBytes(value: number): string {
  if (!Number.isFinite(value)) return '-';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let n = value;
  let i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i++;
  }
  return `${n.toFixed(i === 0 ? 0 : 2)} ${units[i]}`;
}

function pretty(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function parseJson(text: string, label: string): unknown {
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new Error(`${label}: ${errorText(error)}`);
  }
}

function collectFieldPaths(docs: Doc[], prefix = '', target = new Set<string>()): Set<string> {
  for (const doc of docs) {
    const walk = (value: unknown, path: string, depth: number) => {
      if (depth > 6) return;
      if (Array.isArray(value)) {
        for (const item of value) {
          if (item && typeof item === 'object' && !Array.isArray(item)) {
            walk(item, path, depth + 1);
          } else {
            target.add(path);
          }
        }
        if (value.length === 0) target.add(path);
        return;
      }
      if (value && typeof value === 'object') {
        for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
          const childPath = path ? `${path}.${key}` : key;
          target.add(childPath);
          walk(child, childPath, depth + 1);
        }
      }
    };
    walk(doc, prefix, 0);
  }
  return target;
}

function fieldCompletionSource(fields: string[]) {
  return (context: CompletionContext) => {
    const word = context.matchBefore(/[A-Za-z0-9_.$-]*/);
    if (!word || (word.from === word.to && !context.explicit)) return null;
    return {
      from: word.from,
      options: fields.map((field) => ({
        label: field,
        type: 'property',
        detail: 'field',
      })),
    };
  };
}

function JsonEditor({
  value,
  onChange,
  height = '320px',
  fields = [],
}: {
  value: string;
  onChange: (value: string) => void;
  height?: string;
  fields?: string[];
}) {
  const extensions = useMemo(
    () => [
      jsonLanguage(),
      ...(fields.length > 0
        ? [autocompletion({ override: [fieldCompletionSource(fields)] })]
        : []),
    ],
    [fields],
  );
  return (
    <CodeMirror
      className="cm-json-editor"
      value={value}
      height={height}
      theme={oneDark}
      extensions={extensions}
      onChange={onChange}
      basicSetup={{ lineNumbers: true, foldGutter: true, highlightActiveLine: true }}
    />
  );
}

function SqlEditor({ value, onChange }: { value: string; onChange: (value: string) => void }) {
  return (
    <CodeMirror
      className="cm-sql-editor"
      value={value}
      height="220px"
      theme={oneDark}
      extensions={[sqlLanguage()]}
      onChange={onChange}
      basicSetup={{ lineNumbers: true, foldGutter: true, highlightActiveLine: true }}
    />
  );
}

function Modal({
  title,
  subtitle,
  onClose,
  children,
  wide = false,
}: {
  title: string;
  subtitle?: string;
  onClose: () => void;
  children: ReactNode;
  wide?: boolean;
}) {
  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div className={`modal${wide ? ' wide-modal' : ''}`} role="dialog" aria-modal="true">
        <div className="modal-header">
          <div>
            <h3>{title}</h3>
            {subtitle && <p className="muted small">{subtitle}</p>}
          </div>
          <button className="icon-button" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}

function downloadText(filename: string, text: string, type = 'application/json'): void {
  const blob = new Blob([text], { type });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}

function idOf(doc: Doc): string {
  return String(doc._id ?? '');
}

function cleanField(value: string): string {
  return value.trim().replace(/^["'`]+|["'`]+$/g, '');
}

function ConnectView({ onConnected }: { onConnected: (connection: Connection) => void }) {
  const [connection, setConnection] = useState<Connection>(() => loadConnection());
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [advanced, setAdvanced] = useState(false);
  const [profiles, setProfiles] = useState<ConnectionProfile[]>(() => loadProfiles());
  const [profileName, setProfileName] = useState('');

  const rememberProfile = () => {
    const name = profileName.trim() || window.prompt('Profile name')?.trim();
    if (!name) return;
    const next = [
      { name, connection },
      ...profiles.filter((profile) => profile.name !== name),
    ].sort((a, b) => a.name.localeCompare(b.name));
    setProfiles(next);
    saveProfiles(next);
    setProfileName(name);
  };

  const removeProfile = (name: string) => {
    const next = profiles.filter((profile) => profile.name !== name);
    setProfiles(next);
    saveProfiles(next);
  };

  const connect = async () => {
    setBusy(true);
    setError('');
    try {
      await exec(connection, { op: 'list_collections' });
      saveConnection(connection);
      onConnected(connection);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="connect-page">
      <div className="connect-card">
        <div className="brand large">
          <span className="brand-mark"><img src="/nonodb-mark.svg" alt="" /></span>
          <div>
            <h1>Connect to NonoDB</h1>
            <p>Use the same database token as the CLI or API</p>
          </div>
        </div>
        <label>
          Database token
          <input
            type="password"
            value={connection.token}
            onChange={(e) => setConnection({ ...connection, token: e.target.value })}
            placeholder="nodb_..."
            autoComplete="off"
            autoFocus
          />
          <span className="field-hint">
            Leave empty when the daemon runs without authorization. This is the same
            `NONODB_TOKEN` you use with `nonodb-cli` or API clients.
          </span>
        </label>
        <div className="connect-summary">
          <span>Server</span>
          <code>{connection.endpoint}</code>
        </div>
        <button className="primary wide" onClick={connect} disabled={busy}>
          {busy ? 'Connecting…' : 'Connect'}
        </button>
        {error && <div className="error-box">{error}</div>}
        <button className="link-button" onClick={() => setAdvanced(!advanced)}>
          {advanced ? 'Hide advanced options' : 'Advanced: endpoint, profiles, vault'}
        </button>
        {advanced && (
          <div className="advanced-panel">
            <label>
              Endpoint
              <input
                value={connection.endpoint}
                onChange={(e) => setConnection({ ...connection, endpoint: e.target.value })}
                placeholder="http://127.0.0.1:8787"
              />
            </label>
            <label>
              Database
              <input
                value={connection.database}
                onChange={(e) => setConnection({ ...connection, database: e.target.value })}
                placeholder="default"
              />
              <span className="field-hint">Use `default` or a database created with `nonodb-cli databases create`.</span>
            </label>
            {profiles.length > 0 && (
              <label>
                Saved profile
                <div className="inline-control">
                  <select
                    value=""
                    onChange={(e) => {
                      const profile = profiles.find((item) => item.name === e.target.value);
                      if (profile) {
                        setConnection(profile.connection);
                        setProfileName(profile.name);
                      }
                    }}
                  >
                    <option value="">Choose a saved connection…</option>
                    {profiles.map((profile) => (
                      <option key={profile.name} value={profile.name}>
                        {profile.name} — {profile.connection.endpoint}
                      </option>
                    ))}
                  </select>
                  {profileName && (
                    <button className="small" onClick={() => removeProfile(profileName)}>
                      Remove
                    </button>
                  )}
                </div>
              </label>
            )}
            <div className="inline-control">
              <input
                value={profileName}
                onChange={(e) => setProfileName(e.target.value)}
                placeholder="Profile name"
              />
              <button className="small" onClick={rememberProfile}>
                Save profile
              </button>
            </div>
            <p className="hint">
              Saved profiles use browser localStorage. Do not save production tokens on a
              shared machine.
            </p>
            <div className="vault-fields">
              <strong className="small">Vault collection credentials (optional)</strong>
              <label>
                Tenant
                <input
                  value={connection.tenant}
                  onChange={(e) => setConnection({ ...connection, tenant: e.target.value })}
                />
              </label>
              <label>
                Key id
                <input
                  value={connection.keyId}
                  onChange={(e) => setConnection({ ...connection, keyId: e.target.value })}
                />
              </label>
              <label>
                Vault key
                <input
                  type="password"
                  value={connection.key}
                  onChange={(e) => setConnection({ ...connection, key: e.target.value })}
                  autoComplete="off"
                />
              </label>
            </div>
          </div>
        )}
        <p className="hint">
          Tip: run <code>nonodb-cli gui</code>. On a local host with
          <code> NONODB_TOKEN</code> set, the token is filled automatically.
        </p>
      </div>
    </div>
  );
}


function DatabaseMetrics({ connection }: { connection: Connection }) {
  const [metricsData, setMetricsData] = useState<Record<string, unknown> | null>(null);
  const [samples, setSamples] = useState<Array<Record<string, number>>>([]);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [error, setError] = useState('');

  const loadMetrics = useCallback(async () => {
    setError('');
    try {
      const data = await metrics(connection);
      setMetricsData(data);
      setSamples((current) => [
        ...current.slice(-59),
        {
          requests: metricNumber(data, 'requests_total'),
          reads: metricNumber(data, 'read_ops_total'),
          writes: metricNumber(data, 'write_ops_total'),
          errors: metricNumber(data, 'errors_total'),
          queue: metricNumber(data, 'queue_len'),
        },
      ]);
    } catch (err) {
      setError(errorText(err));
    }
  }, [connection]);

  useEffect(() => {
    void loadMetrics();
    if (!autoRefresh) return;
    const timer = window.setInterval(() => void loadMetrics(), 5000);
    return () => window.clearInterval(timer);
  }, [autoRefresh, loadMetrics]);

  const diskUsedPercent = nestedNumber(metricsData, ['disk', 'data', 'used_ratio']) * 100;
  const snapshotRows = Array.isArray(metricsData?.snapshots)
    ? (metricsData.snapshots as Array<Record<string, unknown>>)
    : [];
  const currentSnapshot = snapshotRows.find(
    (row) => row.database === (connection.database || 'default'),
  );
  const snapshotAge =
    currentSnapshot && typeof currentSnapshot.age_seconds === 'number'
      ? currentSnapshot.age_seconds
      : null;

  return (
    <>
      {error && <div className="error-box">{error}</div>}
      <section className="panel">
        <div className="panel-title">
          <h3>Database metrics</h3>
          <div className="actions">
            <label className="checkbox"><input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} /> auto refresh 5s</label>
            <button className="small" onClick={() => void loadMetrics()}>Refresh</button>
          </div>
        </div>
        <div className="stat-grid">
          <div className="stat"><span>Requests</span><strong>{metricNumber(metricsData, 'requests_total').toLocaleString()}</strong></div>
          <div className="stat"><span>Read ops</span><strong>{metricNumber(metricsData, 'read_ops_total').toLocaleString()}</strong></div>
          <div className="stat"><span>Write ops</span><strong>{metricNumber(metricsData, 'write_ops_total').toLocaleString()}</strong></div>
          <div className="stat"><span>Errors</span><strong>{metricNumber(metricsData, 'errors_total').toLocaleString()}</strong></div>
          <div className="stat"><span>Data disk used</span><strong>{diskUsedPercent.toFixed(1)}%</strong></div>
          <div className="stat"><span>Snapshot age</span><strong>{snapshotAge == null ? '-' : `${Math.round(snapshotAge)} s`}</strong></div>
        </div>
        <MetricBars data={metricsData} items={[['requests_total','Requests'],['read_ops_total','Reads'],['write_ops_total','Writes'],['cache_hits_total','Cache hits'],['cache_misses_total','Cache misses'],['errors_total','Errors']]} />
        <div className="chart-grid">
          <MetricChart samples={samples} metric="requests" label="Requests" />
          <MetricChart samples={samples} metric="reads" label="Reads" />
          <MetricChart samples={samples} metric="writes" label="Writes" />
          <MetricChart samples={samples} metric="errors" label="Errors" />
        </div>
        <div className="metric-inline">
          <span>Avg commit: <strong>{metricNumber(metricsData, 'avg_commit_us').toFixed(1)} µs</strong></span>
          <span>Avg batch: <strong>{metricNumber(metricsData, 'avg_batch_size').toFixed(2)}</strong></span>
          <span>Queue: <strong>{metricNumber(metricsData, 'queue_len')}</strong></span>
          <span>TTL backlog: <strong>{metricNumber(metricsData, 'ttl_backlog')}</strong></span>
        </div>
        {metricsData && <details className="raw-metrics"><summary>Raw metrics JSON</summary><pre className="result">{pretty(metricsData)}</pre></details>}
      </section>

    </>
  );
}



type BackupForm = {
  id: string;
  databases: string[];
  enabled: boolean;
  every_minutes: string;
  backupType: 'full' | 'restic';
  destinationKind: 'local' | 's3';
  path: string;
  endpoint: string;
  bucket: string;
  region: string;
  prefix: string;
  access_key_env: string;
  secret_key_env: string;
  session_token_env: string;
  restic_repository: string;
  restic_password_file: string;
  restic_binary: string;
  encrypt: boolean;
  compression: 'none' | 'gzip';
  identity_file: string;
  recipient_file: string;
  keep_last: string;
  verify: boolean;
  exclude_collections: string[];
  include_search_sidecars: boolean;
};

function defaultBackupForm(
  keyDir = '/var/lib/nonodb/backup-keys',
  localPath = '/var/lib/nonodb/backups',
  resticRepository = '/var/lib/nonodb/restic-repo',
): BackupForm {
  return {
    id: 'nightly-default',
    databases: ['default'],
    enabled: true,
    every_minutes: '1440',
    backupType: 'full',
    destinationKind: 'local',
    path: localPath,
    endpoint: '',
    bucket: '',
    region: 'auto',
    prefix: 'nonodb',
    access_key_env: 'AWS_ACCESS_KEY_ID',
    secret_key_env: 'AWS_SECRET_ACCESS_KEY',
    session_token_env: '',
    restic_repository: resticRepository,
    restic_password_file: `${keyDir}/restic-password`,
    restic_binary: 'restic',
    encrypt: false,
    compression: 'none',
    identity_file: `${keyDir}/backup-identity.txt`,
    recipient_file: `${keyDir}/backup-recipient.txt`,
    keep_last: '14',
    verify: true,
    exclude_collections: [],
    include_search_sidecars: true,
  };
}

function formFromBackup(item: BackupItem, keyDir?: string): BackupForm {
  const job = item.job;
  const base = defaultBackupForm(keyDir);
  if (job.destination.kind === 'local') {
    return {
      ...base,
      id: job.id,
      databases: job.databases?.length ? job.databases : [job.database],
      enabled: job.enabled,
      every_minutes: String(job.every_minutes),
      backupType: 'full',
      destinationKind: 'local',
      path: job.destination.path,
      encrypt: Boolean(job.encryption),
      compression: job.compression === 'gzip' ? 'gzip' : 'none',
      recipient_file: job.encryption?.recipient_file ?? '',
      keep_last: String(job.keep_last),
      verify: job.verify,
      exclude_collections: job.exclude_collections ?? [],
      include_search_sidecars: job.include_search_sidecars ?? true,
    };
  }
  if (job.destination.kind === 'restic') {
    return {
      ...base,
      id: job.id,
      databases: job.databases?.length ? job.databases : [job.database],
      enabled: job.enabled,
      every_minutes: String(job.every_minutes),
      backupType: 'restic',
      destinationKind: job.destination.repository.startsWith('s3:') ? 's3' : 'local',
      restic_repository: job.destination.repository,
      restic_password_file: job.destination.password_file,
      restic_binary: job.destination.binary,
      encrypt: false,
      compression: 'none',
      recipient_file: '',
      keep_last: String(job.keep_last),
      verify: job.verify,
      exclude_collections: job.exclude_collections ?? [],
      include_search_sidecars: job.include_search_sidecars ?? true,
    };
  }
  return {
    ...base,
    id: job.id,
    databases: job.databases?.length ? job.databases : [job.database],
    enabled: job.enabled,
    every_minutes: String(job.every_minutes),
    backupType: 'full',
    destinationKind: 's3',
    endpoint: job.destination.endpoint,
    bucket: job.destination.bucket,
    region: job.destination.region,
    prefix: job.destination.prefix,
    access_key_env: job.destination.access_key_env,
    secret_key_env: job.destination.secret_key_env,
    session_token_env: job.destination.session_token_env ?? '',
    encrypt: Boolean(job.encryption),
    compression: job.compression === 'gzip' ? 'gzip' : 'none',
    recipient_file: job.encryption?.recipient_file ?? '',
    keep_last: String(job.keep_last),
    verify: job.verify,
    exclude_collections: job.exclude_collections ?? [],
    include_search_sidecars: job.include_search_sidecars ?? true,
  };
}

function backupFromForm(form: BackupForm): BackupJob {
  const databases = form.databases.length > 0 ? form.databases : ['default'];
  return {
    id: form.id.trim(),
    database: databases[0],
    databases,
    enabled: form.enabled,
    every_minutes: Number(form.every_minutes),
    destination:
      form.backupType === 'restic'
        ? {
            kind: 'restic',
            repository: form.restic_repository.trim(),
            password_file: form.restic_password_file.trim(),
            binary: form.restic_binary.trim() || 'restic',
          }
        : form.destinationKind === 'local'
          ? { kind: 'local', path: form.path.trim() }
          : {
              kind: 's3',
              endpoint: form.endpoint.trim(),
              bucket: form.bucket.trim(),
              region: form.region.trim() || 'auto',
              prefix: form.prefix.trim(),
              access_key_env: form.access_key_env.trim() || 'AWS_ACCESS_KEY_ID',
              secret_key_env: form.secret_key_env.trim() || 'AWS_SECRET_ACCESS_KEY',
              ...(form.session_token_env.trim()
                ? { session_token_env: form.session_token_env.trim() }
                : {}),
            },
    ...(form.encrypt && form.backupType === 'full'
      ? { encryption: { kind: 'age' as const, recipient_file: form.recipient_file.trim() } }
      : {}),
    compression: form.backupType === 'full' ? form.compression : 'none',
    keep_last: Number(form.keep_last),
    verify: form.verify,
    exclude_collections: form.exclude_collections,
    include_search_sidecars: form.include_search_sidecars,
  };
}

function destinationLabel(job: BackupJob): string {
  if (job.destination.kind === 'local') return job.destination.path;
  if (job.destination.kind === 'restic') return `restic:${job.destination.repository}`;
  return `s3://${job.destination.bucket}/${job.destination.prefix}`;
}

function BackupsPage({ connection }: { connection: Connection }) {
  const [data, setData] = useState<BackupsView | null>(null);
  const [form, setForm] = useState<BackupForm>(defaultBackupForm);
  const [editing, setEditing] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [databases, setDatabases] = useState<string[]>([]);
  const [collections, setCollections] = useState<string[]>([]);
  const [disclaimerAccepted, setDisclaimerAccepted] = useState(() => {
    try {
      return localStorage.getItem('nonodb.gui.backup-disclaimer') === '1';
    } catch {
      return false;
    }
  });

  const load = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      setData(await backupsList(connection));
    } catch (err) {
      setError(errorText(err));
    } finally {
      setLoading(false);
    }
  }, [connection]);

  useEffect(() => {
    void load();
  }, [load]);

  const anyRunning = data?.jobs.some((item) => item.state.running) ?? false;
  useEffect(() => {
    if (!anyRunning) return;
    const timer = window.setInterval(() => void load(), 1000);
    return () => window.clearInterval(timer);
  }, [anyRunning, load]);

  const watchRun = async (id: string) => {
    for (let attempt = 0; attempt < 240; attempt += 1) {
      await new Promise((resolve) => window.setTimeout(resolve, 500));
      try {
        const view = await backupsList(connection);
        setData(view);
        const item = view.jobs.find((candidate) => candidate.job.id === id);
        if (!item || item.state.running) continue;
        if (item.state.last_status === 'ok') {
          const size = item.state.last_size_bytes > 0 ? ` (${formatBytes(item.state.last_size_bytes)})` : '';
          setNotice(`Backup ${id} completed${size}: ${item.state.last_destination}`);
        } else if (item.state.last_status === 'error') {
          setError(`Backup ${id} failed: ${item.state.last_error ?? 'unknown error'}`);
        }
        return;
      } catch {
        return;
      }
    }
  };

  useEffect(() => {
    if (!data || editing) return;
    const generic = defaultBackupForm();
    setForm((current) => ({
      ...current,
      identity_file:
        !current.identity_file || current.identity_file === generic.identity_file
          ? data.default_identity_file
          : current.identity_file,
      recipient_file:
        !current.recipient_file || current.recipient_file === generic.recipient_file
          ? data.default_recipient_file
          : current.recipient_file,
      restic_password_file:
        !current.restic_password_file ||
        current.restic_password_file === generic.restic_password_file
          ? data.default_restic_password_file
          : current.restic_password_file,
      path: !current.path || current.path === generic.path ? data.default_local_path : current.path,
      restic_repository:
        !current.restic_repository ||
        current.restic_repository === generic.restic_repository
          ? data.default_restic_repository
          : current.restic_repository,
    }));
  }, [data, editing]);

  useEffect(() => {
    let cancelled = false;
    exec<{ databases: string[] }>(connection, { op: 'list_databases' })
      .then((result) => {
        if (!cancelled) setDatabases(result.databases ?? []);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [connection]);

  useEffect(() => {
    let cancelled = false;
    const selected = form.databases.length > 0 ? form.databases : ['default'];
    Promise.all(
      selected.map((database) =>
        exec<{ collections: string[] }>(
          { ...connection, database },
          { op: 'list_collections' },
        ).catch(() => ({ collections: [] as string[] })),
      ),
    ).then((results) => {
      if (cancelled) return;
      const union = [...new Set(results.flatMap((result) => result.collections ?? []))].sort();
      setCollections(union);
      setForm((current) => ({
        ...current,
        exclude_collections: current.exclude_collections.filter((name) => union.includes(name)),
      }));
    });
    return () => {
      cancelled = true;
    };
  }, [connection, form.databases]);

  useEffect(() => {
    if (!notice) return;
    const timer = window.setTimeout(() => setNotice(''), 5000);
    return () => window.clearTimeout(timer);
  }, [notice]);

  const setField = <K extends keyof BackupForm>(key: K, value: BackupForm[K]) => {
    setForm((current) => ({ ...current, [key]: value }));
  };

  const setDisclaimer = (accepted: boolean) => {
    setDisclaimerAccepted(accepted);
    try {
      if (accepted) localStorage.setItem('nonodb.gui.backup-disclaimer', '1');
      else localStorage.removeItem('nonodb.gui.backup-disclaimer');
    } catch {
      // Storage can be unavailable in hardened browser profiles.
    }
  };

  const toggleExcluded = (name: string) => {
    setForm((current) => ({
      ...current,
      exclude_collections: current.exclude_collections.includes(name)
        ? current.exclude_collections.filter((item) => item !== name)
        : [...current.exclude_collections, name],
    }));
  };

  const toggleDatabase = (name: string) => {
    setForm((current) => ({
      ...current,
      databases: current.databases.includes(name)
        ? current.databases.filter((item) => item !== name)
        : [...current.databases, name].sort(),
    }));
  };

  const generateAgeKey = async () => {
    const identity = form.identity_file.trim();
    const recipient = form.recipient_file.trim();
    if (!identity || !recipient) {
      setError('Identity and recipient file paths are required.');
      return;
    }
    setBusy(true);
    setError('');
    try {
      const key = await generateBackupKey(connection, identity, recipient);
      setForm((current) => ({
        ...current,
        encrypt: true,
        identity_file: key.identity_file,
        recipient_file: key.recipient_file,
      }));
      setNotice(`Generated backup key. Identity stored at ${key.identity_file} (never displayed).`);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const generateResticPassword = async () => {
    const path = form.restic_password_file.trim();
    if (!path) {
      setError('Restic password file path is required.');
      return;
    }
    setBusy(true);
    setError('');
    try {
      await generateBackupSecret(connection, path);
      setNotice(`Generated restic password file ${path}. It is not displayed or editable in the GUI.`);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const resetForm = () => {
    setEditing(null);
    setForm(defaultBackupForm(data?.key_dir, data?.default_local_path, data?.default_restic_repository));
  };

  const save = async () => {
    setBusy(true);
    setError('');
    try {
      const job = backupFromForm(form);
      if (!job.id) throw new Error('Job ID is required.');
      if (form.databases.length === 0) throw new Error('Select at least one database.');
      if (!job.every_minutes || job.every_minutes < 1) throw new Error('Interval must be at least 1 minute.');
      await saveBackupJob(connection, job);
      setNotice(`Saved backup job ${job.id}`);
      resetForm();
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const saveAndRun = async () => {
    setBusy(true);
    setError('');
    try {
      const job = backupFromForm(form);
      if (!job.id) throw new Error('Job ID is required.');
      if (form.databases.length === 0) throw new Error('Select at least one database.');
      if (!job.every_minutes || job.every_minutes < 1) throw new Error('Interval must be at least 1 minute.');
      await saveBackupJob(connection, job);
      await runBackupJob(connection, job.id);
      setNotice(`Backup job ${job.id} started`);
      await load();
      void watchRun(job.id);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const edit = (item: BackupItem) => {
    setEditing(item.job.id);
    setForm(formFromBackup(item, data?.key_dir));
    setError('');
  };

  const remove = async (id: string) => {
    if (!window.confirm(`Delete backup job ${id}?`)) return;
    setBusy(true);
    setError('');
    try {
      await deleteBackupJob(connection, id);
      setNotice(`Deleted backup job ${id}`);
      if (editing === id) resetForm();
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const run = async (id: string) => {
    setBusy(true);
    setError('');
    try {
      await runBackupJob(connection, id);
      setNotice(`Backup job ${id} started`);
      void watchRun(id);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const formatUnix = (value: number) =>
    value > 0 ? new Date(value * 1000).toLocaleString() : '—';

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <h2>Backups</h2>
          <p>Scheduled backups with native age/S3 or restic deduplication and encryption</p>
        </div>
        <div className="actions">
          <button onClick={() => void load()} disabled={loading}>{loading ? 'Loading…' : 'Refresh'}</button>
        </div>
      </header>
      {error && <div className="error-box">{error}</div>}
      {notice && <div className="notice-box">{notice}</div>}
      <section className={`panel disclaimer-box${disclaimerAccepted ? ' accepted' : ''}`}>
        <div className="panel-title">
          <h3>Backup disclaimer</h3>
          <span className={`badge ${disclaimerAccepted ? '' : 'danger-badge'}`}>
            {disclaimerAccepted ? 'accepted' : 'required'}
          </span>
        </div>
        <p>
          Backup and encryption features are provided <strong>as-is, without warranty</strong>.
          The authors and maintainers accept no responsibility for the quality or strength of
          encryption, key management, backup correctness, backup failures, restore failures,
          or data loss. You use these features entirely at your own risk.
        </p>
        <p>
          You are responsible for protecting keys and credentials, monitoring jobs, and
          regularly verifying backups with real restore tests. Do not rely on a backup that
          has never been restored and verified.
        </p>
        <label className="checkbox">
          <input
            type="checkbox"
            checked={disclaimerAccepted}
            onChange={(e) => setDisclaimer(e.target.checked)}
          />
          I understand and accept this disclaimer. I will verify backups and restores myself.
        </label>
      </section>
      {data && (
        <section className="panel">
          <p className="muted small">
            Config: <code>{data.config_path}</code> · Staging: <code>{data.staging_path}</code> · Keys: <code>{data.key_dir}</code>.
            S3 credentials are read from the named environment variables and are never stored in the GUI.
            In Docker these are container paths; mount the key directory and any local backup destination as persistent volumes.
          </p>
        </section>
      )}

      <section className="panel">
        <div className="panel-title"><h3>Jobs</h3><span>{data?.jobs.length ?? 0}</span></div>
        {!data || data.jobs.length === 0 ? (
          <p className="muted">No backup jobs configured.</p>
        ) : (
          <div className="index-list">
            {data.jobs.map((item) => (
              <div key={item.job.id} className="index-row backup-row">
                <div>
                  <strong>{item.job.id}</strong>
                  <span className={`badge ${item.state.running ? '' : item.state.last_status === 'error' ? 'danger-badge' : ''}`}>
                    {item.state.running ? 'running' : item.state.last_status || 'new'}
                  </span>
                  <div className="muted small">
                    {(item.job.databases?.length ? item.job.databases : [item.job.database]).join(', ')} · every {item.job.every_minutes} min · {destinationLabel(item.job)}
                    {item.job.encryption ? ' · encrypted (age)' : ' · unencrypted'}
                    {item.job.compression === 'gzip' ? ' · gzip' : ''}
                    {item.job.destination.kind === 'restic' ? ' · restic compression' : ''}
                    {item.job.include_search_sidecars && !(item.job.exclude_collections?.length) ? ' · search sidecars' : ''}
                    {item.job.exclude_collections?.length
                      ? ` · excludes ${item.job.exclude_collections.join(', ')}`
                      : ''}
                  </div>
                  <div className="muted small">
                    Last: {formatUnix(item.state.last_run_unix)} · Next: {formatUnix(item.state.next_run_unix)}
                    {item.state.last_size_bytes > 0 ? ` · ${formatBytes(item.state.last_size_bytes)}` : ''}
                  </div>
                  {item.state.last_destination && (
                    <div className="muted small">
                      Destination: <code>{item.state.last_destination}</code>
                    </div>
                  )}
                  {item.state.last_error && <div className="muted small error-text">{item.state.last_error}</div>}
                </div>
                <div className="actions">
                  <button className="small" onClick={() => void run(item.job.id)} disabled={busy || !disclaimerAccepted || item.state.running || !item.job.enabled} title={!disclaimerAccepted ? 'Accept the backup disclaimer first' : undefined}>Run now</button>
                  <button className="small" onClick={() => edit(item)} disabled={busy}>Edit</button>
                  <button className="small" onClick={() => void remove(item.job.id)} disabled={busy}>Delete</button>
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="panel">
        <div className="panel-title">
          <h3>{editing ? `Edit ${editing}` : 'New backup job'}</h3>
          {editing && <button className="small" onClick={resetForm}>Cancel edit</button>}
        </div>
        <div className="form-grid">
          <label>
            Job ID
            <input value={form.id} onChange={(e) => setField('id', e.target.value)} disabled={editing !== null} placeholder="nightly-default" />
          </label>
          <div className="form-wide">
            <span className="field-label">Databases</span>
            <div className="checkbox-grid database-grid">
              {[...new Set(['default', connection.database || 'default', ...databases, ...form.databases])].map((name) => (
                <label key={name} className="checkbox">
                  <input
                    type="checkbox"
                    checked={form.databases.includes(name)}
                    onChange={() => toggleDatabase(name)}
                  />
                  {name}
                </label>
              ))}
            </div>
            <span className="field-hint">Select one or more databases. Each database gets its own copy under the job destination.</span>
          </div>
          <label>
            Every minutes
            <input type="number" min={1} value={form.every_minutes} onChange={(e) => setField('every_minutes', e.target.value)} />
            <span className="field-hint">1440 = daily. The schedule is interval-based, UTC-based timestamps are shown after runs.</span>
          </label>
          <label>
            Keep last
            <input type="number" min={0} value={form.keep_last} onChange={(e) => setField('keep_last', e.target.value)} />
            <span className="field-hint">Local destinations delete older job directories after success. 0 keeps all.</span>
          </label>
          <label>
            Backup type
            <select
              value={form.backupType}
              onChange={(e) => {
                const kind = e.target.value as 'full' | 'restic';
                setField('backupType', kind);
                if (kind === 'restic') setField('encrypt', false);
              }}
            >
              <option value="full">Full copy (native)</option>
              <option value="restic">Incremental + deduplication (restic)</option>
            </select>
            <span className="field-hint">
              {form.backupType === 'full'
                ? 'Every run copies the full database. Optional age encryption.'
                : 'Restic stores encrypted, deduplicated incremental snapshots.'}
            </span>
          </label>
          <label>
            Where
            <select
              value={form.destinationKind}
              onChange={(e) => setField('destinationKind', e.target.value as 'local' | 's3')}
            >
              <option value="local">Local directory / filesystem</option>
              <option value="s3">S3 / Cloudflare R2</option>
            </select>
          </label>
          {form.backupType === 'full' && form.destinationKind === 'local' && (
            <label>
              Local path
              <input value={form.path} onChange={(e) => setField('path', e.target.value)} placeholder="/var/backups/nonodb" />
              <span className="field-hint">Default: {data?.default_local_path ?? 'under NONODB_DATA/backups'}. This path must be writable by the daemon user. In Docker it is a container path; mount it as a volume.</span>
            </label>
          )}
          {form.backupType === 'full' && form.destinationKind === 's3' && (
            <>
              <label>
                S3 endpoint
                <input value={form.endpoint} onChange={(e) => setField('endpoint', e.target.value)} placeholder="https://account.r2.cloudflarestorage.com" />
              </label>
              <label>Bucket<input value={form.bucket} onChange={(e) => setField('bucket', e.target.value)} placeholder="nonodb-backups" /></label>
              <label>Region<input value={form.region} onChange={(e) => setField('region', e.target.value)} placeholder="auto" /></label>
              <label>Prefix<input value={form.prefix} onChange={(e) => setField('prefix', e.target.value)} placeholder="prod/nonodb" /></label>
              <label>Access key env<input value={form.access_key_env} onChange={(e) => setField('access_key_env', e.target.value)} /></label>
              <label>Secret key env<input value={form.secret_key_env} onChange={(e) => setField('secret_key_env', e.target.value)} /></label>
              <label>Session token env (optional)<input value={form.session_token_env} onChange={(e) => setField('session_token_env', e.target.value)} /></label>
            </>
          )}
          {form.backupType === 'restic' && (
            <>
              <label>
                {form.destinationKind === 'local' ? 'Restic local repository' : 'Restic S3 repository URL'}
                <input
                  value={form.restic_repository}
                  onChange={(e) => setField('restic_repository', e.target.value)}
                  placeholder={
                    form.destinationKind === 'local'
                      ? '/var/backups/nonodb-restic'
                      : 's3:https://account.r2.cloudflarestorage.com/bucket/prefix'
                  }
                />
                <span className="field-hint">
                  {form.destinationKind === 'local'
                    ? `Default: ${data?.default_restic_repository ?? 'under NONODB_DATA/restic-repo'}. This path must be writable by the daemon user. In Docker it must be a mounted volume.`
                    : 'Use a restic S3 URL. Credentials and AWS_DEFAULT_REGION are read from the daemon environment.'}
                </span>
              </label>
              <label>
                Restic password file
                <div className="inline-input">
                  <input value={form.restic_password_file} onChange={(e) => setField('restic_password_file', e.target.value)} placeholder="/etc/nonodb/restic-password" />
                  <button type="button" className="small" onClick={() => void generateResticPassword()} disabled={busy}>Generate</button>
                </div>
                <span className="field-hint">Generated once with mode 0600. The password itself is never displayed, stored in this config, or editable in the GUI.</span>
              </label>
              <label>
                Restic binary
                <input value={form.restic_binary} onChange={(e) => setField('restic_binary', e.target.value)} placeholder="restic" />
                <span className="field-hint">Restic must be installed on the daemon host or present in the container image.</span>
              </label>
            </>
          )}
          {form.backupType === 'full' && (
            <>
              <label>
                Compression
                <select value={form.compression} onChange={(e) => setField('compression', e.target.value as 'none' | 'gzip')}>
                  <option value="none">None</option>
                  <option value="gzip">Gzip</option>
                </select>
                <span className="field-hint">Compresses the copy before optional age encryption. Restic compresses automatically.</span>
              </label>
              <label>
                Encryption
                <span className="checkbox"><input type="checkbox" checked={form.encrypt} onChange={(e) => setField('encrypt', e.target.checked)} /> age recipient file</span>
              </label>
              {form.encrypt && (
                <>
                  <label>
                    age identity file
                    <input value={form.identity_file} onChange={(e) => setField('identity_file', e.target.value)} placeholder="/etc/nonodb/backup-identity.txt" />
                    <span className="field-hint">Private key. Written once with mode 0600, never displayed or editable in the GUI. Keep it separate from the backup destination.</span>
                  </label>
                  <label>
                    age recipient file
                    <div className="inline-input">
                      <input value={form.recipient_file} onChange={(e) => setField('recipient_file', e.target.value)} placeholder="/etc/nonodb/backup-recipient.txt" />
                      <button type="button" className="small" onClick={() => void generateAgeKey()} disabled={busy}>Generate key</button>
                    </div>
                    <span className="field-hint">Public key used to encrypt backups. Written once. Restore uses the separate identity file via <code>NONODB_BACKUP_IDENTITY_FILE</code>.</span>
                  </label>
                </>
              )}
            </>
          )}
          <label className="checkbox">
            <input
              type="checkbox"
              checked={form.include_search_sidecars}
              disabled={form.exclude_collections.length > 0}
              onChange={(e) => setField('include_search_sidecars', e.target.checked)}
            />{' '}
            Include search sidecars
            <span className="field-hint">
              Copies segment and ANN files next to the LMDB snapshot. Age-encrypted jobs encrypt the sidecars too. Excluded collections disable sidecar copies.
            </span>
          </label>
          <div className="form-wide">
            <span className="field-label">Excluded collections</span>
            {collections.length === 0 ? (
              <p className="muted small">No collections in the selected databases.</p>
            ) : (
              <div className="checkbox-grid">
                {collections.map((name) => (
                  <label key={name} className="checkbox">
                    <input
                      type="checkbox"
                      checked={form.exclude_collections.includes(name)}
                      onChange={() => toggleExcluded(name)}
                    />
                    {name}
                  </label>
                ))}
              </div>
            )}
            <span className="field-hint">Selected collections are removed from every selected database copy only. The live databases are unchanged.</span>
          </div>
          <label className="checkbox"><input type="checkbox" checked={form.verify} onChange={(e) => setField('verify', e.target.checked)} /> Deep-verify before encryption/upload</label>
          <label className="checkbox"><input type="checkbox" checked={form.enabled} onChange={(e) => setField('enabled', e.target.checked)} /> Enabled</label>
        </div>
        <div className="toolbar">
          <button className="primary" onClick={() => void save()} disabled={busy || !disclaimerAccepted} title={!disclaimerAccepted ? 'Accept the backup disclaimer first' : undefined}>{editing ? 'Update job' : 'Create job'}</button>
          <button onClick={() => void saveAndRun()} disabled={busy || !disclaimerAccepted} title={!disclaimerAccepted ? 'Accept the backup disclaimer first' : undefined}>Save & run now</button>
          {editing && <button onClick={resetForm}>Cancel</button>}
        </div>
      </section>
    </div>
  );
}

function ImportsPage({ connection }: { connection: Connection }) {
  const [overview, setOverview] = useState<ImportOverview | null>(null);
  const [sourceKind, setSourceKind] = useState<'local' | 's3'>('local');
  const [localPath, setLocalPath] = useState('');
  const [endpoint, setEndpoint] = useState('');
  const [bucket, setBucket] = useState('');
  const [region, setRegion] = useState('auto');
  const [prefix, setPrefix] = useState('');
  const [accessKeyEnv, setAccessKeyEnv] = useState('AWS_ACCESS_KEY_ID');
  const [secretKeyEnv, setSecretKeyEnv] = useState('AWS_SECRET_ACCESS_KEY');
  const [sessionTokenEnv, setSessionTokenEnv] = useState('');
  const [backups, setBackups] = useState<DiscoveredImport[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [target, setTarget] = useState('');
  const [status, setStatus] = useState<ImportStatus | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');

  const loadOverview = useCallback(async () => {
    try {
      const value = await importOverview(connection);
      setOverview(value);
      setLocalPath((current) => current || value.default_root || value.roots[0] || '');
    } catch (err) {
      setError(errorText(err));
    }
  }, [connection]);

  useEffect(() => {
    void loadOverview();
  }, [loadOverview]);

  useEffect(() => {
    if (!status || !['queued', 'running'].includes(status.status)) return;
    const timer = window.setInterval(() => {
      void importStatus(connection, status.id)
        .then((next) => {
          setStatus(next);
          if (next.status === 'ok') {
            setNotice(`Imported database ${next.target_database}`);
            void loadOverview();
          } else if (next.status === 'error') {
            setError(next.error ?? 'import failed');
          }
        })
        .catch((err) => setError(errorText(err)));
    }, 700);
    return () => window.clearInterval(timer);
  }, [connection, status, loadOverview]);

  const source = (): ImportSource =>
    sourceKind === 'local'
      ? { kind: 'local', path: localPath.trim() }
      : {
          kind: 's3',
          endpoint: endpoint.trim(),
          bucket: bucket.trim(),
          region: region.trim() || 'auto',
          prefix: prefix.trim(),
          access_key_env: accessKeyEnv.trim() || 'AWS_ACCESS_KEY_ID',
          secret_key_env: secretKeyEnv.trim() || 'AWS_SECRET_ACCESS_KEY',
          ...(sessionTokenEnv.trim() ? { session_token_env: sessionTokenEnv.trim() } : {}),
        };

  const browse = async () => {
    setBusy(true);
    setError('');
    setNotice('');
    try {
      const result = await browseImports(connection, source());
      setBackups(result.backups);
      setSelected(result.backups[0]?.location ?? null);
      if (result.backups[0]) {
        setTarget(`restored-${result.backups[0].manifest.source}`);
      }
      setNotice(`Found ${result.backups.length} backup${result.backups.length === 1 ? '' : 's'}`);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const choose = (item: DiscoveredImport) => {
    setSelected(item.location);
    setTarget(`restored-${item.manifest.source}`);
    setStatus(null);
    setError('');
  };

  const start = async () => {
    if (!selected) throw new Error('Select a backup first.');
    if (!target.trim()) throw new Error('Target database name is required.');
    setBusy(true);
    setError('');
    setNotice('');
    try {
      const next = await runImport(connection, source(), selected, target.trim());
      setStatus(next);
      setNotice(`Import ${next.id} started`);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const selectedBackup = backups.find((item) => item.location === selected);
  const formatTime = (ms: number) => (ms > 0 ? new Date(ms).toLocaleString() : '—');
  const formatSize = (bytes: number) => formatBytes(bytes);

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <h2>Import</h2>
          <p>Import a verified backup as a new database. The live database is never replaced.</p>
        </div>
        <div className="actions">
          <button onClick={() => void loadOverview()}>Refresh</button>
        </div>
      </header>
      {error && <div className="error-box">{error}</div>}
      {notice && <div className="notice-box">{notice}</div>}

      <section className="panel">
        <div className="panel-title"><h3>Source</h3></div>
        <div className="form-grid">
          <label>
            Source type
            <select value={sourceKind} onChange={(e) => setSourceKind(e.target.value as 'local' | 's3')}>
              <option value="local">Local folder</option>
              <option value="s3">S3 / Cloudflare R2</option>
            </select>
          </label>
          {sourceKind === 'local' ? (
            <label>
              Folder on the daemon/container
              <input value={localPath} onChange={(e) => setLocalPath(e.target.value)} placeholder={overview?.default_root ?? '/var/lib/nonodb/backups'} />
              <span className="field-hint">
                Allowed roots: <code>{overview?.roots.join(', ') || 'NONODB_DATA/backups'}</code>.
                Configure more with <code>NONODB_IMPORT_ROOTS</code>.
              </span>
            </label>
          ) : (
            <>
              <label>S3 endpoint<input value={endpoint} onChange={(e) => setEndpoint(e.target.value)} placeholder="https://account.r2.cloudflarestorage.com" /></label>
              <label>Bucket<input value={bucket} onChange={(e) => setBucket(e.target.value)} placeholder="nonodb-backups" /></label>
              <label>Region<input value={region} onChange={(e) => setRegion(e.target.value)} placeholder="auto" /></label>
              <label>Prefix<input value={prefix} onChange={(e) => setPrefix(e.target.value)} placeholder="prod/nonodb" /></label>
              <label>Access key env<input value={accessKeyEnv} onChange={(e) => setAccessKeyEnv(e.target.value)} /></label>
              <label>Secret key env<input value={secretKeyEnv} onChange={(e) => setSecretKeyEnv(e.target.value)} /></label>
              <label>Session token env<input value={sessionTokenEnv} onChange={(e) => setSessionTokenEnv(e.target.value)} /></label>
            </>
          )}
        </div>
        <div className="toolbar">
          <button className="primary" onClick={() => void browse()} disabled={busy}>
            {busy ? 'Browsing…' : 'Browse backups'}
          </button>
        </div>
      </section>

      <section className="panel">
        <div className="panel-title"><h3>Backups</h3><span>{backups.length}</span></div>
        {backups.length === 0 ? (
          <p className="muted">No backups found. Choose a source and click Browse backups.</p>
        ) : (
          <div className="index-list">
            {backups.map((item) => (
              <button
                key={`${item.location}:${item.manifest.sha256}`}
                className={`index-row import-row${selected === item.location ? ' selected' : ''}`}
                onClick={() => choose(item)}
              >
                <div>
                  <strong>{item.manifest.source}</strong>
                  <span className="badge">{item.manifest.compression?.kind ?? 'no compression'}</span>
                  <span className="badge">{item.manifest.encryption?.kind ? `encrypted (${item.manifest.encryption.kind})` : 'unencrypted'}</span>
                  {item.manifest.excluded_collections?.length ? <span className="badge danger-badge">partial</span> : null}
                  <div className="muted small">
                    {formatTime(item.manifest.created_at_ms)} · {formatSize(item.manifest.data_size)}
                    {item.manifest.job_id ? ` · job ${item.manifest.job_id}` : ''}
                  </div>
                  <div className="muted small mono">{item.location}</div>
                </div>
                <span className="muted small">{item.info ? 'info file present' : ''}</span>
              </button>
            ))}
          </div>
        )}
      </section>

      {selectedBackup && (
        <section className="panel">
          <div className="panel-title"><h3>Import plan</h3></div>
          <div className="form-grid">
            <label>
              Target database
              <input value={target} onChange={(e) => setTarget(e.target.value)} placeholder={`restored-${selectedBackup.manifest.source}`} />
              <span className="field-hint">Must not already exist. Import never overwrites a database.</span>
            </label>
            <label>
              Selected backup
              <input readOnly value={selectedBackup.location} />
            </label>
          </div>
          {selectedBackup.manifest.encryption && (
            <p className="field-hint">
              This backup is encrypted. The daemon must have
              <code> NONODB_BACKUP_IDENTITY_FILE </code>
              pointing to the matching age identity file.
            </p>
          )}
          {selectedBackup.info && (
            <details className="raw-metrics">
              <summary>backup-info.txt</summary>
              <pre className="result">{selectedBackup.info}</pre>
            </details>
          )}
          <div className="toolbar">
            <button className="primary" onClick={() => void start()} disabled={busy || !target.trim()}>
              Verify &amp; import as new database
            </button>
          </div>
          {status && (
            <div className={status.status === 'error' ? 'error-box' : 'notice-box'}>
              <strong>{status.id}</strong> · {status.status}
              {status.error ? `: ${status.error}` : ''}
              {status.status === 'ok' ? ` · ${formatSize(status.bytes)} → ${status.target_database}` : ''}
            </div>
          )}
        </section>
      )}

      <section className="panel">
        <div className="panel-title"><h3>Recent imports</h3><span>{overview?.jobs.length ?? 0}</span></div>
        {overview?.jobs.length ? (
          <div className="index-list">
            {overview.jobs.map((item) => (
              <div key={item.id} className="index-row">
                <div>
                  <strong>{item.id}</strong>
                  <span className={`badge ${item.status === 'error' ? 'danger-badge' : ''}`}>{item.status}</span>
                  <div className="muted small">{item.target_database} · {item.source}</div>
                  {item.error && <div className="muted small error-text">{item.error}</div>}
                </div>
                <div className="muted small">{formatSize(item.bytes)}</div>
              </div>
            ))}
          </div>
        ) : (
          <p className="muted">No imports yet.</p>
        )}
      </section>
    </div>
  );
}

function RepStat({
  label,
  value,
  help,
  danger = false,
}: {
  label: string;
  value: ReactNode;
  help: string;
  danger?: boolean;
}) {
  return (
    <div className="stat" title={help}>
      <span>{label}</span>
      <strong className={danger ? 'danger-text' : undefined}>{value}</strong>
      <small className="stat-note">{help}</small>
    </div>
  );
}

function ClusterPage({ connection }: { connection: Connection }) {
  const [cluster, setCluster] = useState<ClusterStatus | null>(null);
  const [progress, setProgress] = useState<TestProgress | null>(null);
  const [replication, setReplication] = useState<ReplicationStatus | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [nextCluster, nextProgress, nextReplication] = await Promise.all([
        clusterStatus(connection),
        testProgress(connection),
        replicationStatus(connection).catch(() => null),
      ]);
      setCluster(nextCluster);
      setProgress(nextProgress);
      setReplication(nextReplication);
      setError('');
    } catch (err) {
      setError(errorText(err));
    } finally {
      setLoading(false);
    }
  }, [connection]);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 3000);
    return () => window.clearInterval(timer);
  }, [load]);

  const forgetClusterAck = async (node: string) => {
    if (!window.confirm(
      `Remove replica ${node} from the primary view?\n\n` +
      'This deletes its last ACK. A still-running replica will reappear on its next ACK; pause or disconnect it first.',
    )) return;
    try {
      await forgetReplicaAck(connection, node);
      await load();
    } catch (err) {
      setError(errorText(err));
    }
  };

  const nodes = cluster?.nodes ?? [];
  const online = nodes.filter((node) => node.ok);
  const primaries = online.filter((node) => node.status?.role === 'primary');
  const replicas = online.filter((node) => node.status?.role !== 'primary');
  const maxLag = Math.max(
    0,
    ...online.flatMap((node) => (node.status?.applied ?? []).map((applied) => applied.lag ?? 0)),
  );
  const staleAcks = online.flatMap((node) => node.status?.acks ?? []).filter((ack) => ack.age_seconds > 60);
  const configuredNames = new Set(nodes.map((node) => node.name));
  const dynamicAcks = (replication?.acks ?? []).filter((ack) => !configuredNames.has(ack.node));
  const connectedDynamic = dynamicAcks.filter((ack) => ack.age_seconds <= 60).length;
  const metric = (node: ClusterStatus['nodes'][number], key: string): number =>
    Number(node.metrics?.[key] ?? 0);
  const progressPercent =
    progress?.total && progress.total > 0
      ? Math.min(100, Math.round(((progress.loaded ?? 0) / progress.total) * 100))
      : 0;
  const progressAge = progress?.updated_at_ms
    ? Math.max(0, (Date.now() - progress.updated_at_ms) / 1000)
    : null;
  const progressStale = progressAge !== null && progressAge > 15;

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <h2>Cluster</h2>
          <p>
            One row per node. The primary accepts writes; replicas pull changes from
            their source and are read-only. Lag is how many replication transactions
            a node is behind.
          </p>
        </div>
        <div className="actions">
          <button onClick={() => void load()} disabled={loading}>{loading ? 'Refreshing…' : 'Refresh'}</button>
        </div>
      </header>
      {error && <div className="error-box">{error}</div>}
      {cluster?.message && <div className="panel"><p className="muted">{cluster.message}</p></div>}
      {progress && (
        <section className={`panel test-progress ${progress.active ? 'test-progress-live' : ''}`}>
          <div className="panel-title">
            <div>
              <h3>Live load test</h3>
              <span className="muted small">
                {progress.active
                  ? progressStale
                    ? `progress is ${progressAge?.toFixed(0)}s old — the test may be in a long phase`
                    : 'updating from the running benchmark'
                  : 'last benchmark finished'}
              </span>
            </div>
            <span className={`badge ${progress.active ? '' : 'danger-badge'}`}>
              {progress.active ? progress.phase : 'complete'}
            </span>
          </div>
          <div className="test-progress-line">
            <strong>{progress.message ?? progress.phase.replaceAll('_', ' ')}</strong>
            {progress.elapsed_seconds !== undefined && (
              <span className="muted small">{progress.elapsed_seconds.toFixed(1)}s elapsed</span>
            )}
          </div>
          {progress.total !== undefined && (
            <>
              <div className="progress-track" title="Documents loaded during this benchmark.">
                <span style={{ width: `${progressPercent}%` }} />
              </div>
              <div className="test-progress-meta">
                <span>{(progress.loaded ?? 0).toLocaleString()} / {progress.total.toLocaleString()} docs</span>
                <span>{progressPercent}%</span>
                {progress.docs_per_second !== undefined && (
                  <span>{progress.docs_per_second.toLocaleString()} docs/s</span>
                )}
                {progress.nodes_online !== undefined && <span>{progress.nodes_online} nodes online</span>}
              </div>
            </>
          )}
          <div className="test-progress-meta">
            {progress.written !== undefined && <span>{progress.written.toLocaleString()} writes</span>}
            {progress.new_primary && <span>primary: {progress.new_primary}</span>}
            {progress.epoch !== undefined && <span>epoch {progress.epoch}</span>}
            {progress.result_file && <span className="mono small">{progress.result_file}</span>}
          </div>
        </section>
      )}
      <div className="stat-grid">
        <RepStat label="Nodes" value={nodes.length} help="Configured nodes in the cluster view." />
        <RepStat label="Online" value={online.length} help="Nodes that answered the last probe." />
        <RepStat label="Primary" value={primaries.map((node) => node.name).join(', ') || 'none'} help="Node that currently accepts client writes." />
        <RepStat label="Max lag" value={maxLag.toLocaleString()} help="Largest replication-transaction gap across all replicas." danger={maxLag > 0} />
        <RepStat label="Replicas" value={replicas.length} help="Read-only nodes that pull changes from a primary." />
        <RepStat label="Unhealthy" value={nodes.length - online.length} help="Nodes that are offline or failed the probe." danger={nodes.length > online.length} />
        <RepStat label="Stale acks" value={staleAcks.length} help="Replica acknowledgements older than 60 seconds." danger={staleAcks.length > 0} />
        <RepStat label="Dynamic replicas" value={dynamicAcks.length} help="Replicas that acknowledged the primary but are not in the configured --cluster probe list." />
        <RepStat label="Updated" value={cluster?.generated_at_ms ? new Date(cluster.generated_at_ms).toLocaleTimeString() : '—'} help="Time of the last cluster probe." />
      </div>
      <div className="cluster-grid">
        {nodes.map((node) => {
          const status = node.status;
          const applied = status?.applied ?? [];
          const lag = Math.max(0, ...applied.map((item) => item.lag ?? 0));
          const acks = status?.acks ?? [];
          const nodeError =
            status?.role === 'primary'
              ? { transport: null, apply: null }
              : activeReplicationError(status?.replication);
          return (
            <section key={`${node.name}:${node.url}`} className={`panel cluster-card ${node.ok ? '' : 'cluster-down'}`}>
              <div className="panel-title">
                <div>
                  <h3>{node.name}</h3>
                  <span className="mono small">{node.url}</span>
                </div>
                <span className={`badge ${status?.role === 'primary' ? '' : 'danger-badge'}`}>
                  {node.ok ? status?.role ?? 'unknown' : 'offline'}
                </span>
              </div>
              {!node.ok ? (
                <div className="error-box">{node.error ?? 'node unreachable'}</div>
              ) : (
                <>
                  <div className="stat-grid cluster-stats">
                    <RepStat label="Epoch" value={status?.epoch ?? 0} help="Fencing generation. A higher epoch wins and rejects the old primary." />
                    <RepStat label="Head" value={(status?.head ?? 0).toLocaleString()} help="Last replication transaction stored on this node." />
                    <RepStat label="Lag" value={lag.toLocaleString()} help="Transactions this node still has to apply from its source." danger={lag > 0} />
                    <RepStat label="Acks" value={acks.length} help="Replicas that have acknowledged this node as their source." />
                    <RepStat label="Applied" value={metric(node, 'replication_applied_total').toLocaleString()} help="Replication transactions applied since the daemon started." />
                    <RepStat label="Transport errors (total)" value={metric(node, 'replication_transport_errors_total').toLocaleString()} help="Cumulative source connection or HTTP errors since startup; historical failover errors can remain." />
                    <RepStat label="Write ops" value={metric(node, 'write_ops_total').toLocaleString()} help="Documents written locally since startup." />
                    <RepStat label="Avg commit" value={`${metric(node, 'avg_commit_us').toFixed(1)} µs`} help="Average LMDB commit time for local writes." />
                  </div>
                  <div className="cluster-detail">
                    <span>Source</span>
                    <strong className="mono">
                      {status?.role === 'primary'
                        ? 'not used while primary'
                        : status?.replication?.source ?? '—'}
                    </strong>
                  </div>
                  <div className="cluster-detail"><span>Write URL</span><strong className="mono">{status?.write_url ?? '—'}</strong></div>
                  <div className="cluster-detail"><span>Mode</span><strong>{status?.read_only ? 'read-only' : 'writable'} · interval {status?.replication?.interval_secs ?? 0}s · {status?.replication?.bootstrap || 'no bootstrap'}</strong></div>
                  <div className="cluster-detail"><span>Databases</span><strong className="mono">{(status?.replication?.databases ?? []).join(', ') || '—'}</strong></div>
                  <div className="cluster-detail"><span>Requests</span><strong>{metric(node, 'requests_total').toLocaleString()}</strong></div>
                  <div className="cluster-detail"><span>Reads</span><strong>{metric(node, 'read_ops_total').toLocaleString()}</strong></div>
                  <div className="cluster-detail"><span>Errors</span><strong>{metric(node, 'errors_total').toLocaleString()}</strong></div>
                  <div className="cluster-detail"><span>Disk used</span><strong>{(Number(node.metrics?.disk?.data?.used_ratio ?? 0) * 100).toFixed(1)}%</strong></div>
                  {nodeError.transport && <div className="error-box">transport: {nodeError.transport}</div>}
                  {nodeError.apply && <div className="error-box">apply: {nodeError.apply}</div>}
                </>
              )}
            </section>
          );
        })}
      </div>
      {dynamicAcks.length > 0 && (
        <section className="panel">
          <div className="panel-title">
            <h3>Dynamic replica connections</h3>
            <span>{connectedDynamic} connected / {dynamicAcks.length} total</span>
          </div>
          <p className="field-hint">
            Replicas that acknowledged the primary but are not in this GUI's
            <code> --cluster</code> probe list. They are connected and synchronized
            through replication; their URL is not known here, so only ACK data is
            shown. Add them to <code>--cluster</code> to probe metrics and disk.
          </p>
          <div className="index-list">
            {dynamicAcks.map((ack) => (
              <div key={ack.node} className="index-row">
                <div>
                  <strong className="mono">{ack.node}</strong>
                  <span className="badge">seq {ack.seq.toLocaleString()}</span>
                  {ack.age_seconds <= 60 && <span className="badge">connected</span>}
                  {ack.url && <span className="muted small mono"> · {ack.url}</span>}
                </div>
                <span className={`muted small ${ack.age_seconds > 60 ? 'danger-text' : ''}`}>
                  {ack.age_seconds.toFixed(1)}s ago
                </span>
                <button className="danger" onClick={() => void forgetClusterAck(ack.node)}>
                  Remove replica
                </button>
              </div>
            ))}
          </div>
        </section>
      )}
      {nodes.length === 0 && (
        <div className="panel">
          <p className="muted">No nodes configured. Start the GUI with <code>--cluster name=url,name=url</code> or set <code>NONODB_GUI_CLUSTER</code>.</p>
        </div>
      )}
    </div>
  );
}

function activeReplicationError(
  replication: ReplicationStatus['replication'] | undefined,
): { transport: string | null; apply: string | null } {
  if (!replication) return { transport: null, apply: null };
  const errorIsNewer =
    (replication.last_error_unix ?? 0) > (replication.last_success_unix ?? 0);
  if (!errorIsNewer) return { transport: null, apply: null };
  return {
    transport: replication.last_transport_error ?? null,
    apply: replication.last_apply_error ?? null,
  };
}

function ReplicationPage({ connection }: { connection: Connection }) {
  const [status, setStatus] = useState<ReplicationStatus | null>(null);
  const [changes, setChanges] = useState<ReplicationChangeTxn[]>([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [sourceDraft, setSourceDraft] = useState('');
  const [databasesDraft, setDatabasesDraft] = useState('default');
  const [intervalDraft, setIntervalDraft] = useState('1');
  const [bootstrapDraft, setBootstrapDraft] = useState('auto');
  const [savingSource, setSavingSource] = useState(false);
  const [epochDraft, setEpochDraft] = useState('0');
  const [successorDraft, setSuccessorDraft] = useState('');
  const [changingRole, setChangingRole] = useState(false);
  const [pairName, setPairName] = useState('');
  const [pairDatabases, setPairDatabases] = useState('default');
  const [pairing, setPairing] = useState<PairingView | null>(null);
  const [pendingPairings, setPendingPairings] = useState<PairingView[]>([]);
  const [pairingBusy, setPairingBusy] = useState(false);
  const [replicationBusy, setReplicationBusy] = useState(false);
  const [replicationNotice, setReplicationNotice] = useState('');

  const load = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const [nextStatus, nextChanges, pendingState] = await Promise.all([
        replicationStatus(connection),
        replicationChanges(connection, 0, 50),
        listReplicaPairings(connection)
          .then((value) => ({ ok: true, pending: value.pending ?? [] }))
          .catch(() => ({ ok: false, pending: [] as PairingView[] })),
      ]);
      setStatus(nextStatus);
      setChanges(nextChanges.changes);
      const pendingItems = pendingState.pending;
      if (pendingState.ok) {
        setPendingPairings(pendingItems);
        setPairing((current) => {
          if (!current) return null;
          if (Date.now() >= current.expires_at_ms) return null;
          return pendingItems.some((item) => item.code === current.code) ? current : null;
        });
      }
      setSourceDraft(nextStatus.replication?.source ?? '');
      setDatabasesDraft((nextStatus.replication?.databases ?? ['default']).join(', '));
      setIntervalDraft(String(nextStatus.replication?.interval_secs ?? 1));
      setBootstrapDraft(nextStatus.replication?.bootstrap ?? 'auto');
      setEpochDraft(String(nextStatus.epoch ?? 0));
    } catch (err) {
      setError(errorText(err));
    } finally {
      setLoading(false);
    }
  }, [connection]);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => {
      void load();
    }, 3000);
    return () => window.clearInterval(timer);
  }, [load]);

  const forgetAck = async (node: string) => {
    if (!window.confirm(
      `Remove replica ${node} from the primary view?\n\n` +
      'This deletes its last ACK on the primary. If the replica is still running, it will reappear after its next ACK; pause or disconnect it first.',
    )) return;
    setReplicationBusy(true);
    setError('');
    setReplicationNotice('');
    try {
      await forgetReplicaAck(connection, node);
      setReplicationNotice(`Forgot replica ${node}.`);
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setReplicationBusy(false);
    }
  };

  const pauseOrResume = async (pause: boolean) => {
    setReplicationBusy(true);
    setError('');
    setReplicationNotice('');
    try {
      if (pause) await pauseReplication(connection);
      else await resumeReplication(connection);
      setReplicationNotice(pause ? 'Replication paused.' : 'Replication resumed.');
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setReplicationBusy(false);
    }
  };

  const disconnect = async () => {
    if (!window.confirm(
      'Disconnect this node from its replication source?\n\n' +
      'The source and token are removed, pulling stops, and the node stays read-only until promoted.',
    )) return;
    setReplicationBusy(true);
    setError('');
    setReplicationNotice('');
    try {
      await disconnectReplication(connection);
      setReplicationNotice('Disconnected from the replication source. The node remains read-only.');
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setReplicationBusy(false);
    }
  };

  const changeRole = async (action: 'promote' | 'demote') => {
    const label = action === 'promote' ? 'Promote this node to primary?' : 'Demote this node to replica?';
    if (!window.confirm(`${label}\n\nExpected epoch: ${epochDraft}`)) return;
    setChangingRole(true);
    setError('');
    try {
      if (action === 'promote') {
        await promoteReplication(connection, Number(epochDraft) || 0);
      } else {
        await demoteReplication(connection, Number(epochDraft) || 0, 'replica', successorDraft);
      }
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setChangingRole(false);
    }
  };

  const saveSource = async () => {
    setSavingSource(true);
    setError('');
    try {
      await updateReplicationSource(
        connection,
        sourceDraft,
        databasesDraft.split(',').map((item) => item.trim()).filter(Boolean),
        Math.max(1, Number(intervalDraft) || 1),
        bootstrapDraft,
      );
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setSavingSource(false);
    }
  };

  const createPairing = async () => {
    setPairingBusy(true);
    setError('');
    try {
      const result = await createReplicaPairing(
        connection,
        pairName.trim(),
        pairDatabases.split(',').map((item) => item.trim()).filter(Boolean),
      );
      setPairing(result);
      setPendingPairings((current) => [...current, result]);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setPairingBusy(false);
    }
  };

  const revokePairing = async (code: string) => {
    setPairingBusy(true);
    setError('');
    try {
      await revokeReplicaPairing(connection, code);
      setPendingPairings((current) => current.filter((item) => item.code !== code));
      if (pairing?.code === code) setPairing(null);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setPairingBusy(false);
    }
  };

  const replicationError =
    status?.role === 'primary'
      ? { transport: null, apply: null }
      : activeReplicationError(status?.replication);

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <h2>Replication</h2>
          <p>
            State of the node you are connected to. A primary accepts writes; a
            replica is read-only and pulls committed transactions from its source.
            Open <strong>Cluster</strong> to see every node at once.
          </p>
        </div>
        <div className="actions">
          <button onClick={() => void load()} disabled={loading}>{loading ? 'Loading…' : 'Refresh'}</button>
        </div>
      </header>
      {error && <div className="error-box">{error}</div>}
      <section className="panel">
        <div className="panel-title"><h3>How to read this page</h3><span>glossary</span></div>
        <div className="replication-glossary">
          <div><strong>Primary</strong><span>Accepts client writes and appends them to the replication log.</span></div>
          <div><strong>Replica</strong><span>Read-only node that pulls committed transactions from a primary.</span></div>
          <div><strong>Source</strong><span>The node this replica fetches changes from.</span></div>
          <div><strong>Epoch</strong><span>Fencing generation. A newer primary has a higher epoch; stale writers are rejected.</span></div>
          <div><strong>Head sequence</strong><span>Last replication transaction number on this node, not a document count.</span></div>
          <div><strong>Applied / lag</strong><span>How many transactions were applied per origin, and how many are still missing.</span></div>
          <div><strong>Ack</strong><span>A replica's confirmation that it applied changes up to a sequence number.</span></div>
          <div><strong>Bootstrap</strong><span>Initial snapshot copy used when a new replica joins or is too far behind.</span></div>
          <div><strong>Write URL</strong><span>Endpoint clients should use for writes: the primary or its announced successor.</span></div>
          <div><strong>Origin</strong><span>Stable node identity stored in every replicated transaction.</span></div>
        </div>
      </section>
      <section className="panel">
        <div className="panel-title">
          <h3>Node status</h3>
          {status && <span className={`badge ${status.role === 'primary' ? '' : 'danger-badge'}`}>{status.role}</span>}
        </div>
        {status ? (
          <div className="stat-grid">
            <RepStat label="Database" value={status.database} help="Database whose replication log is shown on this page." />
            <RepStat label="Log capture" value={status.enabled ? 'enabled' : 'disabled'} help="Whether committed writes are recorded in the durable replication log." />
            <RepStat label="Origin" value={<span className="mono">{status.origin ?? '—'}</span>} help="Stable node identity written into each replication transaction." />
            <RepStat label="Epoch" value={status.epoch ?? status.replication?.epoch ?? 0} help="Fencing generation. Higher epochs reject writes from older primaries." />
            <RepStat label="Write URL" value={<span className="mono">{status.write_url ?? '—'}</span>} help="Where clients should send writes. On a replica this points at the primary or successor." />
            <RepStat label="Head sequence" value={status.head.toLocaleString()} help="Last replication transaction number on this node. It is not the document count." />
          </div>
        ) : (
          <p className="muted">No status available.</p>
        )}
        {status && !status.enabled && (
          <p className="field-hint">
            Set <code>NONODB_REPLICATION_ENABLED=1</code> and
            <code> NONODB_REPLICATION_ORIGIN</code> on the daemon to capture changes.
          </p>
        )}
      </section>
      {status?.role === 'primary' && (
        <section className="panel">
          <div className="panel-title">
            <h3>Add replica</h3>
            <span>one-time pairing</span>
          </div>
          <p className="field-hint">
            Creates a short-lived 8-character code. Run
            <code> nonodb-cli replication join {status.write_url ?? "<primary-api-url>"} &lt;pairing-code&gt; </code>
            on the new node and enter the code there. Use the daemon API URL from
            <strong> Write URL</strong> (default port 7878, not the GUI proxy port 8787).
            The replica receives the cluster auth registry and backup configuration
            automatically; they stay dormant until this node is promoted.
          </p>
          <details className="docker-help" open>
            <summary>Run the new replica in Docker</summary>
            <ol>
              <li>Create the pairing code above and keep the one-time code.</li>
              <li>
                Prepare a writable auth file in the container's data volume. The replica
                receives the primary's auth registry after joining, but TCP requires
                <code> NONODB_AUTH_FILE</code> at startup.
              </li>
              <li>
                Publish the replica API on a host port and set
                <code> NONODB_REPLICATION_ADVERTISE_URL</code> to that host address.
                Do not advertise the container IP (<code>172.x</code>): the primary
                cannot reach it. Docker NAT also prevents the automatic LAN fallback.
              </li>
              <li>
                Run the join command inside the container. The daemon must already be
                running; the CLI talks to its Unix socket, not to the primary directly.
              </li>
            </ol>
            <pre className="docker-command">{`docker volume create nonodb-replica-data
docker volume create nonodb-replica-run

docker run --rm \
  -v nonodb-replica-data:/var/lib/nonodb \
  -e NONODB_AUTH_FILE=/var/lib/nonodb/auth.json \
  ghcr.io/essmos/nonodb:0.30.0 \
  nonodb-cli auth init

docker run -d --name nonodb-replica \
  -p 7912:7878 \
  -v nonodb-replica-data:/var/lib/nonodb \
  -v nonodb-replica-run:/run/nonodb \
  -e NONODB_AUTH_FILE=/var/lib/nonodb/auth.json \
  -e NONODB_TCP=0.0.0.0:7878 \
  -e NONODB_REPLICATION_ROLE=replica \
  -e NONODB_REPLICATION_NODE_ID=replica-docker \
  -e NONODB_REPLICATION_ADVERTISE_URL=http://DOCKER_HOST_IP:7912 \
  ghcr.io/essmos/nonodb:0.30.0

docker exec nonodb-replica nonodb-cli replication join \
  http://PRIMARY_HOST:7878 PAIRING-CODE

docker exec nonodb-replica nonodb-cli replication status`}</pre>
            <p className="field-hint">
              Replace <code>DOCKER_HOST_IP</code> with the Docker host's LAN address,
              <code> PRIMARY_HOST</code> with the primary's API host (not the GUI proxy),
              and <code>PAIRING-CODE</code> with the code above. The primary GUI adds the
              replica after its first acknowledgement and probes it for metrics.
            </p>
          </details>
          <div className="form-grid">
            <label>Replica name<input value={pairName} onChange={(e) => setPairName(e.target.value)} placeholder="replica-eu" />
              <span className="field-hint">Used for the replicated principal name and audit records.</span>
            </label>
            <label>Databases<input value={pairDatabases} onChange={(e) => setPairDatabases(e.target.value)} placeholder="default, app1" />
              <span className="field-hint">Comma-separated databases the new replica should pull initially.</span>
            </label>
          </div>
          <div className="actions">
            <button onClick={() => void createPairing()} disabled={pairingBusy || !pairName.trim()}>
              {pairingBusy ? 'Working…' : 'Create pairing code'}
            </button>
          </div>
          {pairing && (
            <div className="pairing-box">
              <div className="pairing-code">{pairing.code}</div>
              <p className="field-hint">
                Expires {new Date(pairing.expires_at_ms).toLocaleTimeString()}.
                The code is single-use. The replica receives a cluster credential and
                the full control-plane snapshot after joining.
              </p>
              <code>nonodb-cli replication join {status.write_url ?? "<primary-api-url>"} {pairing.code}</code>
            </div>
          )}
          {pendingPairings.length > 0 && (
            <>
              <div className="panel-title"><h3>Pending pairings</h3><span>{pendingPairings.length}</span></div>
              <div className="index-list">
                {pendingPairings.map((item) => (
                  <div className="index-row" key={item.code}>
                    <div><strong className="mono">{item.code}</strong><span className="muted"> · {item.name} · {(item.databases ?? []).join(', ')}</span></div>
                    <button className="danger" disabled={pairingBusy} onClick={() => void revokePairing(item.code)}>Revoke</button>
                  </div>
                ))}
              </div>
            </>
          )}
        </section>
      )}
      {replicationNotice && <div className="notice-box">{replicationNotice}</div>}
      {status && status.role !== 'primary' && (
        <section className="panel">
          <div className="panel-title">
            <h3>Replication control</h3>
            <span>{status.replication?.paused ? 'paused' : 'running'}</span>
          </div>
          <p className="field-hint">
            Pause stops applying new transactions but keeps the source for resume.
            Disconnect removes the source and token and leaves this node read-only.
            Neither action promotes the node or deletes local data.
          </p>
          <div className="actions">
            <button
              onClick={() => void pauseOrResume(!status.replication?.paused)}
              disabled={replicationBusy || !status.replication?.source}
            >
              {status.replication?.paused ? 'Resume replication' : 'Pause replication'}
            </button>
            <button className="danger" onClick={() => void disconnect()} disabled={replicationBusy || !status.replication?.source}>
              Disconnect from source
            </button>
          </div>
        </section>
      )}
      {status?.replication && (
        <section className="panel">
          <div className="panel-title">
            <h3>Transport health</h3>
            {status.role === 'primary' && <span>not used while primary</span>}
          </div>
          {status.role === 'primary' ? (
            <p className="field-hint">
              This node is the primary and accepts client writes, so it does not pull
              from a source. Source settings below are kept for if this node is demoted.
            </p>
          ) : (
            <>
          <div className="stat-grid">
            <RepStat label="Source" value={<span className="mono">{status.replication.source ?? '—'}</span>} help="Primary URL this node pulls replication changes from." />
            <RepStat label="Bootstrap" value={status.replication.bootstrap ?? '—'} help="Snapshot policy: auto copies a snapshot when needed, always forces it, never pulls only the log." />
            <RepStat label="Poll interval" value={`${status.replication.interval_secs ?? 0}s`} help="How often this node asks the source for new transactions." />
            <RepStat label="Mode" value={status.read_only ? 'read-only' : 'writable'} help="Only the primary is writable. Replicas and standbys reject client writes." />
            <RepStat label="Last success" value={status.replication.last_success_unix ? new Date(status.replication.last_success_unix * 1000).toLocaleString() : '—'} help="Last successful pull/apply/ack cycle. Errors older than this are hidden." />
            <RepStat label="Databases" value={<span className="mono">{(status.replication.databases ?? []).join(', ') || '—'}</span>} help="Databases included in this replication source configuration." />
          </div>
          {replicationError.transport && (
            <div className="error-box">transport: {replicationError.transport}</div>
          )}
          {replicationError.apply && (
            <div className="error-box">apply: {replicationError.apply}</div>
          )}
            </>
          )}
          <div className="panel-title"><h3>Source settings</h3></div>
          <p className="field-hint">
            These settings tell a replica where to pull from. Saving them updates the
            source without restarting the daemon. Use only on a replica/standby.
          </p>
          <div className="form-grid">
            <label>Source URL<input value={sourceDraft} onChange={(e) => setSourceDraft(e.target.value)} placeholder="https://primary:7878" />
              <span className="field-hint">Primary base URL. Replicas call its replication endpoints.</span>
            </label>
            <label>Databases<input value={databasesDraft} onChange={(e) => setDatabasesDraft(e.target.value)} placeholder="default, app1" />
              <span className="field-hint">Comma-separated databases to replicate. Empty means the default database.</span>
            </label>
            <label>Poll interval (s)<input type="number" min="1" value={intervalDraft} onChange={(e) => setIntervalDraft(e.target.value)} />
              <span className="field-hint">Seconds between pulls. Lower means lower lag but more requests.</span>
            </label>
            <label>Bootstrap<select value={bootstrapDraft} onChange={(e) => setBootstrapDraft(e.target.value)}>
              <option value="auto">auto</option>
              <option value="always">always</option>
              <option value="never">never</option>
            </select>
              <span className="field-hint">auto: snapshot only when needed. always: replace local data from a snapshot. never: log only.</span>
            </label>
          </div>
          <div className="actions">
            <button onClick={() => void saveSource()} disabled={savingSource || !sourceDraft.trim()}>
              {savingSource ? 'Saving…' : 'Save source'}
            </button>
          </div>
          <div className="panel-title"><h3>Role control</h3><span>epoch {status.epoch ?? 0}</span></div>
          <p className="field-hint">
            Promote makes this node writable and increments the epoch. Demote makes it
            read-only. Manual role changes should only happen during controlled failover.
          </p>
          <div className="form-grid">
            <label>Expected epoch<input type="number" min="0" value={epochDraft} onChange={(e) => setEpochDraft(e.target.value)} />
              <span className="field-hint">Safety check. Must equal the current epoch shown above; prevents acting on stale state.</span>
            </label>
            <label>Successor URL<input value={successorDraft} onChange={(e) => setSuccessorDraft(e.target.value)} placeholder="https://new-primary:7878" />
              <span className="field-hint">Optional address advertised to clients after this primary is demoted.</span>
            </label>
          </div>
          <div className="actions">
            <button onClick={() => void changeRole('promote')} disabled={changingRole || status.role === 'primary'}>Promote to primary</button>
            <button className="danger" onClick={() => void changeRole('demote')} disabled={changingRole || status.role !== 'primary'}>Demote to replica</button>
          </div>
          <p className="field-hint">Quorum promotion still requires a voter majority. Demote or stop the old primary to avoid split-brain.</p>
        </section>
      )}
      {status && (
        <section className="panel">
          <div className="panel-title">
            <h3>Replica connections</h3>
            <span>
              {(status.acks ?? []).filter((ack) => ack.age_seconds <= 60).length} connected
              {" / "}
              {(status.acks ?? []).length} total
            </span>
          </div>
          <p className="field-hint">
            Each replica periodically reports the last sequence it applied. The primary
            uses these acks for safe pruning. A fresh acknowledgement means the replica is
            connected; an age above 60 seconds is considered stale. Newly joined replicas
            appear here after their first successful pull.
          </p>
          {status.acks && status.acks.length > 0 ? (
            <div className="index-list">
              {status.acks.map((ack) => (
                <div key={ack.node} className="index-row" title="Replica identity, applied sequence, and time since its last acknowledgement.">
                  <div>
                    <strong className="mono">{ack.node}</strong>
                    <span className="badge">seq {ack.seq.toLocaleString()}</span>
                    {ack.age_seconds <= 60 && <span className="badge">connected</span>}
                    {ack.url && <span className="muted small mono"> · {ack.url}</span>}
                  </div>
                  <span className={`muted small ${ack.age_seconds > 60 ? 'danger-text' : ''}`}>
                    {ack.age_seconds.toFixed(1)}s ago
                  </span>
                  <button
                    className="danger"
                    disabled={replicationBusy}
                    onClick={() => void forgetAck(ack.node)}
                  >
                    Remove replica
                  </button>
                </div>
              ))}
            </div>
          ) : (
            <p className="muted">No replica acknowledgements recorded on this node yet. Waiting for the first replica pull…</p>
          )}
        </section>
      )}
      {status && (
        <section className="panel">
          <div className="panel-title"><h3>Applied sources</h3><span>{status.applied?.length ?? 0}</span></div>
          <p className="field-hint">
            Each origin is a node that wrote transactions. Applied is how many of that
            origin's transactions this node has stored; source head is the origin's latest
            sequence. Lag above zero means this node is still behind.
          </p>
          {status.applied && status.applied.length > 0 ? (
            <div className="index-list">
              {status.applied.map((source) => (
                <div key={source.origin} className="index-row" title="Origin, role epoch, applied position, source head, and remaining lag.">
                  <div>
                    <strong className="mono">{source.origin}</strong>
                    <span className="badge">epoch {source.epoch ?? 0}</span>
                    <span className={`badge ${source.lag > 0 ? 'danger-badge' : ''}`}>
                      lag {source.lag}
                    </span>
                  </div>
                  <span className="muted small">
                    applied {source.applied.toLocaleString()} / head {source.source_head.toLocaleString()}
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <p className="muted">No applied source history on this node yet.</p>
          )}
        </section>
      )}
      <section className="panel">
        <div className="panel-title"><h3>Recent replication transactions</h3><span>{changes.length}</span></div>
        <p className="field-hint">
          One row is one committed write transaction. seq is the global replication order,
          origin is the node that produced it, and the last line lists each collection and
          document changed by that transaction.
        </p>
        {changes.length === 0 ? (
          <p className="muted">No replicated transactions captured.</p>
        ) : (
          <div className="index-list">
            {changes.slice().reverse().map((txn) => (
              <div key={txn.seq} className="index-row replication-row" title="A single committed write transaction in replication order.">
                <div>
                  <strong>seq {txn.seq}</strong>
                  <span className="badge">{txn.origin}</span>
                  <div className="muted small">{new Date(txn.created_at_ms).toLocaleString()}</div>
                  <div className="muted small">
                    {txn.changes.map((change, index) => (
                      <span key={`${txn.seq}:${index}`} className="replication-change">
                        {change.op} {change.collection}/{change.id}
                      </span>
                    ))}
                  </div>
                </div>
                <span className="muted small">{txn.changes.length} change{txn.changes.length === 1 ? '' : 's'}</span>
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

function StorageView({
  connection,
  onOpenCollection,
}: {
  connection: Connection;
  onOpenCollection: (name: string) => void;
}) {
  const [stats, setStats] = useState<DatabaseStorageStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const load = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      setStats(await databaseStats(connection));
    } catch (err) {
      setError(errorText(err));
    } finally {
      setLoading(false);
    }
  }, [connection]);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <h2>Storage</h2>
          <p>Record counts and estimated space usage for {connection.database || 'default'}</p>
        </div>
        <div className="actions">
          <button onClick={() => void load()} disabled={loading}>
            {loading ? 'Loading…' : 'Refresh'}
          </button>
        </div>
      </header>
      {error && <div className="error-box">{error}</div>}
      {loading && !stats && <p className="muted">Reading LMDB storage statistics…</p>}
      {stats && (
        <>
          <div className="stat-grid">
            <div className="stat"><span>Collections</span><strong>{stats.collections.length}</strong></div>
            <div className="stat"><span>Records</span><strong>{stats.total_records.toLocaleString()}</strong></div>
            <div className="stat"><span>Collections size</span><strong>{formatBytes(stats.collection_bytes)}</strong></div>
            <div className="stat"><span>LMDB file used</span><strong>{formatBytes(stats.used_bytes)}</strong></div>
          </div>
          <section className="panel">
            <div className="panel-title">
              <h3>Collections</h3>
              <span>{stats.collections.length}</span>
            </div>
            <p className="muted small">
              Data and index sizes use LMDB page allocation. History/TTL are estimated from
              shared entries. The file high-water mark can be larger because LMDB keeps freed pages.
            </p>
            {stats.collections.length === 0 ? (
              <p className="muted">No collections in this database.</p>
            ) : (
              <div className="metric-table-wrap">
                <table className="metric-table storage-table">
                  <thead>
                    <tr>
                      <th>Collection</th>
                      <th>Records</th>
                      <th>Data</th>
                      <th>Indexes</th>
                      <th>History</th>
                      <th>TTL</th>
                      <th>Total</th>
                      <th>Traffic/min</th>
                      <th>Avg/s</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stats.collections.map((item) => (
                      <tr
                        key={item.collection}
                        className="clickable-row"
                        tabIndex={0}
                        title={`Open metrics for ${item.collection}`}
                        onClick={() => onOpenCollection(item.collection)}
                        onKeyDown={(event) => {
                          if (event.key === 'Enter' || event.key === ' ') {
                            event.preventDefault();
                            onOpenCollection(item.collection);
                          }
                        }}
                      >
                        <td className="mono">{item.collection}</td>
                        <td>{item.records.toLocaleString()}</td>
                        <td>{formatBytes(item.data_bytes)}</td>
                        <td>{formatBytes(item.index_bytes)} <span className="muted small">({item.indexes})</span></td>
                        <td>{formatBytes(item.history_bytes)}</td>
                        <td>{formatBytes(item.ttl_bytes)}</td>
                        <td><strong>{formatBytes(item.total_bytes)}</strong></td>
                        <td>{item.requests_last_minute.toLocaleString()}</td>
                        <td>{item.avg_requests_per_second.toFixed(2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        </>
      )}
    </div>
  );
}

function NetworkView({ connection }: { connection: Connection }) {
  const [data, setData] = useState<NetworkInfo | null>(null);
  const [allowedText, setAllowedText] = useState('');
  const [portsText, setPortsText] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');

  const applyData = useCallback((next: NetworkInfo) => {
    setData(next);
    setAllowedText(next.allowed.join(", "));
    setPortsText(next.ports.join(", "));
  }, []);

  const load = useCallback(async () => {
    setBusy(true);
    setError('');
    try {
      applyData(await networkInfo(connection));
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  }, [applyData, connection]);

  useEffect(() => {
    void load();
  }, [load]);

  const parsed = () => {
    const allowed = allowedText
      .split(/[,\n]/)
      .map((value) => value.trim())
      .filter(Boolean);
    const ports = portsText
      .split(/[,\n]/)
      .map((value) => value.trim())
      .filter(Boolean)
      .map((value) => Number(value));
    if (ports.some((port) => !Number.isInteger(port) || port < 1 || port > 65535)) {
      throw new Error("Ports must be integers between 1 and 65535.");
    }
    return { allowed, ports };
  };

  const save = async () => {
    setBusy(true);
    setError('');
    setNotice('');
    try {
      const values = parsed();
      const next = await saveFirewall(connection, values.allowed, values.ports);
      applyData(next);
      setNotice("Saved firewall preview.");
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const apply = async () => {
    setBusy(true);
    setError('');
    setNotice('');
    try {
      const values = parsed();
      await saveFirewall(connection, values.allowed, values.ports);
      applyData(await applyFirewall(connection));
      setNotice("Firewall rules applied.");
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const disable = async () => {
    setBusy(true);
    setError('');
    setNotice('');
    try {
      applyData(await disableFirewall(connection));
      setNotice("Managed firewall table removed.");
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <h2>Network</h2>
          <p>Listener addresses and host firewall for NonoDB ports</p>
        </div>
        <div className="actions">
          <button onClick={() => void load()} disabled={busy}>Refresh</button>
        </div>
      </header>
      {error && <div className="error-box">{error}</div>}
      {notice && <div className="notice-box">{notice}</div>}

      <section className="panel">
        <div className="panel-title"><h3>Listeners</h3><span>actual bind</span></div>
        {data ? (
          <>
            <div className="cluster-detail"><span>TCP</span><strong className="mono">{data.listeners.tcp?.bound ?? "disabled"}{data.listeners.tcp?.tls ? " · TLS" : ""}</strong></div>
            <div className="cluster-detail"><span>Configured</span><strong className="mono">{data.listeners.tcp?.configured ?? "—"}</strong></div>
            <div className="cluster-detail"><span>Unix socket</span><strong className="mono">{data.listeners.unix ?? "disabled"}</strong></div>
            <div className="cluster-detail"><span>Advertise URL</span><strong className="mono">{data.listeners.advertise_url ?? "—"}</strong></div>
            {data.listeners.tcp?.bound?.startsWith("127.") && (
              <div className="error-box">TCP is loopback-only. Remote replicas cannot connect until the daemon listens on 0.0.0.0 or a LAN address.</div>
            )}
          </>
        ) : <p className="muted">Loading listener details…</p>}
      </section>

      <section className="panel">
        <div className="panel-title"><h3>Host firewall</h3><span>nftables</span></div>
        <p className="field-hint">
          Creates a dedicated <code>{data?.firewall.table ?? "inet nonodb_firewall"}</code> table and
          restricts only the listed NonoDB TCP ports. SSH, the GUI port, and unrelated host
          services are not touched. Loopback is always allowed.
        </p>
        {data && (
          <>
            <div className="cluster-detail"><span>Backend</span><strong>{data.firewall.backend} · {data.firewall.available ? "available" : "unavailable"}</strong></div>
            <div className="cluster-detail"><span>Apply</span><strong>{data.firewall.apply_enabled ? "enabled" : "disabled (preview only)"}</strong></div>
            <div className="cluster-detail"><span>Managed table</span><strong>{data.firewall.managed ? "active" : "not active"}</strong></div>
            {!data.firewall.apply_enabled && (
              <div className="error-box">
                Apply is disabled. Set <code>NONODB_FIREWALL_MANAGE=1</code> on the daemon and restart it.
                The rules below are still generated for review.
              </div>
            )}
            {data.firewall.error && <div className="error-box">{data.firewall.error}</div>}
          </>
        )}
        <div className="form-grid">
          <label>Allowed addresses
            <textarea
              value={allowedText}
              onChange={(event) => setAllowedText(event.target.value)}
              placeholder={"10.0.0.0/24\n192.168.1.20"}
              rows={4}
            />
            <span className="field-hint">Comma- or newline-separated IPv4/IPv6 addresses or CIDRs. Empty means loopback only.</span>
          </label>
          <label>TCP ports
            <input value={portsText} onChange={(event) => setPortsText(event.target.value)} placeholder="7902" />
            <span className="field-hint">Ports used by NonoDB API/replication. Do not add SSH or the GUI port.</span>
          </label>
        </div>
        <div className="actions">
          <button onClick={() => void save()} disabled={busy}>Save &amp; preview</button>
          <button className="primary" onClick={() => void apply()} disabled={busy || !data?.firewall.apply_enabled}>Apply firewall</button>
          <button className="danger" onClick={() => void disable()} disabled={busy || !data?.firewall.available}>Disable managed firewall</button>
        </div>
        <div className="panel-title"><h3>Generated nftables rules</h3><span>review before apply</span></div>
        <pre className="result">{data?.preview ?? ""}</pre>
        {data?.firewall.status && (
          <details><summary>Current managed table</summary><pre className="result">{data.firewall.status}</pre></details>
        )}
      </section>
    </div>
  );
}

function Dashboard({
  connection,
  collections,
  onRefresh,
}: {
  connection: Connection;
  collections: string[];
  onRefresh: () => void;
}) {
  const [info, setInfo] = useState<MapInfo | null>(null);
  const [healthValue, setHealthValue] = useState<unknown>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    let cancelled = false;
    Promise.all([exec<MapInfo>(connection, { op: 'map_info' }), health(connection)])
      .then(([map, healthResult]) => {
        if (!cancelled) {
          setInfo(map);
          setHealthValue(healthResult);
        }
      })
      .catch((err) => !cancelled && setError(errorText(err)));
    return () => {
      cancelled = true;
    };
  }, [connection]);

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <h2>Dashboard</h2>
          <p>Database status and metrics</p>
        </div>
        <div className="actions">
          <button onClick={onRefresh}>Refresh</button>
        </div>
      </header>
      {error && <div className="error-box">{error}</div>}
      <div className="stat-grid">
        <div className="stat">
          <span>Collections</span>
          <strong>{collections.length}</strong>
        </div>
        <div className="stat">
          <span>Map used</span>
          <strong>{info ? formatBytes(info.used_bytes) : '-'}</strong>
        </div>
        <div className="stat">
          <span>Map size</span>
          <strong>{info ? formatBytes(info.map_size) : '-'}</strong>
        </div>
        <div className="stat">
          <span>Health</span>
          <strong>{healthValue ? 'OK' : '-'}</strong>
        </div>
      </div>
      <DatabaseMetrics connection={connection} />
    </div>
  );
}

function parseCsv(text: string): Doc[] {
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = '';
  let quoted = false;
  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    if (quoted) {
      if (char === '"' && text[i + 1] === '"') {
        cell += '"';
        i += 1;
      } else if (char === '"') {
        quoted = false;
      } else {
        cell += char;
      }
    } else if (char === '"') {
      quoted = true;
    } else if (char === ',') {
      row.push(cell);
      cell = '';
    } else if (char === '\n') {
      row.push(cell.replace(/\r$/, ''));
      rows.push(row);
      row = [];
      cell = '';
    } else {
      cell += char;
    }
  }
  if (cell.length > 0 || row.length > 0) {
    row.push(cell.replace(/\r$/, ''));
    rows.push(row);
  }
  if (rows.length < 2) return [];
  const headers = rows[0];
  const coerce = (value: string): unknown => {
    if (value === '') return '';
    if (value === 'true') return true;
    if (value === 'false') return false;
    if (value === 'null') return null;
    if (/^-?\d+(\.\d+)?$/.test(value)) return Number(value);
    if ((value.startsWith('{') && value.endsWith('}')) || (value.startsWith('[') && value.endsWith(']'))) {
      try {
        return JSON.parse(value);
      } catch {
        return value;
      }
    }
    return value;
  };
  return rows.slice(1).filter((values) => values.some((value) => value !== '')).map((values) => {
    const doc: Doc = {};
    headers.forEach((header, index) => {
      doc[header] = coerce(values[index] ?? '');
    });
    return doc;
  });
}

function buildSchema(docs: Doc[]): Array<{ path: string; types: string[]; coverage: number }> {
  const stats = new Map<string, { types: Set<string>; count: number }>();
  const walk = (value: unknown, path: string) => {
    const type = value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value;
    if (path) {
      const current = stats.get(path) ?? { types: new Set<string>(), count: 0 };
      current.types.add(type);
      current.count += 1;
      stats.set(path, current);
    }
    if (Array.isArray(value)) {
      for (const item of value) {
        if (item && typeof item === 'object' && !Array.isArray(item)) walk(item, path);
      }
    } else if (value && typeof value === 'object') {
      for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
        walk(child, path ? `${path}.${key}` : key);
      }
    }
  };
  for (const doc of docs) walk(doc, '');
  return [...stats.entries()]
    .map(([path, value]) => ({
      path,
      types: [...value.types].sort(),
      coverage: docs.length === 0 ? 0 : (value.count / docs.length) * 100,
    }))
    .sort((a, b) => a.path.localeCompare(b.path));
}

function CollectionView({
  connection,
  name,
  initialTab = 'documents',
  onOpenEventing,
}: {
  connection: Connection;
  name: string;
  initialTab?: CollectionTab;
  onOpenEventing?: () => void;
}) {
  const [tab, setTab] = useState<CollectionTab>(initialTab);
  const [docs, setDocs] = useState<Doc[]>([]);
  const [sampleDocs, setSampleDocs] = useState<Doc[]>([]);
  const [fieldPaths, setFieldPaths] = useState<string[]>([]);
  const [savedViews, setSavedViews] = useState<Array<Record<string, string | number>>>(() => {
    try {
      return JSON.parse(localStorage.getItem(`nonodb.gui.views.${name}`) ?? '[]') as Array<Record<string, string | number>>;
    } catch {
      return [];
    }
  });
  const [viewName, setViewName] = useState('');
  const [count, setCount] = useState<number | null>(null);
  const [filterText, setFilterText] = useState('{}');
  const [projectionText, setProjectionText] = useState('');
  const [limit, setLimit] = useState(50);
  const [skip, setSkip] = useState(0);
  const [sortField, setSortField] = useState('_id');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  const [selected, setSelected] = useState<Doc | null>(null);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [editor, setEditor] = useState('{}');
  const [newDoc, setNewDoc] = useState('{\n  "name": "example"\n}');
  const [indexes, setIndexes] = useState<IndexInfo[]>([]);
  const [textIndexes, setTextIndexes] = useState<string[]>([]);
  const [fuzzyIndexes, setFuzzyIndexes] = useState<string[]>([]);
  const [vectorIndexes, setVectorIndexes] = useState<Array<{ field: string; dims: number; metric?: string }>>([]);
  const [ttl, setTtl] = useState('');
  const [history, setHistory] = useState('');
  const [tsSettings, setTsSettings] = useState<TimeSeriesSettings | null>(null);
  const [tsStats, setTsStats] = useState<TimeSeriesStats | null>(null);
  const [tsEditCompression, setTsEditCompression] = useState('balanced');
  const [tsEditExpire, setTsEditExpire] = useState('');
  const [tsEditBucketDocs, setTsEditBucketDocs] = useState('');
  const [eventsEnabled, setEventsEnabled] = useState(false);
  const [indexKind, setIndexKind] = useState<'secondary' | 'text' | 'fuzzy' | 'vector'>('secondary');
  const [indexField, setIndexField] = useState('');
  const [indexUnique, setIndexUnique] = useState(false);
  const [indexInclude, setIndexInclude] = useState('');
  const [indexGeo, setIndexGeo] = useState(false);
  const [vectorDims, setVectorDims] = useState('3');
  const [vectorMetric, setVectorMetric] = useState('cosine');
  const [subscriptions, setSubscriptions] = useState<Array<Record<string, unknown>>>([]);
  const [subName, setSubName] = useState('');
  const [subFilter, setSubFilter] = useState('{}');
  const [subCount, setSubCount] = useState('');
  const [subOps, setSubOps] = useState('');
  const [subOnce, setSubOnce] = useState(false);
  const [subLease, setSubLease] = useState('600');
  const [searchMode, setSearchMode] = useState<'text' | 'fuzzy' | 'vector'>('text');
  const [searchField, setSearchField] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [searchVector, setSearchVector] = useState('[0.1, 0.2, 0.3]');
  const [searchLimit, setSearchLimit] = useState('20');
  const [searchResult, setSearchResult] = useState<unknown>(null);
  const [pipeline, setPipeline] = useState('[\n  {"$match": {}},\n  {"$limit": 20}\n]');
  const [aggregateResult, setAggregateResult] = useState<unknown>(null);
  const [liveEvents, setLiveEvents] = useState<Array<Record<string, unknown>>>([]);
  const [liveRunning, setLiveRunning] = useState(false);
  const [liveError, setLiveError] = useState('');
  const liveController = useRef<AbortController | null>(null);
  const [collectionMetrics, setCollectionMetrics] = useState<Record<string, unknown> | null>(null);
  const [metricsBusy, setMetricsBusy] = useState(false);
  const [busy, setBusy] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const [showExplain, setShowExplain] = useState(false);
  const [showFilterBuilder, setShowFilterBuilder] = useState(false);
  const [explainResult, setExplainResult] = useState<unknown>(null);
  const [revisions, setRevisions] = useState<Revision[]>([]);
  const [selectedRevision, setSelectedRevision] = useState<Revision | null>(null);
  const [historyBusy, setHistoryBusy] = useState(false);
  const [historyError, setHistoryError] = useState('');
  const [importUpsert, setImportUpsert] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const fileInput = useRef<HTMLInputElement>(null);

  const sort = useMemo(() => {
    const field = cleanField(sortField);
    return field ? [[field, sortDir === 'asc' ? 1 : -1]] : [];
  }, [sortDir, sortField]);

  const load = useCallback(async () => {
    setBusy(true);
    setError('');
    try {
      const filter = parseJson(filterText, 'Filter');
      const projection = projectionText.trim() ? parseJson(projectionText, 'Projection') : undefined;
      const [found, total, indexResult, textResult, fuzzyResult, vectorResult, ttlResult, historyResult, eventsResult, timeseriesResult] =
        await Promise.all([
          exec<Doc[]>(connection, {
            op: 'find',
            coll: name,
            filter,
            ...(projection === undefined ? {} : { projection }),
            sort,
            skip,
            limit,
          }),
          exec<{ count: number }>(connection, { op: 'count', coll: name, filter }),
          exec<{ indexes: IndexInfo[] }>(connection, { op: 'list_indexes', coll: name }),
          exec<{ text_indexes: string[] }>(connection, { op: 'list_text_indexes', coll: name }),
          exec<{ fuzzy_indexes: string[] }>(connection, { op: 'list_fuzzy_indexes', coll: name }),
          exec<{ vector_indexes: Array<{ field: string; dims: number; metric?: string }> }>(connection, {
            op: 'list_vector_indexes',
            coll: name,
          }),
          exec<{ ttl_seconds: number | null }>(connection, { op: 'get_collection_ttl', coll: name }),
          exec<{ max_versions: number }>(connection, { op: 'get_collection_history', coll: name }),
          exec<{ enabled: boolean }>(connection, { op: 'get_collection_events', coll: name }),
          exec<{ timeseries: TimeSeriesSettings; stats: TimeSeriesStats }>(connection, {
            op: 'get_collection_timeseries',
            coll: name,
          }).catch(() => null),
        ]);
      setDocs(found);
      setFieldPaths((current) => [...collectFieldPaths(found, '', new Set(current))].sort());
      setCount(total.count);
      setIndexes(indexResult.indexes ?? []);
      setTextIndexes(textResult.text_indexes ?? []);
      setFuzzyIndexes(fuzzyResult.fuzzy_indexes ?? []);
      setVectorIndexes(vectorResult.vector_indexes ?? []);
      setTtl(ttlResult.ttl_seconds == null ? '' : String(ttlResult.ttl_seconds));
      setHistory(String(historyResult.max_versions ?? 0));
      setEventsEnabled(Boolean(eventsResult.enabled));
      setTsSettings(timeseriesResult?.timeseries ?? null);
      setTsStats(timeseriesResult?.stats ?? null);
      if (timeseriesResult) {
        setTsEditCompression(timeseriesResult.timeseries.compression ?? 'balanced');
        setTsEditExpire(
          timeseriesResult.timeseries.expire_after_seconds == null
            ? ''
            : String(timeseriesResult.timeseries.expire_after_seconds),
        );
        setTsEditBucketDocs(String(timeseriesResult.timeseries.bucket_max_docs ?? ''));
      }
      if (selected) {
        const fresh = found.find((doc) => idOf(doc) === idOf(selected));
        setSelected(fresh ?? null);
        if (fresh) setEditor(pretty(fresh));
      }
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  }, [connection, filterText, limit, name, projectionText, selected, skip, sort]);

  const loadSubscriptions = useCallback(async () => {
    try {
      const result = await exec<{ subscriptions: Array<Record<string, unknown>> }>(connection, {
        op: 'list_subscriptions',
        coll: name,
      });
      setSubscriptions(result.subscriptions ?? []);
    } catch (err) {
      setError(errorText(err));
    }
  }, [connection, name]);

  const detectFields = useCallback(async () => {
    try {
      const sample = await exec<Doc[]>(connection, {
        op: 'find',
        coll: name,
        filter: {},
        limit: 500,
      });
      setSampleDocs(sample);
      setFieldPaths([...collectFieldPaths(sample)].sort());
    } catch {
      // Field discovery is best-effort; filters still work.
    }
  }, [connection, name]);

  useEffect(() => {
    void load();
    void detectFields();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connection, name]);

  useEffect(() => {
    if (tab === 'events') void loadSubscriptions();
    if (tab === 'metrics' && collectionMetrics === null) void loadCollectionMetrics();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab]);

  useEffect(() => {
    if (!notice) return;
    const timer = window.setTimeout(() => setNotice(''), 3000);
    return () => window.clearTimeout(timer);
  }, [notice]);

  const loadCollectionMetrics = async () => {
    setMetricsBusy(true);
    setError('');
    try {
      setCollectionMetrics(await fetchCollectionMetrics(connection, name));
    } catch (err) {
      setError(errorText(err));
    } finally {
      setMetricsBusy(false);
    }
  };

  const choose = (doc: Doc) => {
    setSelected(doc);
    setEditor(pretty(doc));
    setNotice('');
  };

  const toggleSelected = (id: string) => {
    setSelectedIds((current) => {
      const next = new Set(current);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const togglePage = () => {
    setSelectedIds((current) => {
      const ids = docs.map(idOf).filter((id) => id && !current.has(id));
      if (ids.length === 0) return new Set();
      return new Set(ids);
    });
  };

  const saveView = () => {
    const label = viewName.trim() || window.prompt('View name')?.trim();
    if (!label) return;
    const next = [
      ...savedViews.filter((view) => view.name !== label),
      { name: label, filter: filterText, sortField, sortDir, limit, projection: projectionText },
    ].sort((a, b) => String(a.name).localeCompare(String(b.name)));
    setSavedViews(next);
    localStorage.setItem(`nonodb.gui.views.${name}`, JSON.stringify(next));
    setViewName('');
    setNotice(`Saved view "${label}"`);
  };

  const applyView = (label: string) => {
    const view = savedViews.find((item) => item.name === label);
    if (!view) return;
    setFilterText(String(view.filter ?? '{}'));
    setSortField(String(view.sortField ?? '_id'));
    setSortDir(view.sortDir === 'desc' ? 'desc' : 'asc');
    setLimit(Number(view.limit ?? 50));
    setProjectionText(String(view.projection ?? ''));
    setSkip(0);
  };

  const openHistory = async () => {
    if (!selected || !idOf(selected)) return;
    setShowHistory(true);
    setHistoryBusy(true);
    setHistoryError('');
    setRevisions([]);
    setSelectedRevision(null);
    try {
      const result = await exec<{ revisions: Revision[] }>(connection, {
        op: 'history',
        coll: name,
        id: idOf(selected),
        limit: 100,
        include_docs: true,
      });
      setRevisions(result.revisions ?? []);
      setSelectedRevision(result.revisions?.[0] ?? null);
    } catch (err) {
      setHistoryError(errorText(err));
    } finally {
      setHistoryBusy(false);
    }
  };

  const insertDoc = async () => {
    setBusy(true);
    setError('');
    setNotice('');
    try {
      const doc = parseJson(newDoc, 'Document') as Doc;
      await exec(connection, { op: 'insert', coll: name, doc });
      setNotice('Document inserted');
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const updateDocument = async (target: Doc, doc: Doc) => {
    const id = idOf(target);
    if (!id) throw new Error('Document has no _id');
    const set: Doc = { ...doc };
    delete set._id;
    const unset: Doc = {};
    for (const key of Object.keys(target)) {
      if (key !== '_id' && !(key in doc)) unset[key] = '';
    }
    const update: Doc = {};
    if (Object.keys(set).length) update.$set = set;
    if (Object.keys(unset).length) update.$unset = unset;
    if (!Object.keys(update).length) return false;
    await exec(connection, {
      op: 'update',
      coll: name,
      filter: { _id: id },
      update,
    });
    return true;
  };

  const saveDoc = async () => {
    if (!selected) return;
    setBusy(true);
    setError('');
    setNotice('');
    try {
      const doc = parseJson(editor, 'Document') as Doc;
      const changed = await updateDocument(selected, doc);
      setNotice(changed ? 'Document saved' : 'No changes to save');
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const deleteDoc = async () => {
    if (!selected || !idOf(selected)) return;
    if (!window.confirm(`Delete document ${idOf(selected)}?`)) return;
    setBusy(true);
    setError('');
    setNotice('');
    try {
      await exec(connection, {
        op: 'delete',
        coll: name,
        filter: { _id: idOf(selected) },
        multi: false,
      });
      setSelected(null);
      setEditor('{}');
      setNotice('Document deleted');
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const bulkDelete = async () => {
    if (selectedIds.size === 0 || !window.confirm(`Delete ${selectedIds.size} documents?`)) return;
    setBusy(true);
    setError('');
    try {
      await exec(connection, {
        op: 'delete',
        coll: name,
        filter: { _id: { $in: [...selectedIds] } },
        multi: true,
      });
      setSelectedIds(new Set());
      setSelected(null);
      setNotice('Selected documents deleted');
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const bulkSetField = async () => {
    if (selectedIds.size === 0) return;
    const field = window.prompt('Field to set');
    if (!field) return;
    const raw = window.prompt(`JSON value for ${field}`);
    if (raw === null) return;
    setBusy(true);
    setError('');
    try {
      const value = JSON.parse(raw);
      await exec(connection, {
        op: 'update',
        coll: name,
        filter: { _id: { $in: [...selectedIds] } },
        update: { $set: { [field]: value } },
        multi: true,
      });
      setNotice(`Updated ${selectedIds.size} documents`);
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const saveSettings = async () => {
    setBusy(true);
    setError('');
    setNotice('');
    try {
      if (tsSettings) {
        await exec(connection, {
          op: 'set_collection_timeseries',
          coll: name,
          timeseries: {
            ...tsSettings,
            compression: tsEditCompression,
            expire_after_seconds: tsEditExpire.trim() === '' ? null : Number(tsEditExpire),
            bucket_max_docs:
              tsEditBucketDocs.trim() === ''
                ? tsSettings.bucket_max_docs
                : Number(tsEditBucketDocs),
          },
        });
      } else {
        await exec(connection, {
          op: 'set_collection_ttl',
          coll: name,
          ttl_seconds: ttl.trim() === '' ? null : Number(ttl),
        });
        await exec(connection, {
          op: 'set_collection_history',
          coll: name,
          max_versions: Number(history || 0),
        });
      }
      setNotice('Collection settings saved');
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const saveEvents = async () => {
    setBusy(true);
    setError('');
    try {
      const current = await exec<{ enabled: boolean; fields: string[] }>(connection, {
        op: 'get_collection_events',
        coll: name,
      });
      await exec(connection, {
        op: 'set_collection_events',
        coll: name,
        enabled: eventsEnabled,
        fields: current.fields ?? [],
        ops: [],
        filter: {},
        include_before: true,
        include_after: true,
        projection: null,
      });
      setNotice(eventsEnabled ? 'Automatic change events enabled' : 'Automatic change events disabled');
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const createIndex = async () => {
    const fields = indexField.split(',').map((part) => cleanField(part)).filter(Boolean);
    if (!fields.length) {
      setError('Index needs at least one field');
      return;
    }
    if (indexKind !== 'secondary' && fields.length !== 1) {
      setError('Text, fuzzy, and vector indexes need exactly one field');
      return;
    }
    setBusy(true);
    setError('');
    try {
      if (indexKind === 'secondary') {
        await exec(connection, {
          op: 'create_index',
          coll: name,
          fields,
          unique: indexUnique,
          include: indexInclude.split(',').map((part) => part.trim()).filter(Boolean),
          geo: indexGeo,
        });
      } else if (indexKind === 'text') {
        await exec(connection, { op: 'create_text_index', coll: name, field: fields[0] });
      } else if (indexKind === 'fuzzy') {
        await exec(connection, { op: 'create_fuzzy_index', coll: name, field: fields[0] });
      } else {
        await exec(connection, {
          op: 'create_vector_index',
          coll: name,
          field: fields[0],
          dims: Number(vectorDims),
          metric: vectorMetric,
        });
      }
      setIndexField('');
      setIndexInclude('');
      setIndexUnique(false);
      setIndexGeo(false);
      setNotice('Index created');
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const dropIndex = async (field: string, kind: 'secondary' | 'text' | 'fuzzy' | 'vector') => {
    if (!window.confirm(`Drop ${kind} index ${field}?`)) return;
    setBusy(true);
    setError('');
    try {
      const op = kind === 'secondary' ? 'drop_index' : `drop_${kind}_index`;
      await exec(connection, { op, coll: name, field });
      setNotice('Index dropped');
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const reindexIndexes = async () => {
    if (!window.confirm('Rebuild all indexes for this collection? This scans every document.')) return;
    setBusy(true);
    setError('');
    try {
      await exec(connection, { op: 'reindex', coll: name });
      setNotice('Indexes rebuilt');
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const runExplain = async () => {
    setBusy(true);
    setError('');
    try {
      const filter = parseJson(filterText, 'Filter');
      const projection = projectionText.trim() ? parseJson(projectionText, 'Projection') : undefined;
      setExplainResult(
        await exec(connection, {
          op: 'explain',
          coll: name,
          filter,
          ...(projection === undefined ? {} : { projection }),
          sort,
          skip,
          limit,
        }),
      );
      setShowExplain(true);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const exportCollection = async () => {
    setBusy(true);
    setError('');
    try {
      const filter = parseJson(filterText, 'Filter');
      const all: Doc[] = [];
      let cursor = 0;
      for (;;) {
        const page = await exec<Doc[]>(connection, {
          op: 'find',
          coll: name,
          filter,
          sort,
          skip: cursor,
          limit: 1000,
        });
        all.push(...page);
        if (page.length < 1000) break;
        cursor += page.length;
      }
      downloadText(`${name}.json`, pretty(all));
      setNotice(`Exported ${all.length} documents`);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const exportCsv = async () => {
    setBusy(true);
    setError('');
    try {
      const filter = parseJson(filterText, 'Filter');
      const all: Doc[] = [];
      let cursor = 0;
      for (;;) {
        const page = await exec<Doc[]>(connection, {
          op: 'find',
          coll: name,
          filter,
          sort,
          skip: cursor,
          limit: 1000,
        });
        all.push(...page);
        if (page.length < 1000) break;
        cursor += page.length;
      }
      downloadText(`${name}.csv`, toCsv(all), 'text/csv');
      setNotice(`Exported ${all.length} rows`);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const importFile = async (file: File) => {
    setBusy(true);
    setError('');
    try {
      const text = await file.text();
      let docs: Doc[];
      if (file.name.toLowerCase().endsWith('.csv')) {
        docs = parseCsv(text);
      } else {
        try {
          const parsed = JSON.parse(text);
          docs = Array.isArray(parsed) ? parsed : [parsed];
        } catch {
          docs = text
            .split(/\r?\n/)
            .map((line) => line.trim())
            .filter(Boolean)
            .map((line) => JSON.parse(line) as Doc);
        }
      }
      let imported = 0;
      for (let i = 0; i < docs.length; i += 500) {
        const chunk = docs.slice(i, i + 500);
        if (importUpsert) {
          for (const doc of chunk) {
            const id = idOf(doc);
            if (id) {
              const body = { ...doc };
              delete body._id;
              await exec(connection, {
                op: 'update',
                coll: name,
                filter: { _id: id },
                update: { $set: body },
                upsert: true,
              });
            } else {
              await exec(connection, { op: 'insert', coll: name, doc });
            }
          }
        } else {
          await exec(connection, { op: 'insert_many', coll: name, docs: chunk });
        }
        imported += chunk.length;
      }
      setNotice(`Imported ${imported} documents`);
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
      if (fileInput.current) fileInput.current.value = '';
    }
  };

  const createSubscription = async () => {
    setBusy(true);
    setError('');
    try {
      const count = subCount.trim() ? parseJson(subCount, 'Count condition') : undefined;
      await exec(connection, {
        op: 'create_subscription',
        coll: name,
        name: subName,
        filter: parseJson(subFilter, 'Filter'),
        ...(count === undefined ? {} : { count }),
        ops: subOps.split(',').map((part) => part.trim()).filter(Boolean),
        once: subOnce,
        lease_seconds: Number(subLease || 600),
      });
      setSubName('');
      setNotice('Subscription created');
      await loadSubscriptions();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const renewSubscription = async (subscriptionName: string) => {
    setBusy(true);
    try {
      await exec(connection, {
        op: 'renew_subscription',
        coll: name,
        name: subscriptionName,
        lease_seconds: Number(subLease || 600),
      });
      setNotice('Subscription renewed');
      await loadSubscriptions();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const deleteSubscription = async (subscriptionName: string) => {
    if (!window.confirm(`Delete subscription ${subscriptionName}?`)) return;
    setBusy(true);
    try {
      await exec(connection, { op: 'delete_subscription', coll: name, name: subscriptionName });
      setNotice('Subscription deleted');
      await loadSubscriptions();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const runSearch = async () => {
    setBusy(true);
    setError('');
    setSearchResult(null);
    try {
      const filter = parseJson(filterText, 'Filter');
      const limitValue = Number(searchLimit || 20);
      let result: unknown;
      if (searchMode === 'text') {
        result = await exec(connection, {
          op: 'search',
          coll: name,
          field: cleanField(searchField),
          query: searchQuery,
          filter,
          limit: limitValue,
        });
      } else if (searchMode === 'fuzzy') {
        result = await exec(connection, {
          op: 'fuzzy',
          coll: name,
          field: cleanField(searchField),
          query: searchQuery,
          filter,
          limit: limitValue,
        });
      } else {
        result = await exec(connection, {
          op: 'vector_search',
          coll: name,
          field: cleanField(searchField),
          vector: parseJson(searchVector, 'Vector'),
          filter,
          limit: limitValue,
        });
      }
      setSearchResult(result);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const startLive = async () => {
    setLiveError('');
    setLiveRunning(true);
    setLiveEvents([]);
    const controller = new AbortController();
    liveController.current = controller;
    try {
      await watchStream(
        connection,
        {
          op: 'watch',
          coll: name,
          mode: 'live',
          filter: parseJson(filterText, 'Filter'),
          heartbeat_ms: 10_000,
        },
        (event) => setLiveEvents((current) => [event, ...current].slice(0, 100)),
        controller.signal,
      );
    } catch (err) {
      if (!controller.signal.aborted) setLiveError(errorText(err));
    } finally {
      if (liveController.current === controller) {
        liveController.current = null;
        setLiveRunning(false);
      }
    }
  };

  const stopLive = () => {
    liveController.current?.abort();
    liveController.current = null;
    setLiveRunning(false);
  };

  const runAggregate = async () => {
    setBusy(true);
    setError('');
    setAggregateResult(null);
    try {
      setAggregateResult(
        await exec(connection, {
          op: 'aggregate',
          coll: name,
          pipeline: parseJson(pipeline, 'Pipeline'),
        }),
      );
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const restoreRevision = async (revision: Revision) => {
    if (!selected || !revision.doc) return;
    if (!window.confirm(`Restore revision ${revision.rev}?`)) return;
    setBusy(true);
    setError('');
    try {
      await updateDocument(selected, revision.doc);
      setShowHistory(false);
      setNotice(`Restored revision ${revision.rev}`);
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const schema = useMemo(() => buildSchema(sampleDocs.length > 0 ? sampleDocs : docs), [docs, sampleDocs]);
  const title = useMemo(() => `${name}${count == null ? '' : ` · ${count} docs`}`, [count, name]);
  const pageStart = count === null || count === 0 ? 0 : skip + 1;
  const pageEnd = Math.min(skip + docs.length, count ?? 0);
  const selectedRevisionIndex = selectedRevision
    ? revisions.findIndex((revision) => revision.rev === selectedRevision.rev)
    : -1;
  const olderRevision =
    selectedRevisionIndex >= 0 && selectedRevisionIndex + 1 < revisions.length
      ? revisions[selectedRevisionIndex + 1]
      : null;

  return (
    <div className="page">
      <datalist id="nonodb-field-paths">
        {fieldPaths.map((field) => <option key={field} value={field} />)}
      </datalist>
      <header className="page-header">
        <div>
          <h2>{title}</h2>
          <p>Browse, edit, query, and configure this collection</p>
        </div>
        <div className="actions">
          <button onClick={() => setShowSettings(true)}>Settings</button>
          <button onClick={() => void load()} disabled={busy}>
            {busy ? 'Working…' : 'Refresh'}
          </button>
        </div>
      </header>
      {error && <div className="error-box">{error}</div>}
      {notice && <div className="notice-box">{notice}</div>}

      <div className="tab-bar">
        {(['documents', 'indexes', 'events', 'search', 'aggregate', 'schema', 'metrics'] as const).map((item) => (
          <button key={item} className={tab === item ? 'active' : ''} onClick={() => setTab(item)}>
            {item === 'events' ? 'Changes' : item[0].toUpperCase() + item.slice(1)}
            {item === 'indexes' && <span className="tab-count">{indexes.length + textIndexes.length + fuzzyIndexes.length + vectorIndexes.length}</span>}
            {item === 'events' && <span className="tab-count">{subscriptions.length}</span>}
          </button>
        ))}
      </div>

      {tab === 'documents' && (
        <>
          <section className="panel">
            <div className="toolbar">
              <div className="grow filter-editor">
                <span className="field-label">Filter JSON — Ctrl+Space for field suggestions</span>
                <JsonEditor
                  value={filterText}
                  onChange={setFilterText}
                  height="90px"
                  fields={fieldPaths}
                />
              </div>
              <label className="compact">
                Sort field
                <input value={sortField} onChange={(e) => setSortField(e.target.value)} list="nonodb-field-paths" />
              </label>
              <label className="compact">
                Order
                <select value={sortDir} onChange={(e) => setSortDir(e.target.value as 'asc' | 'desc')}>
                  <option value="asc">asc</option>
                  <option value="desc">desc</option>
                </select>
              </label>
              <label className="compact">
                Limit
                <input type="number" min={1} max={1000} value={limit} onChange={(e) => setLimit(Number(e.target.value))} />
              </label>
              <button onClick={() => { setSkip(0); void load(); }} disabled={busy}>Apply</button>
              <details className="tools-menu">
                <summary>Tools ▾</summary>
                <div className="tools-panel">
                  <div className="tools-section">
                    <span className="tools-title">Query tools</span>
                    <button onClick={() => setShowFilterBuilder(true)}>Filter builder</button>
                    <button onClick={() => void runExplain()} disabled={busy}>Explain plan</button>
                  </div>
                  <div className="tools-section">
                    <span className="tools-title">Projection</span>
                    <input
                      value={projectionText}
                      onChange={(e) => setProjectionText(e.target.value)}
                      placeholder='{"_id":1,"name":1}'
                      spellCheck={false}
                    />
                  </div>
                  <div className="tools-section">
                    <span className="tools-title">Saved views</span>
                    <div className="tools-row">
                      <select value="" onChange={(e) => e.target.value && applyView(e.target.value)}>
                        <option value="">Open saved view…</option>
                        {savedViews.map((view) => (
                          <option key={String(view.name)} value={String(view.name)}>{String(view.name)}</option>
                        ))}
                      </select>
                      <input value={viewName} onChange={(e) => setViewName(e.target.value)} placeholder="view name" className="view-name" />
                      <button onClick={saveView}>Save</button>
                    </div>
                  </div>
                  <div className="tools-section">
                    <span className="tools-title">Data</span>
                    <button onClick={() => void exportCollection()} disabled={busy}>Export JSON</button>
                    <button onClick={() => void exportCsv()} disabled={busy}>Export CSV</button>
                    <label className="checkbox">
                      <input type="checkbox" checked={importUpsert} onChange={(e) => setImportUpsert(e.target.checked)} />
                      upsert on import
                    </label>
                    <button onClick={() => fileInput.current?.click()} disabled={busy}>Import JSON / NDJSON / CSV</button>
                    <input
                      ref={fileInput}
                      type="file"
                      accept=".json,.ndjson,.jsonl,.csv,application/json,text/csv"
                      hidden
                      onChange={(e) => e.target.files?.[0] && void importFile(e.target.files[0])}
                    />
                  </div>
                </div>
              </details>
            </div>
            {selectedIds.size > 0 && (
              <div className="bulk-bar">
                <span>{selectedIds.size} selected</span>
                <button className="danger" onClick={() => void bulkDelete()} disabled={busy}>Bulk delete</button>
                <button onClick={() => void bulkSetField()} disabled={busy}>Bulk set field</button>
                <button onClick={() => setSelectedIds(new Set())}>Clear</button>
              </div>
            )}
            <div className="split">
              <div className="doc-list">
                <label className="select-all">
                  <input type="checkbox" checked={docs.length > 0 && docs.every((doc) => selectedIds.has(idOf(doc)))} onChange={togglePage} />
                  Select page
                </label>
                {docs.length === 0 ? (
                  <p className="muted">No matching documents.</p>
                ) : (
                  docs.map((doc) => (
                    <div key={idOf(doc)} className={`doc-row-wrap${selected && idOf(selected) === idOf(doc) ? ' active' : ''}`}>
                      <input type="checkbox" checked={selectedIds.has(idOf(doc))} onChange={() => toggleSelected(idOf(doc))} />
                      <button className="doc-row" onClick={() => choose(doc)}>
                        <strong>{idOf(doc) || '(no _id)'}</strong>
                        <span>{pretty(doc).slice(0, 120)}</span>
                      </button>
                    </div>
                  ))
                )}
                <div className="pagination">
                  <button onClick={() => setSkip(Math.max(0, skip - limit))} disabled={skip === 0 || busy}>Previous</button>
                  <span>{pageStart}–{pageEnd} of {count ?? '?'}</span>
                  <button onClick={() => setSkip(skip + limit)} disabled={count !== null && skip + limit >= count}>Next</button>
                </div>
              </div>
              <div className="editor-pane">
                <div className="panel-title">
                  <h3>{selected ? `Edit ${idOf(selected)}` : 'New document'}</h3>
                  <div className="actions">
                    <button onClick={() => { try { setEditor(pretty(parseJson(editor, 'Document'))); } catch (err) { setError(errorText(err)); } }} disabled={!selected}>Format</button>
                    <button onClick={() => void navigator.clipboard.writeText(selected ? editor : newDoc)}>Copy</button>
                    {selected && <button onClick={() => void openHistory()} disabled={busy || historyBusy}>History</button>}
                    {selected && <button onClick={() => { setSelected(null); setEditor('{}'); setNotice(''); }} disabled={busy}>New</button>}
                    {selected && <button className="danger" onClick={() => void deleteDoc()} disabled={busy}>Delete</button>}
                    <button className="primary" onClick={() => void (selected ? saveDoc() : insertDoc())} disabled={busy}>{selected ? 'Save' : 'Insert'}</button>
                  </div>
                </div>
                <JsonEditor
                  value={selected ? editor : newDoc}
                  onChange={(value) => (selected ? setEditor(value) : setNewDoc(value))}
                  fields={fieldPaths}
                />
              </div>
            </div>
          </section>
        </>
      )}

      {tab === 'indexes' && (
        <section className="panel">
          <div className="panel-title"><h3>Indexes</h3><span>{indexes.length + textIndexes.length + fuzzyIndexes.length + vectorIndexes.length}</span></div>
          <div className="toolbar">
            <label className="compact">
              Type
              <select value={indexKind} onChange={(e) => setIndexKind(e.target.value as typeof indexKind)}>
                <option value="secondary">secondary</option>
                <option value="text">text / BM25</option>
                <option value="fuzzy">fuzzy</option>
                <option value="vector">vector</option>
              </select>
            </label>
            <label className="grow">
              Field(s)
              <input value={indexField} onChange={(e) => setIndexField(e.target.value)} list="nonodb-field-paths" placeholder={indexKind === 'secondary' ? 'field1,field2' : 'single field'} />
            </label>
            {indexKind === 'secondary' && (
              <>
                <label className="compact">
                  Include
                  <input value={indexInclude} onChange={(e) => setIndexInclude(e.target.value)} placeholder="fields" />
                </label>
                <label className="checkbox"><input type="checkbox" checked={indexUnique} onChange={(e) => setIndexUnique(e.target.checked)} /> unique</label>
                <label className="checkbox"><input type="checkbox" checked={indexGeo} onChange={(e) => setIndexGeo(e.target.checked)} /> geo</label>
              </>
            )}
            {indexKind === 'vector' && (
              <>
                <label className="compact">Dims<input value={vectorDims} onChange={(e) => setVectorDims(e.target.value)} /></label>
                <label className="compact">Metric<select value={vectorMetric} onChange={(e) => setVectorMetric(e.target.value)}><option value="cosine">cosine</option><option value="dot">dot</option><option value="l2">l2</option></select></label>
              </>
            )}
            <button onClick={() => void createIndex()} disabled={busy}>Create</button>
            <button onClick={() => void reindexIndexes()} disabled={busy || indexes.length + textIndexes.length + fuzzyIndexes.length + vectorIndexes.length === 0}>Reindex all</button>
          </div>
          <div className="index-list">
            {indexes.map((index) => (
              <div key={`secondary-${index.field}`} className="index-row">
                <span><strong>{index.field}</strong><span className="badge">secondary</span>{index.unique && <span className="badge">unique</span>}{index.geo && <span className="badge">geo</span>}</span>
                <button className="danger small" onClick={() => void dropIndex(index.field, 'secondary')}>Drop</button>
              </div>
            ))}
            {textIndexes.map((field) => (
              <div key={`text-${field}`} className="index-row"><span><strong>{field}</strong><span className="badge">text</span></span><button className="danger small" onClick={() => void dropIndex(field, 'text')}>Drop</button></div>
            ))}
            {fuzzyIndexes.map((field) => (
              <div key={`fuzzy-${field}`} className="index-row"><span><strong>{field}</strong><span className="badge">fuzzy</span></span><button className="danger small" onClick={() => void dropIndex(field, 'fuzzy')}>Drop</button></div>
            ))}
            {vectorIndexes.map((index) => (
              <div key={`vector-${index.field}`} className="index-row"><span><strong>{index.field}</strong><span className="badge">vector</span><span className="badge">{index.dims}d {index.metric ?? ''}</span></span><button className="danger small" onClick={() => void dropIndex(index.field, 'vector')}>Drop</button></div>
            ))}
            {indexes.length + textIndexes.length + fuzzyIndexes.length + vectorIndexes.length === 0 && <p className="muted">No indexes.</p>}
          </div>
        </section>
      )}

      {tab === 'events' && (
        <>
        <section className="panel">
          <div className="panel-title">
            <h3>Automatic change events</h3>
            <span className={`badge ${eventsEnabled ? '' : 'danger-badge'}`}>{eventsEnabled ? 'enabled' : 'disabled'}</span>
          </div>
          <p className="muted small">
            Append insert/update/delete events for this collection to the transactional outbox. Subscriptions, live
            watch, and webhooks all consume these changes; webhook filters and aggregates still run outside the writer.
          </p>
          <label className="checkbox">
            <input type="checkbox" checked={eventsEnabled} onChange={(e) => setEventsEnabled(e.target.checked)} />
            Write change events to <code>_events</code>
          </label>
          <div className="actions">
            <button className="primary" onClick={() => void saveEvents()} disabled={busy}>Save event source</button>
            {onOpenEventing && <button onClick={onOpenEventing}>Open Eventing</button>}
          </div>
        </section>
        <section className="panel">
          <div className="panel-title"><h3>Subscriptions</h3><button onClick={() => void loadSubscriptions()}>Refresh</button></div>
          <p className="muted small">Persistent subscriptions write matching events to <code>_events</code>. Renew the lease before it expires.</p>
          <div className="form-grid">
            <label>Name<input value={subName} onChange={(e) => setSubName(e.target.value)} placeholder="paid" /></label>
            <label>Lease seconds<input value={subLease} onChange={(e) => setSubLease(e.target.value)} /></label>
            <label>Filter<input value={subFilter} onChange={(e) => setSubFilter(e.target.value)} spellCheck={false} /></label>
            <label>Count condition (optional)<input value={subCount} onChange={(e) => setSubCount(e.target.value)} placeholder='{"filter":{},"gt":0}' spellCheck={false} /></label>
            <label>Ops<input value={subOps} onChange={(e) => setSubOps(e.target.value)} placeholder="insert,update,delete" /></label>
            <label className="checkbox"><input type="checkbox" checked={subOnce} onChange={(e) => setSubOnce(e.target.checked)} /> once / edge-triggered</label>
          </div>
          <div><button className="primary" onClick={() => void createSubscription()} disabled={busy || !subName}>Create subscription</button></div>
          <div className="index-list">
            {subscriptions.map((subscription) => {
              const subNameValue = String(subscription.name ?? '');
              return (
                <div key={subNameValue} className="index-row">
                  <span><strong>{subNameValue}</strong><span className="badge">{String(subscription.event_type ?? `subscription.${subNameValue}`)}</span><span className="muted small"> expires {subscription.expires_at ? new Date(Number(subscription.expires_at)).toLocaleString() : '-'}</span></span>
                  <span className="actions"><button className="small" onClick={() => void renewSubscription(subNameValue)}>Renew</button><button className="danger small" onClick={() => void deleteSubscription(subNameValue)}>Delete</button></span>
                </div>
              );
            })}
            {subscriptions.length === 0 && <p className="muted">No subscriptions.</p>}
          </div>
          <div className="panel-title">
            <h3>Live condition stream</h3>
            <div className="actions">
              <button onClick={() => void startLive()} disabled={liveRunning}>Start live</button>
              <button className="danger" onClick={stopLive} disabled={!liveRunning}>Stop</button>
            </div>
          </div>
          <p className="muted small">
            Uses the current collection filter in <code>mode:"live"</code>. Events are pushed
            directly over the stream and are not written to <code>_events</code>.
          </p>
          {liveError && <div className="error-box">{liveError}</div>}
          <div className="live-events">
            {liveEvents.length === 0 ? (
              <p className="muted">{liveRunning ? 'Waiting for matching writes…' : 'Not running.'}</p>
            ) : (
              liveEvents.map((event, index) => (
                <pre key={`${String(event._id ?? index)}-${index}`} className="result live-event">
                  {pretty(event)}
                </pre>
              ))
            )}
          </div>
        </section>
        </>
      )}

      {tab === 'search' && (
        <section className="panel">
          <div className="panel-title"><h3>Search</h3></div>
          <div className="form-grid">
            <label>Mode<select value={searchMode} onChange={(e) => setSearchMode(e.target.value as typeof searchMode)}><option value="text">text / BM25</option><option value="fuzzy">fuzzy</option><option value="vector">vector</option></select></label>
            <label>Field<input value={searchField} onChange={(e) => setSearchField(e.target.value)} list="nonodb-field-paths" placeholder="name (without quotes)" /></label>
            {searchMode === 'vector' ? <label>Vector JSON<input value={searchVector} onChange={(e) => setSearchVector(e.target.value)} spellCheck={false} /></label> : <label>Query<input value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} /></label>}
            <label>Limit<input value={searchLimit} onChange={(e) => setSearchLimit(e.target.value)} /></label>
          </div>
          <div><button className="primary" onClick={() => void runSearch()} disabled={busy || !searchField}>Run search</button></div>
          {searchResult !== null && <pre className="result">{pretty(searchResult)}</pre>}
        </section>
      )}

      {tab === 'aggregate' && (
        <section className="panel">
          <div className="panel-title"><h3>Aggregation pipeline</h3><button className="primary" onClick={() => void runAggregate()} disabled={busy}>Run</button></div>
          <p className="muted small">JSON array of stages: $match, $group, $sort, $project, $limit, $skip, $unwind and other supported stages.</p>
          <JsonEditor value={pipeline} onChange={setPipeline} height="240px" fields={fieldPaths} />
          {aggregateResult !== null && (
            <div className="panel-title"><h3>Result</h3><button onClick={() => downloadText(`${name}-aggregate.json`, pretty(aggregateResult))}>Export JSON</button></div>
          )}
          {aggregateResult !== null && <pre className="result">{pretty(aggregateResult)}</pre>}
        </section>
      )}

      {tab === 'schema' && (
        <section className="panel">
          <div className="panel-title"><h3>Schema explorer</h3><span>{schema.length} field paths</span></div>
          <p className="muted small">Inferred from up to 500 sampled documents. Click a field to add an exists filter.</p>
          <div className="schema-table">
            <div className="schema-row schema-head"><span>Field</span><span>Types</span><span>Coverage</span></div>
            {schema.map((field) => (
              <button key={field.path} className="schema-row" onClick={() => { setFilterText(JSON.stringify({ [field.path]: { $exists: true } }, null, 2)); setTab('documents'); }}>
                <span className="mono">{field.path}</span>
                <span>{field.types.join(', ')}</span>
                <span className="schema-coverage"><span style={{ width: `${Math.min(100, field.coverage)}%` }} />{field.coverage.toFixed(1)}%</span>
              </button>
            ))}
            {schema.length === 0 && <p className="muted">No sampled documents.</p>}
          </div>
        </section>
      )}

      {tab === 'metrics' && (
        <section className="panel">
          <div className="panel-title"><h3>Collection metrics</h3><button onClick={() => void loadCollectionMetrics()} disabled={metricsBusy}>{metricsBusy ? 'Loading…' : 'Refresh metrics'}</button></div>
          <div className="stat-grid">
            <div className="stat"><span>Requests</span><strong>{metricNumber(collectionMetrics, 'requests_total').toLocaleString()}</strong></div>
            <div className="stat"><span>Reads</span><strong>{metricNumber(collectionMetrics, 'read_ops_total').toLocaleString()}</strong></div>
            <div className="stat"><span>Writes</span><strong>{metricNumber(collectionMetrics, 'write_requests_total').toLocaleString()}</strong></div>
            <div className="stat"><span>Errors</span><strong>{metricNumber(collectionMetrics, 'errors_total').toLocaleString()}</strong></div>
          </div>
          <div className="metric-inline">
            <span>Avg request: <strong>{metricNumber(collectionMetrics, 'avg_request_us').toFixed(1)} µs</strong></span>
            <span>Last request: <strong>{metricNumber(collectionMetrics, 'last_request_us').toFixed(1)} µs</strong></span>
            <span>Cache hit ratio: <strong>{cacheRatio(collectionMetrics).toFixed(1)}%</strong></span>
          </div>
          <MetricBars data={collectionMetrics} items={[['requests_total','Requests'],['read_ops_total','Reads'],['write_requests_total','Writes'],['cache_hits_total','Cache hits'],['cache_misses_total','Cache misses'],['errors_total','Errors']]} />
          <details className="raw-metrics"><summary>Raw collection metrics JSON</summary><pre className="result">{pretty(collectionMetrics)}</pre></details>
        </section>
      )}

      {showExplain && (
        <Modal title={`Explain ${name}`} subtitle="Selected plan, bounds, and actual counts" onClose={() => setShowExplain(false)} wide>
          <pre className="result">{pretty(explainResult)}</pre>
        </Modal>
      )}

      {showFilterBuilder && (
        <Modal
          title={`Filter builder: ${name}`}
          subtitle="Nested conditions, operators, and a live MongoDB-style JSON preview"
          onClose={() => setShowFilterBuilder(false)}
          wide
        >
          <QueryBuilder value={filterText} onChange={setFilterText} fields={fieldPaths} />
          <div className="modal-actions">
            <button onClick={() => setShowFilterBuilder(false)}>Close</button>
            <button className="primary" onClick={() => setShowFilterBuilder(false)}>Use filter</button>
          </div>
        </Modal>
      )}

      {showHistory && selected && (
        <Modal title={`History: ${idOf(selected)}`} subtitle="Newest revisions first. History requires versioning enabled for this collection." onClose={() => setShowHistory(false)} wide>
          {historyError && <div className="error-box">{historyError}</div>}
          {historyBusy ? <p className="muted">Loading revisions…</p> : revisions.length === 0 ? <p className="muted">No revisions found. Enable versioning in collection Settings, then update the document.</p> : (
            <div className="history-layout">
              <div className="revision-list">
                {revisions.map((revision) => (
                  <button key={revision.rev} className={`revision-row${selectedRevision?.rev === revision.rev ? ' active' : ''}`} onClick={() => setSelectedRevision(revision)}>
                    <span className="revision-rev">rev {revision.rev}</span>
                    <span className={`badge ${revision.deleted ? 'danger-badge' : ''}`}>{revision.deleted ? 'deleted' : revision.op}</span>
                    <span className="muted small">{new Date(revision.at).toLocaleString()}</span>
                  </button>
                ))}
              </div>
              <div className="revision-detail">
                <div className="panel-title"><h3>Revision {selectedRevision?.rev}</h3><button className="primary" onClick={() => selectedRevision && void restoreRevision(selectedRevision)} disabled={busy || !selectedRevision?.doc}>Restore</button></div>
                <div className="diff-grid">
                  <div><span className="muted small">Selected revision</span><pre className="result revision-doc">{selectedRevision?.doc ? pretty(selectedRevision.doc) : '(no document payload)'}</pre></div>
                  <div><span className="muted small">Previous revision</span><pre className="result revision-doc">{olderRevision?.doc ? pretty(olderRevision.doc) : '(no older revision)'}</pre></div>
                </div>
              </div>
            </div>
          )}
        </Modal>
      )}

      {showSettings && (
        <Modal title={`${name} settings`} subtitle="Applies to this collection only" onClose={() => setShowSettings(false)}>
          <div className="form-grid">
            {tsSettings && (
              <>
                <label>
                  Time-series profile
                  <span className="field-hint">
                    timeField <strong>{tsSettings.time_field}</strong>
                    {tsSettings.meta_field ? ` · metaField ${tsSettings.meta_field}` : ''} ·{' '}
                    granularity {tsSettings.granularity}
                  </span>
                </label>
                <label>
                  Compression
                  <select
                    value={tsEditCompression}
                    onChange={(e) => setTsEditCompression(e.target.value)}
                  >
                    <option value="fast">fast</option>
                    <option value="balanced">balanced</option>
                    <option value="max">max</option>
                  </select>
                </label>
                <label>
                  Bucket max docs
                  <input
                    type="number"
                    min={1}
                    value={tsEditBucketDocs}
                    onChange={(e) => setTsEditBucketDocs(e.target.value)}
                  />
                  <span className="field-hint">
                    Structural fields (timeField, metaField, granularity) can only change while
                    the collection is empty.
                  </span>
                </label>
                <label>
                  Expire after (seconds)
                  <input
                    value={tsEditExpire}
                    onChange={(e) => setTsEditExpire(e.target.value)}
                    placeholder="empty = disabled"
                  />
                  <span className="field-hint">
                    Read-time expiry relative to the event time.
                  </span>
                </label>
                {tsStats && (
                  <div className="stat-grid">
                    <div className="stat"><span>Rows</span><strong>{tsStats.rows.toLocaleString()}</strong></div>
                    <div className="stat"><span>Deleted rows</span><strong>{tsStats.deleted_rows.toLocaleString()}</strong></div>
                    <div className="stat"><span>Buckets</span><strong>{tsStats.buckets} ({tsStats.open_buckets} open)</strong></div>
                    <div className="stat"><span>Series</span><strong>{tsStats.series}</strong></div>
                    <div className="stat"><span>Encoded size</span><strong>{formatBytes(tsStats.encoded_bytes + tsStats.open_bytes)}</strong></div>
                    <div className="stat"><span>Raw JSON</span><strong>{formatBytes(tsStats.raw_bytes)}</strong></div>
                  </div>
                )}
              </>
            )}
            {!tsSettings && (
              <>
                <label>Default document TTL (seconds)<input value={ttl} onChange={(e) => setTtl(e.target.value)} placeholder="empty = disabled" /><span className="field-hint">Documents expire after this many seconds unless they have their own TTL. Leave empty to disable automatic expiry.</span></label>
                <label>Versioning / history versions<input type="number" min={0} value={history} onChange={(e) => setHistory(e.target.value)} /><span className="field-hint">Number of previous document revisions to keep. 0 disables history for this collection.</span></label>
              </>
            )}
          </div>
          <div className="modal-actions"><button onClick={() => setShowSettings(false)}>Cancel</button><button className="primary" onClick={() => void saveSettings().then(() => setShowSettings(false))} disabled={busy}>Save settings</button></div>
        </Modal>
      )}
    </div>
  );
}

function toCsv(rows: unknown): string {
  if (!Array.isArray(rows) || rows.length === 0) return '';
  const objects = rows.filter((row): row is Record<string, unknown> => Boolean(row) && typeof row === 'object' && !Array.isArray(row));
  if (objects.length === 0) return '';
  const columns = [...new Set(objects.flatMap((row) => Object.keys(row)))];
  const escape = (value: unknown) => {
    const raw = value === undefined || value === null ? '' : typeof value === 'object' ? JSON.stringify(value) : String(value);
    return `"${raw.replaceAll('"', '""')}"`;
  };
  return [columns.map(escape).join(','), ...objects.map((row) => columns.map((column) => escape(row[column])).join(','))].join('\n');
}

type PlaygroundMode = 'sql' | 'json';

function resultRows(value: unknown): Record<string, unknown>[] | null {
  const candidate = Array.isArray(value)
    ? value
    : value && typeof value === 'object'
      ? ((value as Record<string, unknown>).results ??
        (value as Record<string, unknown>).rows ??
        (value as Record<string, unknown>).documents)
      : null;
  if (!Array.isArray(candidate) || candidate.length === 0) return null;
  const rows = candidate.filter(
    (row): row is Record<string, unknown> =>
      Boolean(row) && typeof row === 'object' && !Array.isArray(row),
  );
  return rows.length === candidate.length ? rows : null;
}

function cellText(value: unknown): string {
  if (value === undefined) return '';
  if (value === null) return 'null';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

function PlaygroundTable({ rows }: { rows: Record<string, unknown>[] }) {
  const columns = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  const visible = rows.slice(0, 200);
  return (
    <div className="playground-table-wrap">
      <table className="metric-table playground-table">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {visible.map((row, index) => (
            <tr key={String(row._id ?? index)}>
              {columns.map((column) => (
                <td key={column} title={cellText(row[column])}>
                  {cellText(row[column])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > visible.length && (
        <p className="muted small">
          Showing the first {visible.length} of {rows.length} rows. Export CSV or JSON for the full result.
        </p>
      )}
    </div>
  );
}

function PlaygroundView({
  connection,
  collections,
  selected,
}: {
  connection: Connection;
  collections: string[];
  selected: string | null;
}) {
  const [mode, setMode] = useState<PlaygroundMode>('sql');
  const [query, setQuery] = useState('SELECT * FROM orders LIMIT 20');
  const [params, setParams] = useState('[]');
  const [jsonRequest, setJsonRequest] = useState(`{
  "op": "find",
  "coll": "orders",
  "filter": {},
  "limit": 20
}`);
  const [result, setResult] = useState<unknown>(null);
  const [explainResult, setExplainResult] = useState<unknown>(null);
  const [sqlHistory, setSqlHistory] = useState<string[]>(() => {
    try {
      return JSON.parse(localStorage.getItem('nonodb.gui.sqlHistory') ?? '[]') as string[];
    } catch {
      return [];
    }
  });
  const [jsonHistory, setJsonHistory] = useState<string[]>(() => {
    try {
      return JSON.parse(localStorage.getItem('nonodb.gui.jsonHistory') ?? '[]') as string[];
    } catch {
      return [];
    }
  });
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  const history = mode === 'sql' ? sqlHistory : jsonHistory;
  const historyKey = mode === 'sql' ? 'nonodb.gui.sqlHistory' : 'nonodb.gui.jsonHistory';

  const remember = (statement: string) => {
    const current = mode === 'sql' ? sqlHistory : jsonHistory;
    const next = [statement, ...current.filter((item) => item !== statement)].slice(0, 30);
    if (mode === 'sql') setSqlHistory(next);
    else setJsonHistory(next);
    localStorage.setItem(historyKey, JSON.stringify(next));
  };

  const clearHistory = () => {
    if (mode === 'sql') setSqlHistory([]);
    else setJsonHistory([]);
    localStorage.removeItem(historyKey);
  };

  const parsedParams = () => parseJson(params, 'Params') as unknown[];

  const parsedRequest = () => {
    const value = parseJson(jsonRequest, 'JSON request');
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      throw new Error('JSON request: expected one object, for example {"op":"find","coll":"orders"}');
    }
    const request = value as Record<string, unknown>;
    if (typeof request.op !== 'string' || request.op.trim() === '') {
      throw new Error('JSON request: the `op` field is required');
    }
    return request;
  };

  const run = async () => {
    setBusy(true);
    setError('');
    setResult(null);
    setExplainResult(null);
    try {
      if (mode === 'sql') {
        const out = await exec(connection, { op: 'sql', query, params: parsedParams() });
        setResult(out);
        remember(query);
      } else {
        const request = parsedRequest();
        const out = await exec(connection, request);
        setResult(out);
        remember(jsonRequest);
      }
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const explain = async () => {
    setBusy(true);
    setError('');
    setExplainResult(null);
    try {
      setExplainResult(await exec(connection, { op: 'explain_sql', query, params: parsedParams() }));
      remember(query);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const formatJson = () => {
    try {
      setJsonRequest(pretty(parseJson(jsonRequest, 'JSON request')));
      setError('');
    } catch (err) {
      setError(errorText(err));
    }
  };

  const loadTemplate = (kind: 'find' | 'aggregate' | 'search' | 'sql' | 'insert') => {
    const coll = selected ?? collections.find((name) => !name.startsWith('_')) ?? collections[0] ?? 'orders';
    const templates: Record<typeof kind, Record<string, unknown>> = {
      find: { op: 'find', coll, filter: {}, limit: 20 },
      aggregate: {
        op: 'aggregate',
        coll,
        pipeline: [{ $match: {} }, { $sort: { _id: 1 } }, { $limit: 20 }],
      },
      search: {
        op: 'search_all',
        coll,
        mode: 'lexical',
        query: 'coffee',
        fields: ['title'],
        limit: 10,
      },
      sql: { op: 'sql', query: `SELECT * FROM ${coll} LIMIT 20`, params: [] },
      insert: { op: 'insert', coll, doc: { example: true } },
    };
    setJsonRequest(pretty(templates[kind]));
    setError('');
  };

  const rows = resultRows(result);

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <h2>Playground</h2>
          <p>Run SQL reports or raw JSON operations against the current database</p>
        </div>
        <div className="actions">
          {mode === 'sql' && (
            <button onClick={() => void explain()} disabled={busy}>Explain</button>
          )}
          <button onClick={() => void run()} disabled={busy} className="primary">
            {busy ? 'Running…' : `Run ${mode === 'sql' ? 'SQL' : 'JSON'}`}
          </button>
        </div>
      </header>
      <div className="mode-tabs" role="tablist" aria-label="Playground mode">
        <button
          role="tab"
          aria-selected={mode === 'sql'}
          className={mode === 'sql' ? 'active' : ''}
          onClick={() => { setMode('sql'); setError(''); }}
        >
          SQL
        </button>
        <button
          role="tab"
          aria-selected={mode === 'json'}
          className={mode === 'json' ? 'active' : ''}
          onClick={() => { setMode('json'); setError(''); }}
        >
          JSON
        </button>
        <span className="muted small">
          {mode === 'sql'
            ? 'One collection per SELECT/INSERT/UPDATE/DELETE statement.'
            : 'Any single /v1/exec request: find, aggregate, search_all, insert, SQL, and more.'}
        </span>
      </div>
      {error && <div className="error-box">{error}</div>}
      {mode === 'sql' ? (
        <section className="panel">
          <SqlEditor value={query} onChange={setQuery} />
          <label>Params JSON<input value={params} onChange={(e) => setParams(e.target.value)} spellCheck={false} /></label>
        </section>
      ) : (
        <section className="panel">
          <div className="panel-title">
            <h3>JSON request</h3>
            <div className="actions">
              <button className="small" onClick={() => loadTemplate('find')}>Find</button>
              <button className="small" onClick={() => loadTemplate('aggregate')}>Aggregate</button>
              <button className="small" onClick={() => loadTemplate('search')}>Search</button>
              <button className="small" onClick={() => loadTemplate('sql')}>SQL</button>
              <button className="small" onClick={() => loadTemplate('insert')}>Insert</button>
              <button className="small" onClick={formatJson}>Format</button>
            </div>
          </div>
          <JsonEditor value={jsonRequest} onChange={setJsonRequest} height="300px" />
          <p className="muted small">
            Templates use {selected ? <code>{selected}</code> : 'the first visible collection'}. Change <code>coll</code> freely; results use the same indexes, filters, and search modes as the clients.
          </p>
        </section>
      )}
      {history.length > 0 && (
        <section className="panel">
          <div className="panel-title">
            <h3>{mode === 'sql' ? 'SQL' : 'JSON'} history</h3>
            <button onClick={clearHistory}>Clear</button>
          </div>
          <div className="query-history">
            {history.map((statement, index) => (
              <button
                key={`${index}-${statement}`}
                className="query-history-row"
                onClick={() => (mode === 'sql' ? setQuery(statement) : setJsonRequest(statement))}
                title={statement}
              >
                {statement.replace(/\s+/g, ' ')}
              </button>
            ))}
          </div>
        </section>
      )}
      {result !== null && (
        <section className="panel">
          <div className="panel-title">
            <h3>Result{rows ? ` · ${rows.length} row${rows.length === 1 ? '' : 's'}` : ''}</h3>
            <div className="actions">
              <button onClick={() => downloadText('playground-result.json', pretty(result))}>Export JSON</button>
              <button
                onClick={() => downloadText('playground-result.csv', toCsv(rows ?? result), 'text/csv')}
                disabled={!rows}
              >
                Export CSV
              </button>
            </div>
          </div>
          {rows ? (
            <>
              <PlaygroundTable rows={rows} />
              <details className="raw-result">
                <summary>Raw JSON</summary>
                <pre className="result">{pretty(result)}</pre>
              </details>
            </>
          ) : (
            <pre className="result">{pretty(result)}</pre>
          )}
        </section>
      )}
      {explainResult !== null && (
        <section className="panel">
          <div className="panel-title"><h3>Explain</h3></div>
          <pre className="result">{pretty(explainResult)}</pre>
        </section>
      )}
    </div>
  );
}

function metricNumber(data: Record<string, unknown> | null, key: string): number {
  const value = data?.[key];
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

function cacheRatio(data: Record<string, unknown> | null): number {
  const hits = metricNumber(data, 'cache_hits_total');
  const total = hits + metricNumber(data, 'cache_misses_total');
  return total > 0 ? (hits / total) * 100 : 0;
}

function nestedNumber(data: Record<string, unknown> | null, path: string[]): number {
  let current: unknown = data;
  for (const key of path) {
    if (!current || typeof current !== 'object' || Array.isArray(current)) return 0;
    current = (current as Record<string, unknown>)[key];
  }
  return typeof current === 'number' && Number.isFinite(current) ? current : 0;
}

function MetricBars({
  data,
  items,
}: {
  data: Record<string, unknown> | null;
  items: Array<[string, string]>;
}) {
  const max = Math.max(1, ...items.map(([key]) => metricNumber(data, key)));
  return (
    <div className="metric-bars">
      {items.map(([key, label]) => {
        const value = metricNumber(data, key);
        return (
          <div className="metric-bar-row" key={key}>
            <span>{label}</span>
            <div className="metric-track">
              <div className="metric-fill" style={{ width: `${(value / max) * 100}%` }} />
            </div>
            <strong>{value.toLocaleString()}</strong>
          </div>
        );
      })}
    </div>
  );
}

function MetricChart({
  samples,
  metric,
  label,
}: {
  samples: Array<Record<string, number>>;
  metric: string;
  label: string;
}) {
  if (samples.length < 2) return <p className="muted small">{label}: waiting for samples…</p>;
  const width = 620;
  const height = 90;
  const values = samples.map((sample) => sample[metric] ?? 0);
  const max = Math.max(1, ...values);
  const min = Math.min(...values);
  const range = Math.max(1, max - min);
  const points = values
    .map((value, index) => {
      const x = (index / (values.length - 1)) * width;
      const y = height - ((value - min) / range) * (height - 8) - 4;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');
  return (
    <div className="metric-chart">
      <div className="metric-chart-title"><span>{label}</span><strong>{values[values.length - 1]?.toLocaleString()}</strong></div>
      <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
        <polyline points={points} fill="none" stroke="currentColor" strokeWidth="2" />
      </svg>
    </div>
  );
}


function parseScopes(text: string): string[] {
  return text
    .split(/[\n,]+/)
    .map((scope) => scope.trim())
    .filter(Boolean);
}

function shortFingerprint(value: string): string {
  return value.length > 24 ? `${value.slice(0, 12)}…${value.slice(-8)}` : value;
}

function AuthView({ connection }: { connection: Connection }) {
  const [summary, setSummary] = useState<AuthSummary | null>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [editing, setEditing] = useState<string | null>(null);
  const [principalName, setPrincipalName] = useState('');
  const [scopes, setScopes] = useState('read:*');
  const [tokenPrincipal, setTokenPrincipal] = useState('');
  const [tokenKid, setTokenKid] = useState('');
  const [newToken, setNewToken] = useState('');
  const [certFingerprint, setCertFingerprint] = useState('');
  const [certPrincipal, setCertPrincipal] = useState('');

  const load = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const result = await authSummary(connection);
      setSummary(result);
      setTokenPrincipal((current) =>
        result.principals.some((principal) => principal.name === current)
          ? current
          : result.principals[0]?.name || '',
      );
      setCertPrincipal((current) =>
        result.principals.some((principal) => principal.name === current)
          ? current
          : result.principals[0]?.name || '',
      );
    } catch (err) {
      setError(errorText(err));
    } finally {
      setLoading(false);
    }
  }, [connection]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    if (!notice) return;
    const timer = window.setTimeout(() => setNotice(''), 4000);
    return () => window.clearTimeout(timer);
  }, [notice]);

  const savePrincipal = async () => {
    const name = principalName.trim();
    const parsed = parseScopes(scopes);
    if (!name || parsed.length === 0) {
      setError('Enter a principal name and at least one scope.');
      return;
    }
    setBusy(true);
    setError('');
    try {
      if (editing) {
        await updatePrincipal(connection, editing, parsed);
        setNotice(`Updated principal ${editing}`);
      } else {
        await createPrincipal(connection, name, parsed);
        setNotice(`Added principal ${name}`);
      }
      setEditing(null);
      setPrincipalName('');
      setScopes('read:*');
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const editPrincipal = (name: string, currentScopes: string[]) => {
    setEditing(name);
    setPrincipalName(name);
    setScopes(currentScopes.join(', '));
    setError('');
  };

  const removePrincipal = async (name: string) => {
    if (!window.confirm(`Delete principal ${name}?`)) return;
    setBusy(true);
    setError('');
    try {
      await deletePrincipal(connection, name);
      setNotice(`Deleted principal ${name}`);
      if (editing === name) {
        setEditing(null);
        setPrincipalName('');
      }
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const createToken = async () => {
    if (!tokenPrincipal) {
      setError('Choose a principal for the token.');
      return;
    }
    setBusy(true);
    setError('');
    setNewToken('');
    try {
      const result = await createAuthToken(connection, tokenPrincipal, tokenKid.trim() || undefined);
      setNewToken(result.token);
      setTokenKid('');
      setNotice(`Created token ${result.kid}. Copy it now — it will not be shown again.`);
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const toggleToken = async (kid: string, disabled: boolean) => {
    setBusy(true);
    setError('');
    try {
      await updateAuthToken(connection, kid, disabled);
      setNotice(`${disabled ? 'Disabled' : 'Enabled'} token ${kid}`);
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const addCertificate = async () => {
    if (!certFingerprint.trim() || !certPrincipal) {
      setError('Enter a SHA-256 fingerprint and a principal.');
      return;
    }
    setBusy(true);
    setError('');
    try {
      await createCertificate(connection, certFingerprint.trim(), certPrincipal);
      setCertFingerprint('');
      setNotice('Added client certificate');
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const toggleCertificate = async (sha256: string, disabled: boolean) => {
    setBusy(true);
    setError('');
    try {
      await updateCertificate(connection, sha256, disabled);
      setNotice(`${disabled ? 'Disabled' : 'Enabled'} certificate`);
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const copy = async (text: string) => {
    await navigator.clipboard.writeText(text);
    setNotice('Copied to clipboard');
  };

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <h2>Authorization</h2>
          <p>Principals, bearer tokens, and mTLS certificates</p>
        </div>
        <div className="actions">
          <button onClick={() => void load()} disabled={loading || busy}>
            {loading ? 'Loading…' : 'Refresh'}
          </button>
        </div>
      </header>
      {error && <div className="error-box">{error}</div>}
      {notice && <div className="notice-box">{notice}</div>}

      {!summary ? (
        <p className="muted">Loading authorization state…</p>
      ) : (
        <>
          <section className="panel">
            <div className="stat-grid">
              <div className="stat"><span>Principals</span><strong>{summary.principals.length}</strong></div>
              <div className="stat"><span>Tokens</span><strong>{summary.tokens.length}</strong></div>
              <div className="stat"><span>Active tokens</span><strong>{summary.tokens.filter((token) => !token.disabled).length}</strong></div>
              <div className="stat"><span>Certificates</span><strong>{summary.certificates.length}</strong></div>
            </div>
            <p className="muted small">Registry generation: {summary.generation}. Only principals with <code>admin:db</code> can manage authorization.</p>
          </section>

          <section className="panel">
            <div className="panel-title"><h3>Credential types</h3></div>
            <div className="credential-grid">
              <article className="credential-card">
                <h4>Principal</h4>
                <p><strong>Who the caller is.</strong> A principal is an identity plus scopes. It is not a secret. Tokens and certificates authenticate <em>as</em> a principal and inherit its permissions.</p>
              </article>
              <article className="credential-card">
                <h4>Bearer token</h4>
                <p><strong>Application secret.</strong> Sent as <code>Authorization: Bearer ...</code>. The plaintext is shown once and only its SHA-256 hash is stored. Use one token per application and disable it to revoke access.</p>
              </article>
              <article className="credential-card">
                <h4>mTLS certificate</h4>
                <p><strong>Machine identity.</strong> A client certificate is validated against <code>NONODB_TLS_CLIENT_CA</code> and mapped to a principal by its SHA-256 fingerprint. No bearer secret is sent in HTTP headers.</p>
              </article>
            </div>
            <p className="muted small">Scopes are permissions, not credentials: <code>write</code> does not imply <code>read</code>, <code>admin:db</code> is instance-wide, and <code>metrics:read</code> is monitoring-only.</p>
          </section>

          {newToken && (
            <section className="panel auth-token-panel">
              <div className="panel-title"><h3>New token — copy it now</h3><button className="small" onClick={() => void copy(newToken)}>Copy token</button></div>
              <p className="muted small">This plaintext token is shown once. Store it in a secret manager; the daemon keeps only its SHA-256 hash.</p>
              <code className="auth-token-value">{newToken}</code>
              <button className="small" onClick={() => setNewToken('')}>Hide token</button>
            </section>
          )}

          <section className="panel">
            <div className="panel-title">
              <h3>Principals</h3>
              <span>{summary.principals.length}</span>
            </div>
            <p className="muted small">Identity and permissions. Create one principal per application or team, then attach a token or certificate to it.</p>
            <div className="form-grid">
              <label>
                Principal name
                <input
                  value={principalName}
                  onChange={(e) => setPrincipalName(e.target.value)}
                  disabled={editing !== null}
                  placeholder="app-reader"
                />
              </label>
              <label>
                Scopes (comma or newline separated)
                <input
                  value={scopes}
                  onChange={(e) => setScopes(e.target.value)}
                  placeholder="read:app1/orders, write:app1/orders"
                />
                <span className="field-hint">
                  Examples: <code>read:app1/orders</code>, <code>write:app1/*</code>, <code>admin:db</code>, <code>metrics:read</code>.
                </span>
              </label>
            </div>
            <div className="toolbar">
              <button className="primary" onClick={() => void savePrincipal()} disabled={busy}>
                {editing ? 'Update principal' : 'Add principal'}
              </button>
              {editing && (
                <button onClick={() => { setEditing(null); setPrincipalName(''); setScopes('read:*'); }}>
                  Cancel edit
                </button>
              )}
            </div>
            <div className="index-list">
              {summary.principals.map((principal) => (
                <div key={principal.name} className="index-row">
                  <div>
                    <strong>{principal.name}</strong>
                    <div className="auth-scope-list">
                      {principal.scopes.map((scope) => <span key={scope} className="badge scope-badge">{scope}</span>)}
                    </div>
                    <span className="muted small">{principal.tokens} token(s), {principal.certificates} certificate(s)</span>
                  </div>
                  <div className="actions">
                    <button className="small" onClick={() => editPrincipal(principal.name, principal.scopes)}>Edit</button>
                    <button className="small" onClick={() => void removePrincipal(principal.name)} disabled={busy}>Delete</button>
                  </div>
                </div>
              ))}
              {summary.principals.length === 0 && <p className="muted">No principals. Create one with the admin scope first.</p>}
            </div>
          </section>

          <section className="panel">
            <div className="panel-title"><h3>Bearer tokens</h3><span>{summary.tokens.length}</span></div>
            <p className="muted small">Secrets for applications, CLI, and API clients. The plaintext token is shown only once; disabling it revokes access without deleting audit history.</p>
            <div className="form-grid">
              <label>
                Principal
                <select value={tokenPrincipal} onChange={(e) => setTokenPrincipal(e.target.value)}>
                  {summary.principals.map((principal) => <option key={principal.name} value={principal.name}>{principal.name}</option>)}
                </select>
              </label>
              <label>
                Kid (optional)
                <input value={tokenKid} onChange={(e) => setTokenKid(e.target.value)} placeholder="auto-generated" />
                <span className="field-hint">Optional stable ID for rotation and audit. Leave empty to generate one.</span>
              </label>
            </div>
            <div className="toolbar">
              <button className="primary" onClick={() => void createToken()} disabled={busy || !tokenPrincipal}>Create token</button>
            </div>
            <div className="index-list">
              {summary.tokens.map((token) => (
                <div key={token.kid} className="index-row">
                  <div>
                    <strong className="mono">{token.kid}</strong>
                    <span className="muted small"> → {token.principal} </span>
                    <span className={`badge ${token.disabled ? 'danger-badge' : ''}`}>{token.disabled ? 'disabled' : 'active'}</span>
                  </div>
                  <button className="small" onClick={() => void toggleToken(token.kid, !token.disabled)} disabled={busy}>
                    {token.disabled ? 'Enable' : 'Disable'}
                  </button>
                </div>
              ))}
              {summary.tokens.length === 0 && <p className="muted">No tokens.</p>}
            </div>
          </section>

          <section className="panel">
            <div className="panel-title"><h3>mTLS certificates</h3><span>{summary.certificates.length}</span></div>
            <p className="muted small">Machine identities over mutual TLS. Register the SHA-256 fingerprint of the client certificate's DER encoding; the certificate must chain to the configured client CA.</p>
            <div className="form-grid">
              <label>
                SHA-256 fingerprint
                <input value={certFingerprint} onChange={(e) => setCertFingerprint(e.target.value)} placeholder="64 hex characters" />
                <span className="field-hint">Example: <code>openssl x509 -in client.crt -outform DER | sha256sum</code>.</span>
              </label>
              <label>
                Principal
                <select value={certPrincipal} onChange={(e) => setCertPrincipal(e.target.value)}>
                  {summary.principals.map((principal) => <option key={principal.name} value={principal.name}>{principal.name}</option>)}
                </select>
              </label>
            </div>
            <div className="toolbar">
              <button className="primary" onClick={() => void addCertificate()} disabled={busy || !certFingerprint.trim() || !certPrincipal}>Add certificate</button>
            </div>
            <div className="index-list">
              {summary.certificates.map((cert) => (
                <div key={cert.sha256} className="index-row">
                  <div>
                    <strong className="mono" title={cert.sha256}>{shortFingerprint(cert.sha256)}</strong>
                    <span className="muted small"> → {cert.principal} </span>
                    <span className={`badge ${cert.disabled ? 'danger-badge' : ''}`}>{cert.disabled ? 'disabled' : 'active'}</span>
                  </div>
                  <button className="small" onClick={() => void toggleCertificate(cert.sha256, !cert.disabled)} disabled={busy}>
                    {cert.disabled ? 'Enable' : 'Disable'}
                  </button>
                </div>
              ))}
              {summary.certificates.length === 0 && <p className="muted">No client certificates.</p>}
            </div>
          </section>
        </>
      )}
    </div>
  );
}

function DatabaseView({ connection }: { connection: Connection }) {
  const [info, setInfo] = useState<MapInfo | null>(null);
  const [size, setSize] = useState('');
  const [importUpsert, setImportUpsert] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [busy, setBusy] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);

  const load = useCallback(async () => {
    setError('');
    try {
      setInfo(await exec<MapInfo>(connection, { op: 'map_info' }));
    } catch (err) {
      setError(errorText(err));
    }
  }, [connection]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    if (!notice) return;
    const timer = window.setTimeout(() => setNotice(''), 3000);
    return () => window.clearTimeout(timer);
  }, [notice]);

  const resize = async () => {
    setBusy(true);
    setError('');
    setNotice('');
    try {
      await exec(connection, { op: 'resize_map', size: Number(size) });
      setNotice('Map resized');
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const exportDatabase = async () => {
    setBusy(true);
    setError('');
    try {
      const result = await exec<{ collections: string[] }>(connection, { op: 'list_collections' });
      const payload: { version: number; exported_at: string; collections: Record<string, Doc[]> } = {
        version: 1,
        exported_at: new Date().toISOString(),
        collections: {},
      };
      for (const coll of result.collections ?? []) {
        const docs: Doc[] = [];
        let cursor = 0;
        for (;;) {
          const page = await exec<Doc[]>(connection, {
            op: 'find',
            coll,
            filter: {},
            sort: [['_id', 1]],
            skip: cursor,
            limit: 1000,
          });
          docs.push(...page);
          if (page.length < 1000) break;
          cursor += page.length;
        }
        payload.collections[coll] = docs;
      }
      downloadText('nonodb-export.json', pretty(payload));
      setNotice('Database exported');
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const importDatabase = async (file: File) => {
    setBusy(true);
    setError('');
    try {
      const payload = JSON.parse(await file.text()) as {
        collections?: Record<string, Doc[]>;
      };
      if (!payload.collections) throw new Error('Expected {"collections": {...}}');
      const existing = await exec<{ collections: string[] }>(connection, { op: 'list_collections' });
      const known = new Set(existing.collections ?? []);
      let imported = 0;
      for (const [coll, docs] of Object.entries(payload.collections)) {
        if (!known.has(coll)) {
          await exec(connection, { op: 'create_collection', coll });
        }
        for (let i = 0; i < docs.length; i += 500) {
          const chunk = docs.slice(i, i + 500);
          if (importUpsert) {
            for (const doc of chunk) {
              const id = idOf(doc);
              if (id) {
                const body = { ...doc };
                delete body._id;
                await exec(connection, {
                  op: 'update',
                  coll,
                  filter: { _id: id },
                  update: { $set: body },
                  upsert: true,
                });
              } else {
                await exec(connection, { op: 'insert', coll, doc });
              }
            }
          } else {
            await exec(connection, { op: 'insert_many', coll, docs: chunk });
          }
          imported += chunk.length;
        }
      }
      setNotice(`Imported ${imported} documents`);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
      if (fileInput.current) fileInput.current.value = '';
    }
  };

  const copy = async (text: string) => {
    await navigator.clipboard.writeText(text);
    setNotice('Copied to clipboard');
  };

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <h2>Database settings</h2>
          <p>LMDB map, import/export, and maintenance</p>
        </div>
        <div className="actions">
          <button onClick={() => void load()}>Refresh</button>
        </div>
      </header>
      {error && <div className="error-box">{error}</div>}
      {notice && <div className="notice-box">{notice}</div>}
      <section className="panel">
        <div className="stat-grid">
          <div className="stat"><span>Map size</span><strong>{info ? formatBytes(info.map_size) : '-'}</strong></div>
          <div className="stat"><span>Used</span><strong>{info ? formatBytes(info.used_bytes) : '-'}</strong></div>
          <div className="stat"><span>Free</span><strong>{info ? formatBytes(info.free_bytes) : '-'}</strong></div>
          <div className="stat"><span>Page size</span><strong>{info ? String(info.page_size) : '-'}</strong></div>
        </div>
        <div className="toolbar">
          <label className="grow">New map size in bytes<input value={size} onChange={(e) => setSize(e.target.value)} placeholder="4294967296" /></label>
          <button onClick={() => void resize()} disabled={busy || !size}>Resize map</button>
        </div>
      </section>

      <section className="panel">
        <div className="panel-title"><h3>Import / export database</h3></div>
        <p className="muted small">Exports every collection to one JSON file. Import creates missing collections and inserts or upserts documents.</p>
        <div className="toolbar">
          <button onClick={() => void exportDatabase()} disabled={busy}>Export all collections</button>
          <button onClick={() => fileInput.current?.click()} disabled={busy}>Import database JSON</button>
          <label className="checkbox"><input type="checkbox" checked={importUpsert} onChange={(e) => setImportUpsert(e.target.checked)} /> upsert on import</label>
          <input ref={fileInput} type="file" accept=".json,application/json" hidden onChange={(e) => e.target.files?.[0] && void importDatabase(e.target.files[0])} />
        </div>
      </section>

      <section className="panel">
        <div className="panel-title"><h3>Databases</h3></div>
        <p className="muted small">This daemon serves one LMDB database at a time. To run another database, start a second daemon with a different <code>NONODB_DATA</code> directory and socket.</p>
      </section>

      <section className="panel">
        <div className="panel-title"><h3>Maintenance (CLI)</h3></div>
        <p className="muted small">Backup, restore, and verification remain CLI-only because the HTTP API intentionally does not expose filesystem paths. Authorization is managed from the <strong>Authorization</strong> tab.</p>
        <div className="command-list">
          {[
            'nonodb-cli backup /var/lib/nonodb ./backup',
            'nonodb-cli verify-backup ./backup',
            'nonodb-cli restore ./backup /var/lib/nonodb --verify',
            'nonodb-cli auth check',
          ].map((command) => (
            <div key={command} className="command-row"><code>{command}</code><button className="small" onClick={() => void copy(command)}>Copy</button></div>
          ))}
        </div>
      </section>
    </div>
  );
}

function Shell({
  connection,
  onConnectionChange,
  onDisconnect,
}: {
  connection: Connection;
  onConnectionChange: (connection: Connection) => void;
  onDisconnect: () => void;
}) {
  const [collections, setCollections] = useState<string[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [tab, setTab] = useState<'dashboard' | 'storage' | 'playground' | 'database' | 'backups' | 'import' | 'replication' | 'cluster' | 'network' | 'auth' | 'eventing'>('dashboard');
  const [showNewCollection, setShowNewCollection] = useState(false);
  const [showPalette, setShowPalette] = useState(false);
  const [paletteQuery, setPaletteQuery] = useState('');
  const [newCollection, setNewCollection] = useState('');
  const [ttl, setTtl] = useState('');
  const [history, setHistory] = useState('0');
  const [profile, setProfile] = useState<'standard' | 'vault' | 'timeseries'>('standard');
  const [timeField, setTimeField] = useState('ts');
  const [metaField, setMetaField] = useState('');
  const [granularity, setGranularity] = useState('seconds');
  const [tsCompression, setTsCompression] = useState('balanced');
  const [expireAfter, setExpireAfter] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [databases, setDatabases] = useState<string[]>([]);
  const [collectionTab, setCollectionTab] = useState<CollectionTab>('documents');

  useEffect(() => {
    let cancelled = false;
    exec<{ databases: string[] }>(connection, { op: 'list_databases' })
      .then((result) => {
        if (!cancelled) setDatabases(result.databases ?? []);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [connection]);

  const switchDatabase = (database: string) => {
    if (!database || database === connection.database) return;
    const next = { ...connection, database };
    saveConnection(next);
    setSelected(null);
    setCollectionTab('documents');
    setTab('dashboard');
    onConnectionChange(next);
  };

  const openCollection = (name: string, target: CollectionTab = 'documents') => {
    setSelected(name);
    setCollectionTab(target);
    setTab('dashboard');
  };

  const refreshCollections = useCallback(async () => {
    try {
      const result = await exec<{ collections: string[] }>(connection, {
        op: 'list_collections',
      });
      setCollections(result.collections ?? []);
      setError('');
    } catch (err) {
      setError(errorText(err));
    }
  }, [connection]);

  useEffect(() => {
    void refreshCollections();
  }, [refreshCollections]);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault();
        setShowPalette((current) => !current);
      }
      if (event.key === 'Escape') setShowPalette(false);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  const createCollection = async () => {
    if (!newCollection.trim()) return;
    setBusy(true);
    setError('');
    try {
      const name = newCollection.trim();
      const request: Record<string, unknown> = {
        op: 'create_collection',
        coll: name,
        profile,
        ttl_seconds: profile === 'timeseries' || ttl.trim() === '' ? null : Number(ttl),
      };
      if (profile === 'timeseries') {
        request.timeseries = {
          timeField: timeField.trim(),
          ...(metaField.trim() ? { metaField: metaField.trim() } : {}),
          granularity,
          compression: tsCompression,
          ...(expireAfter.trim() ? { expireAfterSeconds: Number(expireAfter) } : {}),
        };
      }
      await exec(connection, request);
      const versions = Number(history || 0);
      if (versions > 0) {
        await exec(connection, {
          op: 'set_collection_history',
          coll: name,
          max_versions: versions,
        });
      }
      setNewCollection('');
      setTtl('');
      setHistory('0');
      setProfile('standard');
      setTimeField('ts');
      setMetaField('');
      setGranularity('seconds');
      setTsCompression('balanced');
      setExpireAfter('');
      setShowNewCollection(false);
      await refreshCollections();
      setCollectionTab('documents');
      setTab('dashboard');
      setSelected(name);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-mark"><img src="/nonodb-mark.svg" alt="" /></span>
          <div>
            <strong>NonoDB</strong>
            <small>Console</small>
          </div>
        </div>
        <nav className="nav">
          <button className={tab === 'dashboard' && !selected ? 'active' : ''} onClick={() => { setTab('dashboard'); setSelected(null); }}>
            Dashboard
          </button>
          <button className={tab === 'playground' ? 'active' : ''} onClick={() => setTab('playground')}>
            Playground
          </button>
          <button className={tab === 'database' ? 'active' : ''} onClick={() => setTab('database')}>
            Database
          </button>
          <button className={tab === 'backups' ? 'active' : ''} onClick={() => setTab('backups')}>
            Backups
          </button>
          <button className={tab === 'import' ? 'active' : ''} onClick={() => setTab('import')}>
            Import
          </button>
          <button className={tab === 'eventing' ? 'active' : ''} onClick={() => setTab('eventing')}>
            Eventing
          </button>
          <button className={tab === 'replication' ? 'active' : ''} onClick={() => setTab('replication')}>
            Replication
          </button>
          <button className={tab === 'cluster' ? 'active' : ''} onClick={() => setTab('cluster')}>
            Cluster
          </button>
          <button className={tab === 'network' ? 'active' : ''} onClick={() => setTab('network')}>
            Network
          </button>
          <button className={tab === 'auth' ? 'active' : ''} onClick={() => setTab('auth')}>
            Authorization
          </button>
          {collections.includes('_jobs') && (
            <button onClick={() => openCollection('_jobs')}>Jobs _jobs</button>
          )}
        </nav>
        <div className="sidebar-section">
          <span>Database</span>
          <button className="icon-button" onClick={() => void refreshCollections()} title="Refresh">
            ↻
          </button>
        </div>
        <div className="database-picker">
          <select
            className="database-select"
            value={connection.database || 'default'}
            onChange={(e) => switchDatabase(e.target.value)}
            title="Switch database"
          >
            {[...new Set(['default', connection.database || 'default', ...databases])].map((name) => (
              <option key={name} value={name}>{name}</option>
            ))}
          </select>
          <button
            className="icon-button storage-button"
            onClick={() => setTab('storage')}
            title="Collection storage"
            aria-label="Collection storage"
          >
            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
              <ellipse cx="12" cy="5" rx="8" ry="3" />
              <path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5" />
              <path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6" />
            </svg>
          </button>
        </div>
        <div className="sidebar-section">
          <span>Collections</span>
        </div>
        <div className="collection-list">
          {collections.map((name) => (
            <button
              key={name}
              className={selected === name && tab === 'dashboard' ? 'active' : ''}
              onClick={() => openCollection(name)}
            >
              {name}
            </button>
          ))}
          {collections.length === 0 && <p className="muted small">No collections</p>}
        </div>
        <button className="primary new-collection-button" onClick={() => setShowNewCollection(true)}>
          + New collection
        </button>
        <div className="sidebar-footer">
          <span title={connection.endpoint}>{connection.endpoint}</span>
          <button className="link-button" onClick={() => setShowPalette(true)}>
            Command palette · Ctrl+K
          </button>
          <button className="link-button" onClick={onDisconnect}>
            Disconnect
          </button>
        </div>
      </aside>
      <main className="main">
        {error && <div className="error-box sticky">{error}</div>}
        {tab === 'storage' ? (
          <StorageView connection={connection} onOpenCollection={(name) => openCollection(name, 'metrics')} />
        ) : tab === 'playground' ? (
          <PlaygroundView connection={connection} collections={collections} selected={selected} />
        ) : tab === 'database' ? (
          <DatabaseView connection={connection} />
        ) : tab === 'backups' ? (
          <BackupsPage connection={connection} />
        ) : tab === 'import' ? (
          <ImportsPage connection={connection} />
        ) : tab === 'eventing' ? (
          <EventingPage
            connection={connection}
            collections={collections}
            onOpenCollection={(name, eventingTab) => openCollection(name, eventingTab ?? 'events')}
          />
        ) : tab === 'replication' ? (
          <ReplicationPage connection={connection} />
        ) : tab === 'cluster' ? (
          <ClusterPage connection={connection} />
        ) : tab === 'network' ? (
          <NetworkView connection={connection} />
        ) : tab === 'auth' ? (
          <AuthView connection={connection} />
        ) : selected ? (
          <CollectionView
            key={`${selected}:${collectionTab}`}
            connection={connection}
            name={selected}
            initialTab={collectionTab}
            onOpenEventing={() => setTab('eventing')}
          />
        ) : (
          <Dashboard
            connection={connection}
            collections={collections}
            onRefresh={() => void refreshCollections()}
          />
        )}
      </main>
      {showPalette && (
        <Modal title="Command palette" subtitle="Ctrl+K to open or close" onClose={() => setShowPalette(false)}>
          <input
            autoFocus
            value={paletteQuery}
            onChange={(e) => setPaletteQuery(e.target.value)}
            placeholder="Search commands or collections…"
          />
          <div className="palette-list">
            {[
              { label: 'Open Dashboard', run: () => { setTab('dashboard'); setSelected(null); } },
              { label: 'Open Storage', run: () => setTab('storage') },
              { label: 'Open Playground (SQL + JSON)', run: () => setTab('playground') },
              { label: 'Open Database settings', run: () => setTab('database') },
              { label: 'Open Backups', run: () => setTab('backups') },
              { label: 'Open Import', run: () => setTab('import') },
              { label: 'Open Eventing', run: () => setTab('eventing') },
              { label: 'Open Replication', run: () => setTab('replication') },
              { label: 'Open Cluster', run: () => setTab('cluster') },
              { label: 'Open Network', run: () => setTab('network') },
              { label: 'Open Authorization', run: () => setTab('auth') },
              { label: 'New collection', run: () => setShowNewCollection(true) },
              ...collections.map((coll) => ({ label: `Open collection: ${coll}`, run: () => openCollection(coll) })),
            ]
              .filter((item) => item.label.toLowerCase().includes(paletteQuery.toLowerCase()))
              .slice(0, 20)
              .map((item) => (
                <button
                  key={item.label}
                  className="palette-row"
                  onClick={() => {
                    item.run();
                    setShowPalette(false);
                    setPaletteQuery('');
                  }}
                >
                  {item.label}
                </button>
              ))}
          </div>
        </Modal>
      )}
      {showNewCollection && (
        <Modal
          title="New collection"
          subtitle="Create a standard collection in the current database"
          onClose={() => setShowNewCollection(false)}
        >
          <div className="form-grid">
            <label>
              Collection name
              <input
                autoFocus
                value={newCollection}
                onChange={(e) => setNewCollection(e.target.value)}
                placeholder="orders"
              />
            </label>
            <label>
              Storage profile
              <select
                value={profile}
                onChange={(e) => setProfile(e.target.value as 'standard' | 'vault' | 'timeseries')}
              >
                <option value="standard">standard</option>
                <option value="vault">vault (encrypted)</option>
                <option value="timeseries">timeseries (columnar buckets)</option>
              </select>
              {profile === 'vault' && (
                <span className="field-hint">
                  Vault collections require tenant, key id, and vault key on the Connect
                  screen. Data is encrypted per tenant.
                </span>
              )}
              {profile === 'timeseries' && (
                <span className="field-hint">
                  Rows are grouped into buckets by time window and optional series field.
                  Field names become columns; updates and deletes use tombstones.
                </span>
              )}
            </label>
            {profile === 'timeseries' && (
              <>
                <label>
                  Time field (required)
                  <input
                    value={timeField}
                    onChange={(e) => setTimeField(e.target.value)}
                    placeholder="ts"
                  />
                  <span className="field-hint">
                    Document field holding the event time. Cannot be changed after rows exist.
                  </span>
                </label>
                <label>
                  Meta / series field
                  <input
                    value={metaField}
                    onChange={(e) => setMetaField(e.target.value)}
                    placeholder="device (optional)"
                  />
                </label>
                <label>
                  Granularity
                  <select value={granularity} onChange={(e) => setGranularity(e.target.value)}>
                    <option value="seconds">seconds</option>
                    <option value="minutes">minutes</option>
                    <option value="hours">hours</option>
                  </select>
                </label>
                <label>
                  Compression
                  <select value={tsCompression} onChange={(e) => setTsCompression(e.target.value)}>
                    <option value="fast">fast</option>
                    <option value="balanced">balanced</option>
                    <option value="max">max</option>
                  </select>
                </label>
                <label>
                  Expire after (seconds)
                  <input
                    value={expireAfter}
                    onChange={(e) => setExpireAfter(e.target.value)}
                    placeholder="empty = read-time only"
                  />
                  <span className="field-hint">
                    Rows older than this many seconds are hidden at read time.
                  </span>
                </label>
              </>
            )}
            <label>
              Default document TTL (seconds)
              <input
                value={ttl}
                onChange={(e) => setTtl(e.target.value)}
                placeholder="empty = disabled"
              />
              <span className="field-hint">
                Optional. After this many seconds documents expire automatically. Use 0 or
                empty to disable.
              </span>
            </label>
            <label>
              Versioning / history versions
              <input
                type="number"
                min={0}
                value={history}
                onChange={(e) => setHistory(e.target.value)}
              />
              <span className="field-hint">
                0 disables revision history. A positive number keeps that many previous
                versions per document.
              </span>
            </label>
          </div>
          <div className="modal-actions">
            <button onClick={() => setShowNewCollection(false)}>Cancel</button>
            <button
              className="primary"
              onClick={() => void createCollection()}
              disabled={busy || !newCollection.trim() || (profile === 'timeseries' && !timeField.trim())}
            >
              Create collection
            </button>
          </div>
        </Modal>
      )}
    </div>
  );
}

export default function App() {
  const [connection, setConnection] = useState<Connection>(() => loadConnection());
  const [connected, setConnected] = useState(false);
  const [checking, setChecking] = useState(true);
  const [lastTiming, setLastTiming] = useState<ApiTiming | null>(null);
  const [timings, setTimings] = useState<ApiTiming[]>([]);
  const [showActivity, setShowActivity] = useState(false);

  useEffect(
    () =>
      subscribeTiming((timing) => {
        setLastTiming(timing);
        setTimings((current) => [timing, ...current].slice(0, 100));
      }),
    [],
  );

  useEffect(() => {
    let cancelled = false;
    const boot = async () => {
      let current = loadConnection();
      const defaults = await sessionDefaults();
      const sameOrigin =
        current.endpoint.replace(/\/$/, '') === window.location.origin.replace(/\/$/, '');
      // The CLI injects the active daemon token into /gui/session. When the GUI
      // is served from the same origin, this token is authoritative and must
      // replace a stale token cached by an earlier cluster on the same port.
      if (defaults.token && (sameOrigin || !current.token)) {
        current = { ...current, token: defaults.token };
        saveConnection(current);
      }
      if (cancelled) return;
      setConnection(current);
      if (!current.endpoint) {
        setChecking(false);
        return;
      }
      try {
        await exec(current, { op: 'list_collections' });
        if (!cancelled) setConnected(true);
      } catch {
        if (!cancelled) setConnected(false);
      } finally {
        if (!cancelled) setChecking(false);
      }
    };
    void boot();
    return () => {
      cancelled = true;
    };
  }, []);

  let content: ReactNode;
  if (checking) {
    content = <div className="splash">Connecting to NonoDB…</div>;
  } else if (!connected) {
    content = (
      <ConnectView
        onConnected={(next) => {
          setConnection(next);
          setConnected(true);
        }}
      />
    );
  } else {
    content = (
      <Shell
        connection={connection}
        onConnectionChange={(next) => setConnection(next)}
        onDisconnect={() => {
          clearConnection();
          setConnection(defaultConnection());
          setConnected(false);
        }}
      />
    );
  }

  return (
    <>
      {content}
      {lastTiming && (
        <button
          type="button"
          className={`timing-badge${lastTiming.ok ? '' : ' failed'}`}
          onClick={() => setShowActivity(true)}
          title="Show recent operations"
        >
          <span>{lastTiming.op}</span>
          <strong>{lastTiming.ms.toFixed(1)} ms</strong>
        </button>
      )}
      {showActivity && (
        <Modal
          title="Recent operations"
          subtitle="Backend timings for this GUI session"
          onClose={() => setShowActivity(false)}
          wide
        >
          <div className="activity-list">
            {timings.length === 0 ? (
              <p className="muted">No operations yet.</p>
            ) : (
              timings.map((timing, index) => (
                <div key={`${timing.at}-${index}`} className={`activity-row${timing.ok ? '' : ' failed'}`}>
                  <span className="mono">{timing.op}</span>
                  <strong>{timing.ms.toFixed(1)} ms</strong>
                  <span className="muted small">{new Date(timing.at).toLocaleTimeString()}</span>
                  <span className={`badge ${timing.ok ? '' : 'danger-badge'}`}>{timing.ok ? 'ok' : 'error'}</span>
                </div>
              ))
            )}
          </div>
        </Modal>
      )}
    </>
  );
}
