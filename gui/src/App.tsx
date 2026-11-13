import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { json as jsonLanguage } from '@codemirror/lang-json';
import { sql as sqlLanguage } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';
import { autocompletion, type CompletionContext } from '@codemirror/autocomplete';
import QueryBuilder from './QueryBuilder';
import {
  authSummary,
  backupsList,
  browseImports,
  clearConnection,
  collectionMetrics as fetchCollectionMetrics,
  createAuthToken,
  createCertificate,
  createPrincipal,
  databaseStats,
  defaultConnection,
  deleteBackupJob,
  deletePrincipal,
  errorText,
  exec,
  generateBackupKey,
  generateBackupSecret,
  health,
  importOverview,
  importStatus,
  runImport,
  loadConnection,
  loadProfiles,
  metrics,
  saveConnection,
  saveProfiles,
  saveBackupJob,
  runBackupJob,
  sessionDefaults,
  type AuthSummary,
  type BackupItem,
  type BackupJob,
  type BackupsView,
  type ConnectionProfile,
  type DiscoveredImport,
  type ImportSource,
  type ImportStatus,
  type ImportOverview,
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
}: {
  connection: Connection;
  name: string;
  initialTab?: CollectionTab;
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
      const [found, total, indexResult, textResult, fuzzyResult, vectorResult, ttlResult, historyResult, eventsResult] =
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
      setNotice('Collection settings saved');
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
            {item[0].toUpperCase() + item.slice(1)}
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
              <input value={indexField} onChange={(e) => setIndexField(e.target.value)} list="nonodb-field-paths" placeholder={indexKind === 'secondary' ? 'field1,field2' : 'field'} />
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
            <label>Default document TTL (seconds)<input value={ttl} onChange={(e) => setTtl(e.target.value)} placeholder="empty = disabled" /><span className="field-hint">Documents expire after this many seconds unless they have their own TTL. Leave empty to disable automatic expiry.</span></label>
            <label>Versioning / history versions<input type="number" min={0} value={history} onChange={(e) => setHistory(e.target.value)} /><span className="field-hint">Number of previous document revisions to keep. 0 disables history for this collection.</span></label>
            <label className="checkbox span-2"><input type="checkbox" checked={eventsEnabled} onChange={(e) => setEventsEnabled(e.target.checked)} /> Automatic change events (insert / update / delete)</label>
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

function SqlView({ connection }: { connection: Connection }) {
  const [query, setQuery] = useState('SELECT * FROM orders LIMIT 20');
  const [params, setParams] = useState('[]');
  const [result, setResult] = useState<unknown>(null);
  const [explainResult, setExplainResult] = useState<unknown>(null);
  const [history, setHistory] = useState<string[]>(() => {
    try {
      return JSON.parse(localStorage.getItem('nonodb.gui.sqlHistory') ?? '[]') as string[];
    } catch {
      return [];
    }
  });
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  const remember = (statement: string) => {
    const next = [statement, ...history.filter((item) => item !== statement)].slice(0, 30);
    setHistory(next);
    localStorage.setItem('nonodb.gui.sqlHistory', JSON.stringify(next));
  };

  const parsedParams = () => parseJson(params, 'Params') as unknown[];

  const run = async () => {
    setBusy(true);
    setError('');
    setResult(null);
    setExplainResult(null);
    try {
      const out = await exec(connection, { op: 'sql', query, params: parsedParams() });
      setResult(out);
      remember(query);
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

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <h2>SQL console</h2>
          <p>Read and write one collection per statement</p>
        </div>
        <div className="actions">
          <button onClick={() => void explain()} disabled={busy}>Explain</button>
          <button onClick={() => void run()} disabled={busy} className="primary">{busy ? 'Running…' : 'Run'}</button>
        </div>
      </header>
      {error && <div className="error-box">{error}</div>}
      <section className="panel">
        <SqlEditor value={query} onChange={setQuery} />
        <label>Params JSON<input value={params} onChange={(e) => setParams(e.target.value)} spellCheck={false} /></label>
      </section>
      {history.length > 0 && (
        <section className="panel">
          <div className="panel-title"><h3>Query history</h3><button onClick={() => { setHistory([]); localStorage.removeItem('nonodb.gui.sqlHistory'); }}>Clear</button></div>
          <div className="query-history">
            {history.map((statement, index) => (
              <button key={`${index}-${statement}`} className="query-history-row" onClick={() => setQuery(statement)} title={statement}>
                {statement}
              </button>
            ))}
          </div>
        </section>
      )}
      {result !== null && (
        <section className="panel">
          <div className="panel-title"><h3>Result</h3><div className="actions"><button onClick={() => downloadText('query-result.json', pretty(result))}>Export JSON</button><button onClick={() => downloadText('query-result.csv', toCsv(result), 'text/csv')}>Export CSV</button></div></div>
          <pre className="result">{pretty(result)}</pre>
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
  const [tab, setTab] = useState<'dashboard' | 'storage' | 'sql' | 'database' | 'backups' | 'import' | 'auth'>('dashboard');
  const [showNewCollection, setShowNewCollection] = useState(false);
  const [showPalette, setShowPalette] = useState(false);
  const [paletteQuery, setPaletteQuery] = useState('');
  const [newCollection, setNewCollection] = useState('');
  const [ttl, setTtl] = useState('');
  const [history, setHistory] = useState('0');
  const [profile, setProfile] = useState<'standard' | 'vault'>('standard');
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
      await exec(connection, {
        op: 'create_collection',
        coll: name,
        profile,
        ttl_seconds: ttl.trim() === '' ? null : Number(ttl),
      });
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
          <button className={tab === 'sql' ? 'active' : ''} onClick={() => setTab('sql')}>
            SQL
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
          <button className={tab === 'auth' ? 'active' : ''} onClick={() => setTab('auth')}>
            Authorization
          </button>
          {collections.includes('_jobs') && (
            <button onClick={() => openCollection('_jobs')}>Jobs _jobs</button>
          )}
          {collections.includes('_events') && (
            <button onClick={() => openCollection('_events')}>Outbox _events</button>
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
        ) : tab === 'sql' ? (
          <SqlView connection={connection} />
        ) : tab === 'database' ? (
          <DatabaseView connection={connection} />
        ) : tab === 'backups' ? (
          <BackupsPage connection={connection} />
        ) : tab === 'import' ? (
          <ImportsPage connection={connection} />
        ) : tab === 'auth' ? (
          <AuthView connection={connection} />
        ) : selected ? (
          <CollectionView
            key={`${selected}:${collectionTab}`}
            connection={connection}
            name={selected}
            initialTab={collectionTab}
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
              { label: 'Open SQL console', run: () => setTab('sql') },
              { label: 'Open Database settings', run: () => setTab('database') },
              { label: 'Open Backups', run: () => setTab('backups') },
              { label: 'Open Import', run: () => setTab('import') },
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
              <select value={profile} onChange={(e) => setProfile(e.target.value as 'standard' | 'vault')}>
                <option value="standard">standard</option>
                <option value="vault">vault (encrypted)</option>
              </select>
              {profile === 'vault' && (
                <span className="field-hint">
                  Vault collections require tenant, key id, and vault key on the Connect
                  screen. Data is encrypted per tenant.
                </span>
              )}
            </label>
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
              disabled={busy || !newCollection.trim()}
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
      if (!current.token && defaults.token) {
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
