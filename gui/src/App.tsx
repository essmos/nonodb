import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { json as jsonLanguage } from '@codemirror/lang-json';
import { sql as sqlLanguage } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';
import { autocompletion, type CompletionContext } from '@codemirror/autocomplete';
import QueryBuilder from './QueryBuilder';
import {
  clearConnection,
  defaultConnection,
  errorText,
  exec,
  health,
  loadConnection,
  loadProfiles,
  metrics,
  saveConnection,
  saveProfiles,
  sessionDefaults,
  type ConnectionProfile,
  subscribeTiming,
  watchStream,
  type ApiTiming,
  type Connection,
} from './api';

type Doc = Record<string, unknown>;

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
          <span className="brand-mark">N</span>
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

function Dashboard({
  connection,
  collections,
  onOpen,
  onRefresh,
  onNewCollection,
}: {
  connection: Connection;
  collections: string[];
  onOpen: (name: string) => void;
  onRefresh: () => void;
  onNewCollection: () => void;
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
          <h2>Overview</h2>
          <p>Database status and collections</p>
        </div>
        <div className="actions">
          <button onClick={onNewCollection}>+ New collection</button>
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
      <section className="panel">
        <div className="panel-title">
          <h3>Collections</h3>
          <span>{collections.length}</span>
        </div>
        {collections.length === 0 ? (
          <p className="muted">No collections yet.</p>
        ) : (
          <div className="collection-grid">
            {collections.map((name) => (
              <button key={name} className="collection-card" onClick={() => onOpen(name)}>
                <span className="collection-icon">{name.slice(0, 1).toUpperCase()}</span>
                <span>{name}</span>
              </button>
            ))}
          </div>
        )}
      </section>
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
}: {
  connection: Connection;
  name: string;
}) {
  const [tab, setTab] = useState<
    'documents' | 'indexes' | 'events' | 'search' | 'aggregate' | 'schema' | 'metrics'
  >('documents');
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
      setCollectionMetrics(await metrics(connection));
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
            <div className="stat"><span>Matching documents</span><strong>{count == null ? '-' : count.toLocaleString()}</strong></div>
            <div className="stat"><span>Indexes</span><strong>{indexes.length + textIndexes.length + fuzzyIndexes.length + vectorIndexes.length}</strong></div>
            <div className="stat"><span>Default TTL</span><strong>{ttl ? `${ttl} s` : 'off'}</strong></div>
            <div className="stat"><span>Versioning</span><strong>{history === '0' ? 'off' : `${history} rev`}</strong></div>
          </div>
          <p className="muted small">Runtime counters are daemon-wide in this release. Collection-specific counters are not exposed by the API yet.</p>
          {collectionMetrics ? <><MetricBars data={collectionMetrics} items={[['requests_total','Requests'],['read_ops_total','Reads'],['write_ops_total','Writes'],['cache_hits_total','Cache hits'],['cache_misses_total','Cache misses'],['errors_total','Errors']]} /><details className="raw-metrics"><summary>Raw daemon metrics JSON</summary><pre className="result">{pretty(collectionMetrics)}</pre></details></> : <p className="muted">Metrics not loaded.</p>}
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

function DatabaseView({ connection }: { connection: Connection }) {
  const [info, setInfo] = useState<MapInfo | null>(null);
  const [size, setSize] = useState('');
  const [metricsData, setMetricsData] = useState<Record<string, unknown> | null>(null);
  const [samples, setSamples] = useState<Array<Record<string, number>>>([]);
  const [autoRefresh, setAutoRefresh] = useState(true);
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
    void load();
  }, [load]);

  useEffect(() => {
    void loadMetrics();
    if (!autoRefresh) return;
    const timer = window.setInterval(() => void loadMetrics(), 5000);
    return () => window.clearInterval(timer);
  }, [autoRefresh, loadMetrics]);

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
          <p>LMDB map, metrics, import/export, and maintenance</p>
        </div>
        <div className="actions">
          <button onClick={() => void load()}>Refresh</button>
          <button onClick={() => void loadMetrics()}>Refresh metrics</button>
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
        <div className="panel-title">
          <h3>Metrics over time</h3>
          <label className="checkbox"><input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} /> auto refresh 5s</label>
        </div>
        <div className="stat-grid">
          <div className="stat"><span>Requests</span><strong>{metricNumber(metricsData, 'requests_total').toLocaleString()}</strong></div>
          <div className="stat"><span>Read ops</span><strong>{metricNumber(metricsData, 'read_ops_total').toLocaleString()}</strong></div>
          <div className="stat"><span>Write ops</span><strong>{metricNumber(metricsData, 'write_ops_total').toLocaleString()}</strong></div>
          <div className="stat"><span>Errors</span><strong>{metricNumber(metricsData, 'errors_total').toLocaleString()}</strong></div>
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
        <div className="panel-title"><h3>Maintenance and auth (CLI)</h3></div>
        <p className="muted small">Backup, restore, verification, and auth-file management are CLI-only because the HTTP API intentionally does not expose filesystem paths or write auth credentials.</p>
        <div className="command-list">
          {[
            'nonodb-cli backup /var/lib/nonodb ./backup',
            'nonodb-cli verify-backup ./backup',
            'nonodb-cli restore ./backup /var/lib/nonodb --verify',
            'nonodb-cli auth check',
            'nonodb-cli auth token create admin',
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
  onDisconnect,
}: {
  connection: Connection;
  onDisconnect: () => void;
}) {
  const [collections, setCollections] = useState<string[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [tab, setTab] = useState<'collections' | 'sql' | 'database'>('collections');
  const [showNewCollection, setShowNewCollection] = useState(false);
  const [showPalette, setShowPalette] = useState(false);
  const [paletteQuery, setPaletteQuery] = useState('');
  const [newCollection, setNewCollection] = useState('');
  const [ttl, setTtl] = useState('');
  const [history, setHistory] = useState('0');
  const [profile, setProfile] = useState<'standard' | 'vault'>('standard');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

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
          <span className="brand-mark">N</span>
          <div>
            <strong>NonoDB</strong>
            <small>Console</small>
          </div>
        </div>
        <nav className="nav">
          <button className={tab === 'collections' ? 'active' : ''} onClick={() => setTab('collections')}>
            Collections
          </button>
          <button className={tab === 'sql' ? 'active' : ''} onClick={() => setTab('sql')}>
            SQL
          </button>
          <button className={tab === 'database' ? 'active' : ''} onClick={() => setTab('database')}>
            Database
          </button>
          {collections.includes('_jobs') && (
            <button onClick={() => { setTab('collections'); setSelected('_jobs'); }}>Jobs _jobs</button>
          )}
          {collections.includes('_events') && (
            <button onClick={() => { setTab('collections'); setSelected('_events'); }}>Outbox _events</button>
          )}
        </nav>
        {tab === 'collections' && (
          <>
            <div className="sidebar-section">
              <span>Databases</span>
              <button className="icon-button" onClick={() => void refreshCollections()} title="Refresh">
                ↻
              </button>
            </div>
            <div className="collection-list">
              {collections.map((name) => (
                <button
                  key={name}
                  className={selected === name ? 'active' : ''}
                  onClick={() => {
                    setSelected(name);
                    setTab('collections');
                  }}
                >
                  {name}
                </button>
              ))}
              {collections.length === 0 && <p className="muted small">No collections</p>}
            </div>
            <button className="primary new-collection-button" onClick={() => setShowNewCollection(true)}>
              + New collection
            </button>
          </>
        )}
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
        {tab === 'sql' ? (
          <SqlView connection={connection} />
        ) : tab === 'database' ? (
          <DatabaseView connection={connection} />
        ) : selected ? (
          <CollectionView connection={connection} name={selected} />
        ) : (
          <Dashboard
            connection={connection}
            collections={collections}
            onOpen={(name) => setSelected(name)}
            onRefresh={() => void refreshCollections()}
            onNewCollection={() => setShowNewCollection(true)}
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
              { label: 'Open Collections', run: () => { setTab('collections'); setSelected(null); } },
              { label: 'Open SQL console', run: () => setTab('sql') },
              { label: 'Open Database settings', run: () => setTab('database') },
              { label: 'New collection', run: () => setShowNewCollection(true) },
              ...collections.map((coll) => ({ label: `Open collection: ${coll}`, run: () => { setTab('collections'); setSelected(coll); } })),
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
