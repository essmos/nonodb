import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { exec, errorText, watchStream, type Connection } from './api';
import WebhooksPage from './WebhooksPage';

export type EventingTab = 'overview' | 'rules' | 'deliveries' | 'live' | 'outbox';

type WebhookRule = { name: string; coll: string; enabled?: boolean; url: string };
type Delivery = { _id: string; rule: string; status: string; created_at: number; last_error?: string | null };
type Subscription = {
  coll: string;
  name: string;
  event_type?: string;
  expires_at?: number | string;
  filter?: unknown;
  once?: boolean;
};
type Summary = {
  collections: string[];
  rules: WebhookRule[];
  deliveries: Delivery[];
  subscriptions: Subscription[];
  sources: string[];
};

const tabs: Array<{ id: EventingTab; label: string }> = [
  { id: 'overview', label: 'Overview' },
  { id: 'rules', label: 'Rules' },
  { id: 'deliveries', label: 'Deliveries' },
  { id: 'live', label: 'Live watch' },
  { id: 'outbox', label: 'Outbox' },
];

function pretty(value: unknown) {
  return JSON.stringify(value, null, 2);
}

function formatTime(value?: number | string) {
  if (!value) return '—';
  const numeric = typeof value === 'string' ? Number(value) : value;
  return Number.isFinite(numeric) ? new Date(numeric).toLocaleString() : String(value);
}

function userCollections(collections: string[]) {
  return collections.filter((name) => !name.startsWith('_'));
}

export default function EventingPage({
  connection,
  collections,
  onOpenCollection,
  initialTab = 'overview',
}: {
  connection: Connection;
  collections: string[];
  onOpenCollection: (name: string, tab?: 'events' | 'documents') => void;
  initialTab?: EventingTab;
}) {
  const [tab, setTab] = useState<EventingTab>(initialTab);
  const [summary, setSummary] = useState<Summary | null>(null);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [busy, setBusy] = useState(false);

  const [subColl, setSubColl] = useState('');
  const [subName, setSubName] = useState('');
  const [subFilter, setSubFilter] = useState('{}');
  const [subCount, setSubCount] = useState('');
  const [subOps, setSubOps] = useState('');
  const [subOnce, setSubOnce] = useState(false);
  const [subLease, setSubLease] = useState('600');

  const [liveColl, setLiveColl] = useState('');
  const [liveFilter, setLiveFilter] = useState('{}');
  const [liveEvents, setLiveEvents] = useState<Array<Record<string, unknown>>>([]);
  const [liveRunning, setLiveRunning] = useState(false);
  const [liveError, setLiveError] = useState('');
  const liveController = useRef<AbortController | null>(null);

  const users = useMemo(() => userCollections(collections), [collections]);

  const loadSummary = useCallback(async () => {
    setError('');
    try {
      const [ruleResult, deliveryResult] = await Promise.all([
        exec<{ webhooks: WebhookRule[] }>(connection, { op: 'list_webhooks' }),
        exec<{ deliveries: Delivery[] }>(connection, { op: 'list_webhook_deliveries', limit: 1000 }),
      ]);
      const subscriptionGroups = await Promise.all(
        users.map(async (coll) => {
          try {
            const result = await exec<{ subscriptions: Array<Record<string, unknown>> }>(connection, {
              op: 'list_subscriptions',
              coll,
            });
            return (result.subscriptions ?? []).map((item) => ({
              ...item,
              coll,
            })) as Subscription[];
          } catch {
            return [] as Subscription[];
          }
        }),
      );
      const sourceResults = await Promise.all(
        users.map(async (coll) => {
          try {
            const result = await exec<{ enabled: boolean }>(connection, {
              op: 'get_collection_events',
              coll,
            });
            return result.enabled ? coll : null;
          } catch {
            return null;
          }
        }),
      );
      setSummary({
        collections: users,
        rules: ruleResult.webhooks ?? [],
        deliveries: deliveryResult.deliveries ?? [],
        subscriptions: subscriptionGroups.flat(),
        sources: sourceResults.filter((name): name is string => Boolean(name)),
      });
    } catch (err) {
      setError(errorText(err));
    }
  }, [connection, users]);

  useEffect(() => {
    void loadSummary();
  }, [loadSummary]);

  useEffect(() => {
    if (!subColl || !users.includes(subColl)) setSubColl(users[0] ?? '');
    if (!liveColl || !users.includes(liveColl)) setLiveColl(users[0] ?? '');
  }, [liveColl, subColl, users]);

  useEffect(() => () => liveController.current?.abort(), []);

  useEffect(() => {
    if (!notice) return;
    const timer = window.setTimeout(() => setNotice(''), 3000);
    return () => window.clearTimeout(timer);
  }, [notice]);

  const createSubscription = async () => {
    if (!subColl || !subName.trim()) return;
    setBusy(true);
    setError('');
    try {
      const count = subCount.trim() ? JSON.parse(subCount) : undefined;
      await exec(connection, {
        op: 'create_subscription',
        coll: subColl,
        name: subName.trim(),
        filter: JSON.parse(subFilter || '{}'),
        ...(count === undefined ? {} : { count }),
        ops: subOps.split(',').map((part) => part.trim()).filter(Boolean),
        once: subOnce,
        lease_seconds: Number(subLease || 600),
      });
      setSubName('');
      setNotice('Subscription created');
      await loadSummary();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const deleteSubscription = async (subscription: Subscription) => {
    if (!window.confirm(`Delete subscription ${subscription.coll}/${subscription.name}?`)) return;
    setBusy(true);
    setError('');
    try {
      await exec(connection, {
        op: 'delete_subscription',
        coll: subscription.coll,
        name: subscription.name,
      });
      setNotice('Subscription deleted');
      await loadSummary();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const startLive = async () => {
    if (!liveColl) return;
    setLiveError('');
    setLiveEvents([]);
    setLiveRunning(true);
    const controller = new AbortController();
    liveController.current = controller;
    try {
      await watchStream(
        connection,
        {
          op: 'watch',
          coll: liveColl,
          mode: 'live',
          filter: JSON.parse(liveFilter || '{}'),
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

  const deliveryCounts = useMemo(() => {
    const counts = { pending: 0, retry: 0, delivered: 0, dead: 0 };
    for (const delivery of summary?.deliveries ?? []) {
      if (delivery.status in counts) counts[delivery.status as keyof typeof counts] += 1;
    }
    return counts;
  }, [summary]);

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <h2>Eventing</h2>
          <p>Change events, subscriptions, live watch, webhook deliveries, and the raw outbox in one place.</p>
        </div>
        <div className="actions">
          <button onClick={() => void loadSummary()}>Refresh</button>
        </div>
      </header>

      {error && <div className="error-box">{error}</div>}
      {notice && <div className="notice-box">{notice}</div>}

      <div className="tab-bar">
        {tabs.map((item) => (
          <button key={item.id} className={tab === item.id ? 'active' : ''} onClick={() => setTab(item.id)}>
            {item.label}
            {item.id === 'deliveries' && (deliveryCounts.dead > 0 || deliveryCounts.retry > 0) && (
              <span className="tab-count">{deliveryCounts.dead + deliveryCounts.retry}</span>
            )}
            {item.id === 'rules' && summary && (
              <span className="tab-count">{summary.rules.length + summary.subscriptions.length}</span>
            )}
          </button>
        ))}
      </div>

      {tab === 'overview' && (
        <>
          <section className="panel">
            <div className="stat-grid">
              <div className="stat"><span>Webhook rules</span><strong>{summary?.rules.length ?? 0}</strong></div>
              <div className="stat"><span>Subscriptions</span><strong>{summary?.subscriptions.length ?? 0}</strong></div>
              <div className="stat"><span>Event sources</span><strong>{summary?.sources.length ?? 0}</strong></div>
              <div className="stat"><span>Dead deliveries</span><strong>{deliveryCounts.dead}</strong></div>
            </div>
            <p className="muted small">
              Eventing is asynchronous: writes append to the outbox, while filters, aggregates, subscriptions, and
              HTTP delivery run outside the writer.
            </p>
          </section>

          <section className="panel">
            <div className="panel-title"><h3>Delivery health</h3><span>{summary?.deliveries.length ?? 0} total</span></div>
            <div className="stat-grid">
              <div className="stat"><span>Pending</span><strong>{deliveryCounts.pending}</strong></div>
              <div className="stat"><span>Retry</span><strong>{deliveryCounts.retry}</strong></div>
              <div className="stat"><span>Delivered</span><strong>{deliveryCounts.delivered}</strong></div>
              <div className="stat"><span>Dead</span><strong>{deliveryCounts.dead}</strong></div>
            </div>
            <div className="actions">
              <button onClick={() => setTab('deliveries')}>Open deliveries</button>
              <button onClick={() => setTab('rules')}>Open rules</button>
            </div>
          </section>

          <section className="panel">
            <div className="panel-title"><h3>Collections</h3><span>{summary?.collections.length ?? 0}</span></div>
            {!summary?.collections.length ? <p className="muted">No user collections.</p> : (
              <div className="index-list">
                {summary.collections.map((coll) => (
                  <div key={coll} className="index-row">
                    <div>
                      <strong>{coll}</strong>
                      <span className={`badge ${summary.sources.includes(coll) ? '' : 'danger-badge'}`}>
                        {summary.sources.includes(coll) ? 'events enabled' : 'events off'}
                      </span>
                      <div className="muted small">
                        {summary.subscriptions.filter((subscription) => subscription.coll === coll).length} subscriptions
                      </div>
                    </div>
                    <div className="actions">
                      <button className="small" onClick={() => onOpenCollection(coll, 'events')}>Changes</button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </section>
        </>
      )}

      {tab === 'rules' && (
        <>
          <WebhooksPage connection={connection} view="rules" />

          <section className="panel">
            <div className="panel-title">
              <h3>Subscriptions</h3>
              <button onClick={() => void loadSummary()}>Refresh</button>
            </div>
            <p className="muted small">
              Subscriptions persist matching events to <code>_events</code>. The same rules can also be managed from a
              collection's <strong>Changes</strong> tab.
            </p>
            <div className="form-grid">
              <label>Collection
                <select value={subColl} onChange={(e) => setSubColl(e.target.value)}>
                  {users.map((coll) => <option key={coll} value={coll}>{coll}</option>)}
                </select>
              </label>
              <label>Name<input value={subName} onChange={(e) => setSubName(e.target.value)} placeholder="paid-orders" /></label>
              <label>Lease seconds<input value={subLease} onChange={(e) => setSubLease(e.target.value)} /></label>
              <label>Filter<input value={subFilter} onChange={(e) => setSubFilter(e.target.value)} spellCheck={false} /></label>
              <label>Ops<input value={subOps} onChange={(e) => setSubOps(e.target.value)} placeholder="insert,update,delete" /></label>
              <label>Count condition<input value={subCount} onChange={(e) => setSubCount(e.target.value)} placeholder='{"gt":0}' spellCheck={false} /></label>
              <label className="checkbox"><input type="checkbox" checked={subOnce} onChange={(e) => setSubOnce(e.target.checked)} /> once / edge-triggered</label>
            </div>
            <div className="actions">
              <button className="primary" onClick={() => void createSubscription()} disabled={busy || !subColl || !subName.trim()}>
                Create subscription
              </button>
            </div>
            <div className="index-list">
              {(summary?.subscriptions ?? []).map((subscription) => (
                <div key={`${subscription.coll}:${subscription.name}`} className="index-row">
                  <div>
                    <strong>{subscription.name}</strong>
                    <span className="badge">{subscription.coll}</span>
                    <span className="badge">{subscription.event_type ?? `subscription.${subscription.name}`}</span>
                    <div className="muted small">expires {formatTime(subscription.expires_at)}</div>
                  </div>
                  <div className="actions">
                    <button className="small" onClick={() => onOpenCollection(subscription.coll, 'events')}>Open Changes</button>
                    <button className="danger small" onClick={() => void deleteSubscription(subscription)} disabled={busy}>Delete</button>
                  </div>
                </div>
              ))}
              {!summary?.subscriptions.length && <p className="muted">No subscriptions.</p>}
            </div>
          </section>
        </>
      )}

      {tab === 'deliveries' && <WebhooksPage connection={connection} view="deliveries" />}

      {tab === 'live' && (
        <section className="panel">
          <div className="panel-title">
            <h3>Live watch</h3>
            <div className="actions">
              <button onClick={() => void startLive()} disabled={liveRunning || !liveColl}>Start live</button>
              <button className="danger" onClick={stopLive} disabled={!liveRunning}>Stop</button>
            </div>
          </div>
          <p className="muted small">
            Connection-scoped live stream. Events are pushed directly and are not persisted to <code>_events</code>.
          </p>
          <div className="form-grid">
            <label>Collection
              <select value={liveColl} onChange={(e) => setLiveColl(e.target.value)} disabled={liveRunning}>
                {users.map((coll) => <option key={coll} value={coll}>{coll}</option>)}
              </select>
            </label>
            <label>Filter<input value={liveFilter} onChange={(e) => setLiveFilter(e.target.value)} spellCheck={false} /></label>
          </div>
          {liveError && <div className="error-box">{liveError}</div>}
          <div className="live-events">
            {liveEvents.length === 0 ? (
              <p className="muted">{liveRunning ? 'Waiting for matching writes…' : 'Not running.'}</p>
            ) : (
              liveEvents.map((event, index) => (
                <pre key={`${String(event._id ?? index)}-${index}`} className="result live-event">{pretty(event)}</pre>
              ))
            )}
          </div>
        </section>
      )}

      {tab === 'outbox' && (
        <section className="panel">
          <div className="panel-title"><h3>Raw outbox</h3><span><code>_events</code></span></div>
          <p className="muted small">
            The raw transactional outbox. Use it for inspection, replay, and migration—not for normal rule management.
            Webhook rules and subscriptions evaluate their own filters outside the writer.
          </p>
          {collections.includes('_events') ? (
            <div className="actions">
              <button className="primary" onClick={() => onOpenCollection('_events', 'documents')}>Open _events collection</button>
            </div>
          ) : (
            <p className="muted">The <code>_events</code> collection does not exist yet. Enable automatic events or create a subscription first.</p>
          )}
        </section>
      )}
    </div>
  );
}
