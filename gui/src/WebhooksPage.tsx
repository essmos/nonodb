import { useCallback, useEffect, useState } from 'react';
import { exec, errorText, type Connection } from './api';

type Threshold = {
  aggregate: 'count' | 'sum' | 'avg';
  field?: string;
  filter?: unknown;
  gt?: number; gte?: number; lt?: number; lte?: number; eq?: number;
};
type WebhookRule = {
  name: string; coll: string; url: string; enabled?: boolean; ops?: string[];
  filter?: unknown; threshold?: Threshold | null; once?: boolean; cooldown_seconds?: number;
  secret_env?: string | null; headers?: Record<string, string>; max_attempts?: number; timeout_ms?: number;
};
type Delivery = {
  _id: string; rule: string; url: string; status: string; attempts: number;
  max_attempts: number; next_attempt_ms: number; created_at: number;
  response_status?: number | null; last_error?: string | null; payload?: unknown;
};

function formatTime(ms?: number) {
  return ms ? new Date(ms).toLocaleString() : '—';
}

export default function WebhooksPage({ connection, view = 'all' }: { connection: Connection; view?: 'all' | 'rules' | 'deliveries' }) {
  const [rules, setRules] = useState<WebhookRule[]>([]);
  const [deliveries, setDeliveries] = useState<Delivery[]>([]);
  const [status, setStatus] = useState('');
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [busy, setBusy] = useState(false);
  const [form, setForm] = useState({
    name: '', coll: '', url: '', filter: '{}', threshold: '', ops: '',
    once: false, cooldown: '0', secret_env: '', max_attempts: '8', timeout_ms: '10000', headers: '{}', enabled: true,
  });

  const load = useCallback(async () => {
    setError('');
    try {
      const [ruleView, deliveryView] = await Promise.all([
        exec<{ webhooks: WebhookRule[] }>(connection, { op: 'list_webhooks' }),
        exec<{ deliveries: Delivery[] }>(connection, {
          op: 'list_webhook_deliveries',
          status: status || undefined,
          limit: 200,
        }),
      ]);
      setRules(ruleView.webhooks ?? []);
      setDeliveries(deliveryView.deliveries ?? []);
    } catch (err) {
      setError(errorText(err));
    }
  }, [connection, status]);

  useEffect(() => { void load(); }, [load]);

  const save = async () => {
    setBusy(true); setError(''); setNotice('');
    try {
      if (!form.name.trim() || !form.coll.trim() || !form.url.trim()) {
        throw new Error('Name, collection, and URL are required.');
      }
      const filter = JSON.parse(form.filter || '{}');
      const threshold = form.threshold.trim() ? JSON.parse(form.threshold) : undefined;
      await exec(connection, {
        op: 'create_webhook',
        name: form.name.trim(), coll: form.coll.trim(), url: form.url.trim(),
        filter, threshold, enabled: form.enabled, once: form.once,
        cooldown_seconds: Number(form.cooldown || 0),
        max_attempts: Number(form.max_attempts || 8), timeout_ms: Number(form.timeout_ms || 10000),
        secret_env: form.secret_env.trim() || undefined,
        headers: JSON.parse(form.headers || '{}'),
        ops: form.ops.split(',').map((item) => item.trim()).filter(Boolean),
      });
      setNotice(`Saved webhook ${form.name}.`);
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally { setBusy(false); }
  };

  const remove = async (name: string) => {
    setBusy(true); setError('');
    try {
      await exec(connection, { op: 'delete_webhook', name });
      await load();
    } catch (err) { setError(errorText(err)); } finally { setBusy(false); }
  };

  const retry = async (id: string) => {
    setBusy(true); setError('');
    try {
      await exec(connection, { op: 'retry_webhook_delivery', id });
      setNotice('Delivery queued for retry.');
      await load();
    } catch (err) { setError(errorText(err)); } finally { setBusy(false); }
  };

  const edit = (rule: WebhookRule) => {
    setForm({
      name: rule.name, coll: rule.coll, url: rule.url,
      filter: JSON.stringify(rule.filter ?? {}, null, 2),
      threshold: rule.threshold ? JSON.stringify(rule.threshold, null, 2) : '',
      ops: (rule.ops ?? []).join(','), once: Boolean(rule.once),
      cooldown: String(rule.cooldown_seconds ?? 0), secret_env: rule.secret_env ?? '',
      max_attempts: String(rule.max_attempts || 8), timeout_ms: String(rule.timeout_ms || 10000),
      headers: JSON.stringify(rule.headers ?? {}, null, 2), enabled: rule.enabled !== false,
    });
  };

  return (
    <div className={view === 'all' ? 'page' : ''}>
      {view !== 'deliveries' && (
        <>
          <header className="page-header">
            <div>
              <h2>Webhooks</h2>
              <p>Asynchronous HTTP delivery from change events. Filters and aggregates run outside the writer.</p>
            </div>
            <div className="actions"><button onClick={() => void load()} disabled={busy}>Refresh</button></div>
          </header>
          {error && <div className="error-box">{error}</div>}
          {notice && <div className="notice-box">{notice}</div>}

          <section className="panel">
            <div className="panel-title"><h3>Rules</h3><span>{rules.length}</span></div>
            {rules.length === 0 ? <p className="muted">No webhook rules.</p> : (
              <div className="index-list">
                {rules.map((rule) => (
                  <div key={rule.name} className="index-row">
                    <div>
                      <strong>{rule.name}</strong>
                      <span className={`badge ${rule.enabled === false ? 'danger-badge' : ''}`}>
                        {rule.enabled === false ? 'disabled' : 'enabled'}
                      </span>
                      <div className="muted small"><code>{rule.coll}</code> → <code>{rule.url}</code></div>
                      <div className="muted small">
                        {rule.threshold
                          ? `${rule.threshold.aggregate}${rule.threshold.field ? `(${rule.threshold.field})` : ''} threshold`
                          : 'event filter'} · {rule.once ? 'once · ' : ''}cooldown {rule.cooldown_seconds ?? 0}s
                      </div>
                    </div>
                    <div className="actions">
                      <button className="small" onClick={() => edit(rule)} disabled={busy}>Edit</button>
                      <button className="small" onClick={() => void remove(rule.name)} disabled={busy}>Delete</button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </section>

          <section className="panel">
            <div className="panel-title"><h3>{form.name ? `Edit ${form.name}` : 'New webhook'}</h3></div>
            <div className="form-grid">
              <label>Name<input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} /></label>
              <label>Collection<input value={form.coll} onChange={(e) => setForm({ ...form, coll: e.target.value })} /></label>
              <label>URL<input value={form.url} onChange={(e) => setForm({ ...form, url: e.target.value })} placeholder="https://example.test/hook" /></label>
              <label>Ops<input value={form.ops} onChange={(e) => setForm({ ...form, ops: e.target.value })} placeholder="insert,update,delete" /></label>
              <label>Cooldown seconds<input type="number" min={0} value={form.cooldown} onChange={(e) => setForm({ ...form, cooldown: e.target.value })} /></label>
              <label>Max attempts<input type="number" min={1} value={form.max_attempts} onChange={(e) => setForm({ ...form, max_attempts: e.target.value })} /></label>
              <label>Timeout (ms)<input type="number" min={1} value={form.timeout_ms} onChange={(e) => setForm({ ...form, timeout_ms: e.target.value })} /></label>
              <label>Secret env var<input value={form.secret_env} onChange={(e) => setForm({ ...form, secret_env: e.target.value })} placeholder="WEBHOOK_SECRET" /></label>
            <label>Headers (JSON)<textarea rows={3} value={form.headers} onChange={(e) => setForm({ ...form, headers: e.target.value })} spellCheck={false} /></label>
              <label className="checkbox"><input type="checkbox" checked={form.once} onChange={(e) => setForm({ ...form, once: e.target.checked })} /> Edge-triggered (once)</label>
              <label className="checkbox"><input type="checkbox" checked={form.enabled} onChange={(e) => setForm({ ...form, enabled: e.target.checked })} /> Enabled</label>
            </div>
            <label>Event filter (JSON)<textarea rows={5} value={form.filter} onChange={(e) => setForm({ ...form, filter: e.target.value })} /></label>
            <label>Threshold (optional JSON: aggregate, field, filter, gt/gte/lt/lte/eq)<textarea rows={5} value={form.threshold} onChange={(e) => setForm({ ...form, threshold: e.target.value })} placeholder='{"aggregate":"sum","field":"total","gte":1000,"filter":{"status":"paid"}}' /></label>
            <div className="actions"><button className="primary" onClick={() => void save()} disabled={busy}>{busy ? 'Saving…' : 'Save webhook'}</button></div>
          </section>
        </>
      )}

      {view !== 'rules' && (
        <section className="panel">
          {view === 'deliveries' && <div className="panel-title"><h3>Webhook deliveries</h3><button onClick={() => void load()} disabled={busy}>Refresh</button></div>}
          {view === 'deliveries' && error && <div className="error-box">{error}</div>}
          {view === 'deliveries' && notice && <div className="notice-box">{notice}</div>}
          <div className="panel-title">
            {view === 'all' && <h3>Deliveries</h3>}
            <select value={status} onChange={(e) => setStatus(e.target.value)}>
              <option value="">all</option><option value="pending">pending</option><option value="retry">retry</option>
              <option value="delivered">delivered</option><option value="dead">dead</option>
            </select>
          </div>
          <div className="metric-table-wrap">
            <table className="metric-table">
              <thead><tr><th>Rule</th><th>Status</th><th>Attempts</th><th>Created</th><th>Response</th><th>Error</th><th /></tr></thead>
              <tbody>
                {deliveries.map((delivery) => (
                  <tr key={delivery._id}>
                    <td>{delivery.rule}</td>
                    <td><span className={`badge ${delivery.status === 'dead' ? 'danger-badge' : ''}`}>{delivery.status}</span></td>
                    <td>{delivery.attempts}/{delivery.max_attempts}</td>
                    <td>{formatTime(delivery.created_at)}</td>
                    <td>{delivery.response_status ?? '—'}</td>
                    <td className="muted small">{delivery.last_error ?? ''}</td>
                    <td><button className="small" onClick={() => void retry(delivery._id)} disabled={busy || delivery.status === 'pending'}>Retry</button></td>
                  </tr>
                ))}
                {deliveries.length === 0 && <tr><td colSpan={7} className="muted">No deliveries.</td></tr>}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </div>
  );
}
