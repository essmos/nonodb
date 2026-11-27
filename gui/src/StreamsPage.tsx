import { useCallback, useEffect, useState } from 'react';
import { exec, errorText, type Connection } from './api';

type Retention = {
  max_events?: number;
  max_age_ms?: number;
  min_seq_keep?: number;
};
type GroupInfo = {
  group: string;
  committed_seq: number;
  next_seq: number;
  head_seq: number;
  lag: number;
  inflight: number;
  dead: number;
  max_attempts: number;
  truncated_below?: number | null;
  updated_at?: number;
};
type StreamStats = {
  stream: string;
  coll: string;
  head_seq: number;
  retention: Retention;
  groups: GroupInfo[];
};
type DeadLetter = {
  _id: string;
  stream: string;
  group: string;
  from_seq: number;
  to_seq: number;
  attempts: number;
  max_attempts: number;
  error?: string | null;
  updated_at?: number;
};

function retentionText(retention: Retention) {
  const parts = [
    retention.max_events ? `max_events=${retention.max_events}` : '',
    retention.max_age_ms ? `max_age_ms=${retention.max_age_ms}` : '',
    retention.min_seq_keep ? `min_seq_keep=${retention.min_seq_keep}` : '',
  ].filter(Boolean);
  return parts.length ? parts.join(' ') : 'no retention';
}

export default function StreamsPage({ connection }: { connection: Connection }) {
  const [streams, setStreams] = useState<StreamStats[]>([]);
  const [dead, setDead] = useState<DeadLetter[]>([]);
  const [deadFilter, setDeadFilter] = useState('');
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [busy, setBusy] = useState(false);
  const [streamForm, setStreamForm] = useState({
    coll: '',
    max_events: '',
    max_age_ms: '',
    min_seq_keep: '',
  });
  const [groupForm, setGroupForm] = useState({
    coll: '',
    group: '',
    start: 'latest',
    max_attempts: '5',
  });

  const load = useCallback(async () => {
    setError('');
    try {
      const stats = await exec<{ streams: StreamStats[] }>(connection, {
        op: 'stream_stats',
        coll: deadFilter || undefined,
      });
      setStreams(stats.streams ?? []);
      if (deadFilter) {
        const letters = await exec<{ dead: DeadLetter[] }>(connection, {
          op: 'stream_dead',
          coll: deadFilter,
        });
        setDead(letters.dead ?? []);
      } else {
        setDead([]);
      }
    } catch (err) {
      setError(errorText(err));
    }
  }, [connection, deadFilter]);

  useEffect(() => {
    void load();
  }, [load]);

  const createStream = async () => {
    setBusy(true);
    setError('');
    setNotice('');
    try {
      if (!streamForm.coll.trim()) throw new Error('Collection is required.');
      const retention: Retention = {};
      if (streamForm.max_events) retention.max_events = Number(streamForm.max_events);
      if (streamForm.max_age_ms) retention.max_age_ms = Number(streamForm.max_age_ms);
      if (streamForm.min_seq_keep) retention.min_seq_keep = Number(streamForm.min_seq_keep);
      await exec(connection, {
        op: 'create_stream',
        coll: streamForm.coll.trim(),
        retention,
      });
      setNotice(`Stream created on ${streamForm.coll}.`);
      setStreamForm({ coll: '', max_events: '', max_age_ms: '', min_seq_keep: '' });
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const createGroup = async () => {
    setBusy(true);
    setError('');
    setNotice('');
    try {
      if (!groupForm.coll.trim() || !groupForm.group.trim()) {
        throw new Error('Collection and group are required.');
      }
      const start =
        groupForm.start === 'earliest' || groupForm.start === 'latest'
          ? groupForm.start
          : { seq: Number(groupForm.start) };
      await exec(connection, {
        op: 'create_stream_group',
        coll: groupForm.coll.trim(),
        group: groupForm.group.trim(),
        start,
        max_attempts: Number(groupForm.max_attempts || 5),
      });
      setNotice(`Group ${groupForm.group} created.`);
      setGroupForm({ coll: '', group: '', start: 'latest', max_attempts: '5' });
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const reset = async (stream: string, group: string, to: string) => {
    setBusy(true);
    setError('');
    try {
      await exec(connection, {
        op: 'reset_stream_group',
        coll: stream,
        group,
        to,
      });
      setNotice(`Group ${group} reset to ${to}.`);
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const deleteGroup = async (stream: string, group: string) => {
    setBusy(true);
    setError('');
    try {
      await exec(connection, { op: 'delete_stream_group', coll: stream, group });
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const prune = async (stream: string) => {
    setBusy(true);
    setError('');
    try {
      const out = await exec<{ deleted: number }>(connection, {
        op: 'prune_stream',
        coll: stream,
      });
      setNotice(`Pruned ${out.deleted} events from ${stream}.`);
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const dropDead = async (letter: DeadLetter, retry: boolean) => {
    setBusy(true);
    setError('');
    try {
      await exec(connection, {
        op: retry ? 'retry_stream_dead' : 'drop_stream_dead',
        coll: letter.stream,
        group: letter.group,
        from_seq: letter.from_seq,
      });
      await load();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const groups = streams.flatMap((stream) =>
    (stream.groups ?? []).map((group) => ({ stream, group })),
  );
  const totals = groups.reduce(
    (acc, row) => ({
      lag: acc.lag + row.group.lag,
      inflight: acc.inflight + row.group.inflight,
      dead: acc.dead + row.group.dead,
    }),
    { lag: 0, inflight: 0, dead: 0 },
  );

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <h2>Streams</h2>
          <p>
            Append-only collections with server-side offsets and consumer groups. A
            group stops retention only as far as its committed offset; a paused member
            shows up as lag and in-flight events.
          </p>
        </div>
        <div className="actions">
          <button onClick={() => void load()} disabled={busy}>
            Refresh
          </button>
        </div>
      </header>
      {error && <div className="error-box">{error}</div>}
      {notice && <div className="notice-box">{notice}</div>}

      <section className="panel">
        <div className="panel-title">
          <h3>Consumer groups</h3>
          <span>
            lag {totals.lag} · in-flight {totals.inflight} · dead {totals.dead}
          </span>
        </div>
        {groups.length === 0 ? (
          <p className="muted">No streams or groups yet.</p>
        ) : (
          <div className="metric-table-wrap">
            <table className="metric-table">
              <thead>
                <tr>
                  <th>Stream</th>
                  <th>Group</th>
                  <th>Committed</th>
                  <th>Head</th>
                  <th>Lag</th>
                  <th>In-flight</th>
                  <th>Dead</th>
                  <th>Attempts</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {groups.map(({ stream, group }) => (
                  <tr key={`${stream.stream}:${group.group}`}>
                    <td>
                      <code>{stream.stream}</code>
                    </td>
                    <td>
                      <strong>{group.group}</strong>
                      {group.truncated_below ? (
                        <span className="badge danger-badge">
                          truncated below {group.truncated_below}
                        </span>
                      ) : null}
                    </td>
                    <td>{group.committed_seq}</td>
                    <td>{group.head_seq}</td>
                    <td>{group.lag}</td>
                    <td>{group.inflight}</td>
                    <td>{group.dead}</td>
                    <td>
                      {group.max_attempts}
                      {group.inflight > 0 ? ' · leased' : ''}
                    </td>
                    <td>
                      <div className="actions">
                        <button
                          className="small"
                          onClick={() => void reset(stream.stream, group.group, 'earliest')}
                          disabled={busy}
                        >
                          Reset earliest
                        </button>
                        <button
                          className="small"
                          onClick={() => void reset(stream.stream, group.group, 'latest')}
                          disabled={busy}
                        >
                          Reset latest
                        </button>
                        <button
                          className="small"
                          onClick={() => void deleteGroup(stream.stream, group.group)}
                          disabled={busy}
                        >
                          Delete
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="panel">
        <div className="panel-title">
          <h3>Streams</h3>
          <span>{streams.length}</span>
        </div>
        {streams.length === 0 ? (
          <p className="muted">No streams defined.</p>
        ) : (
          <div className="index-list">
            {streams.map((stream) => (
              <div key={stream.stream} className="index-row">
                <div>
                  <strong>{stream.stream}</strong>
                  <div className="muted small">
                    <code>{stream.coll}</code> · head {stream.head_seq} ·{' '}
                    {stream.groups.length} group(s) · {retentionText(stream.retention)}
                  </div>
                </div>
                <div className="actions">
                  <button className="small" onClick={() => void prune(stream.stream)} disabled={busy}>
                    Prune now
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="panel">
        <div className="panel-title">
          <h3>Dead letters</h3>
          <input
            value={deadFilter}
            onChange={(e) => setDeadFilter(e.target.value)}
            placeholder="collection"
            aria-label="Collection filter for dead letters"
          />
        </div>
        {!deadFilter ? (
          <p className="muted">Enter a collection to list its dead-lettered batches.</p>
        ) : dead.length === 0 ? (
          <p className="muted">No dead letters.</p>
        ) : (
          <div className="metric-table-wrap">
            <table className="metric-table">
              <thead>
                <tr>
                  <th>Group</th>
                  <th>Range</th>
                  <th>Attempts</th>
                  <th>Error</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {dead.map((letter) => (
                  <tr key={letter._id}>
                    <td>{letter.group}</td>
                    <td>
                      {letter.from_seq}–{letter.to_seq}
                    </td>
                    <td>
                      {letter.attempts}/{letter.max_attempts}
                    </td>
                    <td className="muted small">{letter.error ?? ''}</td>
                    <td>
                      <div className="actions">
                        <button
                          className="small"
                          onClick={() => void dropDead(letter, true)}
                          disabled={busy}
                        >
                          Retry
                        </button>
                        <button
                          className="small"
                          onClick={() => void dropDead(letter, false)}
                          disabled={busy}
                        >
                          Drop
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="panel">
        <div className="panel-title">
          <h3>Define a stream</h3>
        </div>
        <div className="form-grid">
          <label>
            Collection
            <input
              value={streamForm.coll}
              onChange={(e) => setStreamForm({ ...streamForm, coll: e.target.value })}
            />
          </label>
          <label>
            Max events
            <input
              type="number"
              min={1}
              value={streamForm.max_events}
              onChange={(e) => setStreamForm({ ...streamForm, max_events: e.target.value })}
              placeholder="unset"
            />
          </label>
          <label>
            Max age (ms)
            <input
              type="number"
              min={1}
              value={streamForm.max_age_ms}
              onChange={(e) => setStreamForm({ ...streamForm, max_age_ms: e.target.value })}
              placeholder="unset"
            />
          </label>
          <label>
            Min seq keep
            <input
              type="number"
              min={0}
              value={streamForm.min_seq_keep}
              onChange={(e) => setStreamForm({ ...streamForm, min_seq_keep: e.target.value })}
              placeholder="0"
            />
          </label>
        </div>
        <div className="actions">
          <button className="primary" onClick={() => void createStream()} disabled={busy}>
            Create stream
          </button>
        </div>
      </section>

      <section className="panel">
        <div className="panel-title">
          <h3>Create a group</h3>
        </div>
        <div className="form-grid">
          <label>
            Collection
            <input
              value={groupForm.coll}
              onChange={(e) => setGroupForm({ ...groupForm, coll: e.target.value })}
            />
          </label>
          <label>
            Group
            <input
              value={groupForm.group}
              onChange={(e) => setGroupForm({ ...groupForm, group: e.target.value })}
            />
          </label>
          <label>
            Start
            <select
              value={groupForm.start}
              onChange={(e) => setGroupForm({ ...groupForm, start: e.target.value })}
            >
              <option value="latest">latest</option>
              <option value="earliest">earliest</option>
            </select>
          </label>
          <label>
            Max attempts
            <input
              type="number"
              min={1}
              value={groupForm.max_attempts}
              onChange={(e) => setGroupForm({ ...groupForm, max_attempts: e.target.value })}
            />
          </label>
        </div>
        <div className="actions">
          <button className="primary" onClick={() => void createGroup()} disabled={busy}>
            Create group
          </button>
        </div>
      </section>
    </div>
  );
}
