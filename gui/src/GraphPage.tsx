import { useEffect, useMemo, useState } from 'react';
import { exec, errorText, type Connection } from './api';

type Doc = Record<string, unknown>;

type GraphNode = {
  node: Doc;
  depth: number;
  via?: Doc;
  cycle?: boolean;
  children?: GraphNode[];
};

type GraphDef = {
  node_collection: string;
  edge_collection: string;
  from_field: string;
  to_field: string;
  label_field: string;
  weight_field: string;
  directed: boolean;
};

function idOf(node: Doc): string {
  const id = node._id;
  return typeof id === 'string' ? id : JSON.stringify(id);
}

function labelOf(node: Doc): string {
  for (const key of ['name', 'title', 'label']) {
    const value = node[key];
    if (typeof value === 'string' && value) return value;
  }
  return idOf(node);
}

function GraphNodeView({
  connection,
  collection,
  entry,
  direction,
  labels,
  includeEdges,
  onError,
}: {
  connection: Connection;
  collection: string;
  entry: GraphNode;
  direction: string;
  labels: string;
  includeEdges: boolean;
  onError: (message: string) => void;
}) {
  const [children, setChildren] = useState<GraphNode[]>(entry.children ?? []);
  const [expanded, setExpanded] = useState((entry.children?.length ?? 0) > 0);
  const [busy, setBusy] = useState(false);
  const [truncated, setTruncated] = useState(false);
  const node = entry.node;
  const missing = node.missing === true;

  useEffect(() => {
    setChildren(entry.children ?? []);
    setExpanded((entry.children?.length ?? 0) > 0);
    setTruncated(false);
  }, [entry]);

  const expand = async () => {
    if (entry.cycle || missing) return;
    setBusy(true);
    try {
      const result = await exec<{ roots: GraphNode[]; truncated?: boolean }>(connection, {
        op: 'traverse',
        coll: collection,
        start: [node._id],
        direction,
        labels: labels
          .split(',')
          .map((item) => item.trim())
          .filter(Boolean),
        max_depth: 1,
        tree: true,
        include_edges: includeEdges,
      });
      setChildren(result.roots?.[0]?.children ?? []);
      setTruncated(Boolean(result.truncated));
      setExpanded(true);
    } catch (err) {
      onError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <li style={{ margin: '4px 0', listStyle: 'none' }}>
      <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
        <button
          className="small"
          style={{ minWidth: 28 }}
          disabled={busy || entry.cycle || missing}
          onClick={() => void expand()}
          title={entry.cycle ? 'cycle' : missing ? 'missing node' : 'expand one level'}
        >
          {entry.cycle ? '↻' : missing ? '?' : expanded ? '−' : '+'}
        </button>
        <code>{idOf(node)}</code>
        <span>{labelOf(node)}</span>
        {entry.via && (
          <span className="muted small">
            via {String(entry.via.label ?? '')}
            {typeof entry.via.weight === 'number' ? ` · ${entry.via.weight}` : ''}
          </span>
        )}
        {entry.cycle && <span className="muted small">cycle</span>}
        {missing && <span className="muted small">missing</span>}
      </div>
      {expanded && children.length > 0 && (
        <ul style={{ margin: '2px 0 2px 18px', padding: 0, borderLeft: '1px solid var(--border, #444)' }}>
          {children.map((child, index) => (
            <GraphNodeView
              key={`${idOf(child.node)}:${index}`}
              connection={connection}
              collection={collection}
              entry={child}
              direction={direction}
              labels={labels}
              includeEdges={includeEdges}
              onError={onError}
            />
          ))}
        </ul>
      )}
      {truncated && <div className="muted small">children truncated by server limits</div>}
    </li>
  );
}

export default function GraphPage({
  connection,
  collections,
}: {
  connection: Connection;
  collections: string[];
}) {
  const users = useMemo(() => collections.filter((name) => !name.startsWith('_')), [collections]);
  const [collection, setCollection] = useState('');
  const [def, setDef] = useState<GraphDef | null>(null);
  const [edgeCollection, setEdgeCollection] = useState('');
  const [fromField, setFromField] = useState('from');
  const [toField, setToField] = useState('to');
  const [labelField, setLabelField] = useState('label');
  const [weightField, setWeightField] = useState('weight');
  const [directed, setDirected] = useState(true);
  const [start, setStart] = useState('');
  const [direction, setDirection] = useState('out');
  const [labels, setLabels] = useState('');
  const [maxDepth, setMaxDepth] = useState('1');
  const [algorithm, setAlgorithm] = useState('bfs');
  const [includeEdges, setIncludeEdges] = useState(false);
  const [roots, setRoots] = useState<GraphNode[]>([]);
  const [summary, setSummary] = useState<string>('');
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (!collection && users.length > 0) setCollection(users[0]);
  }, [collection, users]);

  useEffect(() => {
    if (!collection) return;
    let cancelled = false;
    exec<{ graph: GraphDef | null }>(connection, { op: 'get_graph', coll: collection })
      .then((result) => {
        if (cancelled) return;
        setDef(result.graph);
        if (result.graph) {
          setEdgeCollection(result.graph.edge_collection);
          setFromField(result.graph.from_field);
          setToField(result.graph.to_field);
          setLabelField(result.graph.label_field);
          setWeightField(result.graph.weight_field);
          setDirected(result.graph.directed);
        }
      })
      .catch((err) => {
        if (!cancelled) setError(errorText(err));
      });
    return () => {
      cancelled = true;
    };
  }, [collection, connection]);

  const saveGraph = async () => {
    if (!collection || !edgeCollection) return;
    setBusy(true);
    setError('');
    try {
      const result = await exec<{ graph: GraphDef }>(connection, {
        op: 'set_graph',
        coll: collection,
        def: {
          node_collection: collection,
          edge_collection: edgeCollection,
          from_field: fromField,
          to_field: toField,
          label_field: labelField,
          weight_field: weightField,
          directed,
        },
      });
      setDef(result.graph);
      setNotice('Graph configured and adjacency rebuilt.');
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const dropGraph = async () => {
    if (!collection) return;
    setBusy(true);
    setError('');
    try {
      await exec(connection, { op: 'drop_graph', coll: collection });
      setDef(null);
      setRoots([]);
      setSummary('');
      setNotice('Graph definition removed.');
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const reindex = async () => {
    if (!collection) return;
    setBusy(true);
    setError('');
    try {
      const result = await exec<{ edges: number }>(connection, {
        op: 'reindex_graph',
        coll: collection,
      });
      setNotice(`Adjacency rebuilt from ${result.edges} edge documents.`);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  const traverse = async () => {
    if (!collection || !start.trim()) return;
    setBusy(true);
    setError('');
    try {
      const result = await exec<{
        roots?: GraphNode[];
        nodes: number;
        edges: number;
        truncated: boolean;
        missing: unknown[];
      }>(connection, {
        op: 'traverse',
        coll: collection,
        start: start
          .split(',')
          .map((item) => item.trim())
          .filter(Boolean),
        direction,
        labels: labels
          .split(',')
          .map((item) => item.trim())
          .filter(Boolean),
        max_depth: Number(maxDepth) || 1,
        algorithm,
        tree: true,
        include_edges: includeEdges,
      });
      setRoots(result.roots ?? []);
      setSummary(
        `${result.nodes} nodes · ${result.edges} edges${result.truncated ? ' · truncated' : ''}${
          result.missing.length ? ` · missing ${result.missing.length}` : ''
        }`,
      );
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="view">
      <div className="page-head">
        <div>
          <h1>Graph</h1>
          <p className="muted">
            Traverse relationships between ordinary documents. Expand nodes lazily; every
            request runs in one consistent read snapshot.
          </p>
        </div>
      </div>
      {error && <div className="error-box">{error}</div>}
      {notice && <div className="notice-box">{notice}</div>}

      <section className="panel">
        <h2>Graph definition</h2>
        <div className="form-grid">
          <label>
            Node collection
            <select value={collection} onChange={(e) => setCollection(e.target.value)}>
              {users.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </label>
          <label>
            Edge collection
            <select value={edgeCollection} onChange={(e) => setEdgeCollection(e.target.value)}>
              <option value="">select…</option>
              {users.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </label>
          <label>
            From field
            <input value={fromField} onChange={(e) => setFromField(e.target.value)} />
          </label>
          <label>
            To field
            <input value={toField} onChange={(e) => setToField(e.target.value)} />
          </label>
          <label>
            Label field
            <input value={labelField} onChange={(e) => setLabelField(e.target.value)} />
          </label>
          <label>
            Weight field
            <input value={weightField} onChange={(e) => setWeightField(e.target.value)} />
          </label>
          <label>
            Directed
            <select value={directed ? 'yes' : 'no'} onChange={(e) => setDirected(e.target.value === 'yes')}>
              <option value="yes">yes</option>
              <option value="no">no (both directions)</option>
            </select>
          </label>
        </div>
        <div className="panel-actions">
          <button className="primary" disabled={busy || !edgeCollection} onClick={() => void saveGraph()}>
            {def ? 'Update graph' : 'Configure graph'}
          </button>
          {def && (
            <button disabled={busy} onClick={() => void reindex()}>
              Reindex adjacency
            </button>
          )}
          {def && (
            <button className="danger" disabled={busy} onClick={() => void dropGraph()}>
              Drop graph
            </button>
          )}
        </div>
      </section>

      <section className="panel">
        <h2>Traverse</h2>
        <div className="form-grid">
          <label>
            Start ids (comma separated)
            <input value={start} onChange={(e) => setStart(e.target.value)} placeholder="user-1" />
          </label>
          <label>
            Direction
            <select value={direction} onChange={(e) => setDirection(e.target.value)}>
              <option value="out">out</option>
              <option value="in">in</option>
              <option value="both">both</option>
            </select>
          </label>
          <label>
            Labels (comma separated)
            <input value={labels} onChange={(e) => setLabels(e.target.value)} placeholder="KNOWS, WORKS_AT" />
          </label>
          <label>
            Initial depth
            <input type="number" min={0} max={64} value={maxDepth} onChange={(e) => setMaxDepth(e.target.value)} />
          </label>
          <label>
            Algorithm
            <select value={algorithm} onChange={(e) => setAlgorithm(e.target.value)}>
              <option value="bfs">bfs</option>
              <option value="dfs">dfs</option>
            </select>
          </label>
          <label>
            Include full edge
            <select value={includeEdges ? 'yes' : 'no'} onChange={(e) => setIncludeEdges(e.target.value === 'yes')}>
              <option value="no">no (label + weight)</option>
              <option value="yes">yes</option>
            </select>
          </label>
        </div>
        <div className="panel-actions">
          <button className="primary" disabled={busy || !start.trim() || !def} onClick={() => void traverse()}>
            Traverse
          </button>
          <button disabled={busy || roots.length === 0} onClick={() => setRoots([])}>
            Clear
          </button>
        </div>
        {summary && <p className="muted small">{summary}</p>}
        {roots.length > 0 && (
          <ul style={{ margin: 0, padding: 0 }}>
            {roots.map((root, index) => (
              <GraphNodeView
                key={`${idOf(root.node)}:${index}`}
                connection={connection}
                collection={collection}
                entry={root}
                direction={direction}
                labels={labels}
                includeEdges={includeEdges}
                onError={setError}
              />
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
