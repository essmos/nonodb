import { useMemo, useState } from 'react';

type ValueType = 'string' | 'number' | 'boolean' | 'null' | 'json';
type Logic = 'and' | 'or';

type Condition = {
  id: string;
  kind: 'condition';
  field: string;
  op: string;
  value: string;
  valueType: ValueType;
};

type Group = {
  id: string;
  kind: 'group';
  logic: Logic;
  children: Node[];
};

type Node = Condition | Group;

const OPERATORS: Array<[string, string]> = [
  ['eq', 'equals'],
  ['ne', 'not equals'],
  ['gt', '>'],
  ['gte', '>='],
  ['lt', '<'],
  ['lte', '<='],
  ['in', 'in'],
  ['nin', 'not in'],
  ['exists', 'exists'],
  ['regex', 'regex'],
  ['size', 'array size'],
  ['all', 'array all'],
  ['elemMatch', 'elem match'],
  ['type', 'BSON type'],
];

let nextId = 1;
function id(): string {
  nextId += 1;
  return `qb-${Date.now()}-${nextId}`;
}

function newCondition(): Condition {
  return { id: id(), kind: 'condition', field: '', op: 'eq', value: '', valueType: 'string' };
}

function newGroup(logic: Logic = 'and'): Group {
  return { id: id(), kind: 'group', logic, children: [newCondition()] };
}

function valueTypeOf(value: unknown): ValueType {
  if (value === null) return 'null';
  if (typeof value === 'number') return 'number';
  if (typeof value === 'boolean') return 'boolean';
  if (typeof value === 'string') return 'string';
  return 'json';
}

function valueText(value: unknown): string {
  return value === null ? '' : typeof value === 'string' ? value : JSON.stringify(value);
}

function parseConditionValue(condition: Condition): unknown {
  if (condition.op === 'exists') return condition.value === 'true';
  switch (condition.valueType) {
    case 'string':
      return condition.value;
    case 'number':
      return Number(condition.value);
    case 'boolean':
      return condition.value === 'true';
    case 'null':
      return null;
    case 'json':
      return JSON.parse(condition.value);
  }
}

function cleanField(value: string): string {
  return value.trim().replace(/^["'`]+|["'`]+$/g, '');
}

function conditionToFilter(condition: Condition): Record<string, unknown> | null {
  const field = cleanField(condition.field);
  if (!field) return null;
  try {
    const value = parseConditionValue(condition);
    switch (condition.op) {
      case 'eq':
        return { [field]: value };
      case 'ne':
      case 'gt':
      case 'gte':
      case 'lt':
      case 'lte':
        return { [field]: { [`$${condition.op}`]: value } };
      case 'in':
      case 'nin':
      case 'all':
        return { [field]: { [`$${condition.op}`]: Array.isArray(value) ? value : [] } };
      case 'exists':
        return { [field]: { $exists: Boolean(value) } };
      case 'regex':
        return { [field]: { $regex: String(value) } };
      case 'size':
        return { [field]: { $size: Number(value) } };
      case 'elemMatch':
        return { [field]: { $elemMatch: value } };
      case 'type':
        return { [field]: { $type: value } };
      default:
        return null;
    }
  } catch {
    return null;
  }
}

function nodeToFilter(node: Node): Record<string, unknown> {
  if (node.kind === 'condition') return conditionToFilter(node) ?? {};
  const parts = node.children.map(nodeToFilter).filter((part) => Object.keys(part).length > 0);
  if (parts.length === 0) return {};
  if (parts.length === 1) return parts[0];
  return { [node.logic === 'and' ? '$and' : '$or']: parts };
}

function parseObject(obj: Record<string, unknown>, logic: Logic = 'and'): Group {
  const group: Group = { id: id(), kind: 'group', logic, children: [] };
  for (const [key, value] of Object.entries(obj)) {
    if ((key === '$and' || key === '$or') && Array.isArray(value)) {
      const childGroup: Group = {
        id: id(),
        kind: 'group',
        logic: key === '$and' ? 'and' : 'or',
        children: value
          .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === 'object' && !Array.isArray(item))
          .map((item) => parseObject(item)),
      };
      group.children.push(childGroup);
      continue;
    }
    if (key.startsWith('$')) continue;
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      const operators = Object.entries(value as Record<string, unknown>);
      if (operators.length > 0 && operators.every(([op]) => op.startsWith('$'))) {
        for (const [op, opValue] of operators) {
          group.children.push({
            id: id(),
            kind: 'condition',
            field: key,
            op: op.slice(1),
            value: valueText(opValue),
            valueType: valueTypeOf(opValue),
          });
        }
        continue;
      }
    }
    group.children.push({
      id: id(),
      kind: 'condition',
      field: key,
      op: 'eq',
      value: valueText(value),
      valueType: valueTypeOf(value),
    });
  }
  if (group.children.length === 0) group.children.push(newCondition());
  return group;
}

function parseFilter(raw: string): Group {
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parseObject(parsed as Record<string, unknown>);
    }
  } catch {
    // Invalid JSON falls back to an empty group.
  }
  return newGroup();
}

function removeNode(node: Node, targetId: string): Node {
  if (node.kind !== 'group') return node;
  return {
    ...node,
    children: node.children
      .filter((child) => child.id !== targetId)
      .map((child) => removeNode(child, targetId)),
  };
}

function GroupEditor({
  group,
  onChange,
  onRemove,
  depth,
  fields,
}: {
  group: Group;
  onChange: (group: Group) => void;
  onRemove?: () => void;
  depth: number;
  fields: string[];
}) {
  const updateChild = (child: Node) => {
    onChange({ ...group, children: group.children.map((item) => (item.id === child.id ? child : item)) });
  };

  return (
    <div className={`qb-group${depth > 0 ? ' nested' : ''}`}>
      <div className="qb-group-head">
        <select value={group.logic} onChange={(e) => onChange({ ...group, logic: e.target.value as Logic })}>
          <option value="and">AND</option>
          <option value="or">OR</option>
        </select>
        {onRemove && <button className="danger small" onClick={onRemove}>Remove group</button>}
      </div>
      <div className="qb-children">
        {group.children.map((child) =>
          child.kind === 'group' ? (
            <GroupEditor
              key={child.id}
              group={child}
              depth={depth + 1}
              fields={fields}
              onChange={(next) => updateChild(next)}
              onRemove={() => onChange(removeNode(group, child.id) as Group)}
            />
          ) : (
            <ConditionEditor
              key={child.id}
              condition={child}
              onChange={(next) => updateChild(next)}
              onRemove={() => onChange(removeNode(group, child.id) as Group)}
            />
          ),
        )}
      </div>
      <div className="qb-actions">
        <button className="small" onClick={() => onChange({ ...group, children: [...group.children, newCondition()] })}>
          + Condition
        </button>
        <button className="small" onClick={() => onChange({ ...group, children: [...group.children, newGroup(group.logic)] })}>
          + Group
        </button>
      </div>
    </div>
  );
}

function ConditionEditor({
  condition,
  onChange,
  onRemove,
}: {
  condition: Condition;
  onChange: (condition: Condition) => void;
  onRemove: () => void;
}) {
  return (
    <div className="qb-condition">
      <input
        value={condition.field}
        onChange={(e) => onChange({ ...condition, field: e.target.value })}
        placeholder="field.path"
        className="qb-field"
        list="nonodb-field-paths-qb"
      />
      <select value={condition.op} onChange={(e) => onChange({ ...condition, op: e.target.value })}>
        {OPERATORS.map(([value, label]) => (
          <option key={value} value={value}>{label}</option>
        ))}
      </select>
      <select
        value={condition.valueType}
        onChange={(e) => onChange({ ...condition, valueType: e.target.value as ValueType })}
      >
        <option value="string">string</option>
        <option value="number">number</option>
        <option value="boolean">boolean</option>
        <option value="null">null</option>
        <option value="json">JSON</option>
      </select>
      <input
        value={condition.value}
        onChange={(e) => onChange({ ...condition, value: e.target.value })}
        placeholder={condition.valueType === 'json' ? '[1,2] or {"x":1}' : 'value'}
        className="qb-value"
      />
      <button className="danger small" onClick={onRemove}>✕</button>
    </div>
  );
}

export default function QueryBuilder({
  value,
  onChange,
  fields,
}: {
  value: string;
  onChange: (value: string) => void;
  fields: string[];
}) {
  const [root, setRoot] = useState<Group>(() => parseFilter(value));
  const [error, setError] = useState('');

  const preview = useMemo(() => nodeToFilter(root), [root]);

  const apply = () => {
    try {
      const rendered = JSON.stringify(preview, null, 2);
      JSON.parse(rendered);
      setError('');
      onChange(rendered);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <div className="query-builder">
      <div className="qb-toolbar">
        <span className="muted small">Build nested AND/OR groups. Empty values are ignored until you apply.</span>
        <button className="small" onClick={() => setRoot(parseFilter(value))}>Sync from JSON</button>
      </div>
      <datalist id="nonodb-field-paths-qb">
        {fields.map((field) => <option key={field} value={field} />)}
      </datalist>
      <GroupEditor group={root} depth={0} onChange={setRoot} fields={fields} />
      {error && <div className="error-box">{error}</div>}
      <div className="qb-footer">
        <pre className="qb-preview">{JSON.stringify(preview, null, 2)}</pre>
        <button className="primary" onClick={apply}>Use this filter</button>
      </div>
    </div>
  );
}
