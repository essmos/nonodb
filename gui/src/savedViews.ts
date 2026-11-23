export type SavedView = {
  name: string;
  filter: string;
  sortField: string;
  sortDir: 'asc' | 'desc';
  limit: number;
  projection: string;
};

export const SAVED_VIEWS_FORMAT = 'nonodb.saved-views';
export const SAVED_VIEWS_VERSION = 1;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function text(value: unknown, fallback: string, field: string): string {
  if (value === undefined) return fallback;
  if (typeof value !== 'string') throw new Error(`${field} must be a string`);
  return value;
}

function normalizeView(value: unknown, index: number): SavedView {
  if (!isRecord(value)) throw new Error(`view ${index + 1} must be an object`);
  const rawName = value.name;
  if (typeof rawName !== 'string' || rawName.trim() === '') {
    throw new Error(`view ${index + 1} needs a non-empty name`);
  }
  const sortDir = value.sortDir === undefined ? 'asc' : value.sortDir;
  if (sortDir !== 'asc' && sortDir !== 'desc') {
    throw new Error(`view "${rawName.trim()}" has an invalid sort direction`);
  }
  const rawLimit = value.limit === undefined ? 50 : value.limit;
  if (typeof rawLimit !== 'number' || !Number.isFinite(rawLimit) || rawLimit < 0 || rawLimit > 10_000) {
    throw new Error(`view "${rawName.trim()}" has an invalid limit`);
  }
  return {
    name: rawName.trim(),
    filter: text(value.filter, '{}', 'filter'),
    sortField: text(value.sortField, '_id', 'sortField'),
    sortDir,
    limit: Math.floor(rawLimit),
    projection: text(value.projection, '', 'projection'),
  };
}

export function normalizeViewList(value: unknown): SavedView[] {
  if (!Array.isArray(value)) throw new Error('views must be an array');
  const byName = new Map<string, SavedView>();
  value.forEach((item, index) => {
    const view = normalizeView(item, index);
    byName.set(view.name, view);
  });
  return [...byName.values()].sort((a, b) => a.name.localeCompare(b.name));
}

export function parseSavedViewsFile(content: string): SavedView[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(content);
  } catch {
    throw new Error('the file is not valid JSON');
  }
  if (!isRecord(parsed)) throw new Error('the file must contain a JSON object');
  if (parsed.format !== SAVED_VIEWS_FORMAT) {
    throw new Error(`unsupported format (expected ${SAVED_VIEWS_FORMAT})`);
  }
  if (parsed.version !== SAVED_VIEWS_VERSION) {
    throw new Error(`unsupported version (expected ${SAVED_VIEWS_VERSION})`);
  }
  return normalizeViewList(parsed.views);
}

export function serializeSavedViews(collection: string, views: SavedView[]): string {
  return JSON.stringify(
    {
      format: SAVED_VIEWS_FORMAT,
      version: SAVED_VIEWS_VERSION,
      collection,
      views: views.map(({ name, filter, sortField, sortDir, limit, projection }) => ({
        name,
        filter,
        sortField,
        sortDir,
        limit,
        projection,
      })),
    },
    null,
    2,
  );
}

export function savedViewsFileName(collection: string): string {
  const safe = collection
    .replace(/[^A-Za-z0-9._-]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 80);
  return `nonodb-views-${safe || 'collection'}.json`;
}
