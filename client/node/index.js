'use strict';

const fs = require('node:fs');
const http = require('node:http');
const https = require('node:https');

const READ_OPS = new Set([
  'find',
  'find_one',
  'lookup',
  'watch',
  'count',
  'list_indexes',
  'list_collections',
  'list_text_indexes',
  'list_fuzzy_indexes',
  'list_vector_indexes',
  'search',
  'fuzzy',
  'vector_search',
  'sql',
  'explain_sql',
  'batch',
]);

/** Error returned by the daemon or by the transport. */
class NonoDBError extends Error {
  constructor(code, message, statusCode = 0) {
    super(`${code}: ${message}`);
    this.name = 'NonoDBError';
    this.code = code;
    this.message = message;
    this.statusCode = statusCode;
  }
}

function envVerify() {
  const value = (process.env.NONODB_TLS_VERIFY ?? '1').toLowerCase();
  return !['0', 'false', 'no'].includes(value);
}

function envTimeoutMs() {
  const seconds = Number(process.env.NONODB_TIMEOUT ?? '10');
  return Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : 10000;
}

/**
 * Thin async client for nonodb.
 *
 * With `socketPath` (or `NONODB_SOCK`) it uses a Unix domain socket. With
 * `baseUrl` (or `NONODB_URL`) it uses HTTP over TCP/TLS.
 */
class NonoDB {
  constructor(options = {}) {
    if (typeof options === 'string') options = { socketPath: options };

    this.token = options.token ?? process.env.NONODB_TOKEN ?? '';
    this.timeoutMs = options.timeoutMs ?? envTimeoutMs();
    this.verify = options.verify ?? envVerify();
    this.vaultHeaders = options.vaultHeaders ?? null;
    this.clientCert = options.clientCert ?? process.env.NONODB_TLS_CLIENT_CERT;
    this.clientKey = options.clientKey ?? process.env.NONODB_TLS_CLIENT_KEY;
    this.caCert = options.caCert ?? process.env.NONODB_TLS_CA;
    if (!!this.clientCert !== !!this.clientKey) {
      throw new Error('NONODB_TLS_CLIENT_CERT and NONODB_TLS_CLIENT_KEY must be set together');
    }
    this.tlsData = null;

    const baseUrl = options.baseUrl ?? (options.socketPath ? undefined : process.env.NONODB_URL);
    if (baseUrl) {
      this.baseUrl = baseUrl.replace(/\/+$/, '');
      this.socketPath = undefined;
    } else {
      this.baseUrl = 'http://localhost';
      this.socketPath =
        options.socketPath ??
        process.env.NONODB_SOCK ??
        (process.platform === 'win32' ? undefined : '/run/nonodb.sock');
      if (!this.socketPath) {
        this.baseUrl = (process.env.NONODB_URL ?? 'http://127.0.0.1:7878').replace(/\/+$/, '');
      }
    }
    this.closed = false;
  }

  close() {
    this.closed = true;
  }

  /** Check GET /healthz. */
  async health() {
    try {
      const body = await this._request('GET', '/healthz');
      return body?.ok === true;
    } catch {
      return false;
    }
  }

  /** Execute an arbitrary /v1/exec payload and return its `result`. */
  async exec(payload) {
    if (this.closed) throw new NonoDBError('closed', 'client is closed');
    const attempts = READ_OPS.has(payload?.op) ? 3 : 1;
    let lastError;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      try {
        const body = await this._request('POST', '/v1/exec', payload);
        if (body?.ok !== true) {
          const error = body?.error ?? {};
          throw new NonoDBError(error.code ?? 'error', error.message ?? '', 0);
        }
        return body.result ?? null;
      } catch (error) {
        if (
          error instanceof NonoDBError &&
          error.code === 'connection' &&
          attempt + 1 < attempts
        ) {
          lastError = error;
          await new Promise((resolve) => setTimeout(resolve, 10 * (attempt + 1)));
          continue;
        }
        throw error;
      }
    }
    throw lastError;
  }

  async _request(method, path, body) {
    const payload = body === undefined ? undefined : Buffer.from(JSON.stringify(body));
    const headers = {};
    if (payload) {
      headers['content-type'] = 'application/json';
      headers['content-length'] = String(payload.length);
    }
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    if (this.vaultHeaders) Object.assign(headers, this.vaultHeaders);

    const url = new URL(path, this.baseUrl);
    const useTls = !this.socketPath && url.protocol === 'https:';
    const transport = useTls ? https : http;
    const options = {
      method,
      path: `${url.pathname}${url.search}`,
      headers,
      timeout: this.timeoutMs,
    };
    if (this.socketPath) {
      options.socketPath = this.socketPath;
      options.host = 'localhost';
    } else {
      options.hostname = url.hostname;
      options.port = url.port || (useTls ? 443 : 80);
      options.rejectUnauthorized = this.verify;
      if (useTls) {
        if (this.tlsData === null) {
          this.tlsData = {};
          if (this.clientCert) this.tlsData.cert = fs.readFileSync(this.clientCert);
          if (this.clientKey) this.tlsData.key = fs.readFileSync(this.clientKey);
          if (this.caCert) this.tlsData.ca = fs.readFileSync(this.caCert);
        }
        Object.assign(options, this.tlsData);
      }
    }

    return new Promise((resolve, reject) => {
      const request = transport.request(options, (response) => {
        const chunks = [];
        response.on('data', (chunk) => chunks.push(chunk));
        response.on('error', (error) =>
          reject(new NonoDBError('connection', error.message)),
        );
        response.on('end', () => {
          const raw = Buffer.concat(chunks).toString('utf8');
          let parsed;
          try {
            parsed = raw ? JSON.parse(raw) : {};
          } catch {
            reject(
              new NonoDBError(
                'invalid_response',
                `HTTP ${response.statusCode}: ${raw.slice(0, 200)}`,
                response.statusCode,
              ),
            );
            return;
          }
          if (response.statusCode < 200 || response.statusCode >= 300) {
            const error = parsed?.error ?? {};
            reject(
              new NonoDBError(
                error.code ?? 'http_error',
                error.message ?? `HTTP ${response.statusCode}`,
                response.statusCode,
              ),
            );
            return;
          }
          resolve(parsed);
        });
      });
      request.on('timeout', () => request.destroy(new Error('request timeout')));
      request.on('error', (error) => reject(new NonoDBError('connection', error.message)));
      if (payload) request.write(payload);
      request.end();
    });
  }

  /** Return a clone that sends vault encryption headers on every request. */
  vault(tenant, keyId, key) {
    const clone = Object.create(NonoDB.prototype);
    Object.assign(clone, this);
    clone.vaultHeaders = {
      'x-nonodb-tenant': tenant,
      'x-nonodb-key-id': keyId,
      'x-nonodb-key': key,
    };
    return clone;
  }

  /** Create a collection explicitly. Use profile `vault` for encrypted storage. */
  async createCollection(name, profile = 'standard', ttlSeconds = null, purgeHistory = null) {
    const req = { op: 'create_collection', coll: name, profile };
    if (ttlSeconds !== null) req.ttl_seconds = ttlSeconds;
    if (purgeHistory !== null) req.ttl_purge_history = purgeHistory;
    return this.exec(req);
  }

  /** Set or clear a collection's default document TTL. */
  async setCollectionTtl(name, ttlSeconds, purgeHistory = null) {
    const req = { op: 'set_collection_ttl', coll: name, ttl_seconds: ttlSeconds };
    if (purgeHistory !== null) req.purge_history = purgeHistory;
    return this.exec(req);
  }

  /** Read a collection's default document TTL. */
  async collectionTtl(name) {
    const result = await this.exec({ op: 'get_collection_ttl', coll: name });
    return result?.ttl_seconds ?? null;
  }

  /** List collection names. */
  async collections() {
    const result = await this.exec({ op: 'list_collections' });
    return result?.collections ?? [];
  }

  /** Execute read-only operations in one LMDB transaction. */
  /**
   * Run a read-only SQL SELECT and return its rows.
   * Parameters are positional and may use `?` or `$1` placeholders.
   */
  async sql(query, params = []) {
    const result = await this.exec({ op: 'sql', query, params });
    return result.results ?? [];
  }

  /** Explain a read-only SQL SELECT. */
  async explainSql(query, params = []) {
    return this.exec({ op: 'explain_sql', query, params });
  }

  async batch(ops) {
    const result = await this.exec({ op: 'batch', ops });
    return result?.results ?? [];
  }

  /** Execute writes atomically: all operations commit or none do. */
  async writeBatch(ops) {
    const result = await this.exec({ op: 'write_batch', ops });
    return result?.results ?? [];
  }

  /** Return a handle for one collection. */
  collection(name) {
    return new Collection(this, name);
  }
}

class Collection {
  constructor(db, name) {
    this.db = db;
    this.name = name;
  }

  async insert(doc) {
    const result = await this.db.exec({ op: 'insert', coll: this.name, doc });
    return result.inserted_id;
  }

  async insertMany(docs) {
    const result = await this.db.exec({
      op: 'insert_many',
      coll: this.name,
      docs: Array.from(docs),
    });
    return result.inserted_ids ?? [];
  }

  async update(filter, update, { multi = false, upsert = false } = {}) {
    return (await this.updateResult(filter, update, { multi, upsert })).modified;
  }

  async updateResult(filter, update, { multi = false, upsert = false } = {}) {
    return this.db.exec({
      op: 'update',
      coll: this.name,
      filter,
      update,
      multi,
      upsert,
    });
  }

  async delete(filter, { multi = false } = {}) {
    const result = await this.db.exec({
      op: 'delete',
      coll: this.name,
      filter,
      multi,
    });
    return result.deleted;
  }

  /** Atomically write several operations in this collection. */
  async writeBatch(ops) {
    return this.db.writeBatch(
      ops.map((op) => ({ coll: this.name, ...op })),
    );
  }

  async createIndex(fields, { unique = false, include = [] } = {}) {
    if (typeof fields === 'string') fields = [fields];
    await this.db.exec({
      op: 'create_index',
      coll: this.name,
      fields,
      unique,
      include,
    });
    return true;
  }

  async createFieldIndex(field, unique = false) {
    return this.createIndex([field], { unique });
  }

  async dropIndex(field) {
    const result = await this.db.exec({
      op: 'drop_index',
      coll: this.name,
      field,
    });
    return result.dropped;
  }

  async listIndexes() {
    const result = await this.db.exec({ op: 'list_indexes', coll: this.name });
    return result.indexes ?? [];
  }

  /**
   * Ordered point lookups by `_id` or a unique index.
   * `by` is one field name or an array of fields for a compound unique index.
   * For compound lookups each value must be an array with the same length.
   * Missing values produce `null`. `fields` is an inclusive projection and can
   * be served from a covering index. `projection` accepts a normal flat
   * projection (including `_id: 0`) and cannot be combined with `fields`.
   */
  async lookup(by, values, { fields, projection } = {}) {
    if (fields !== undefined && projection !== undefined) {
      throw new Error('lookup accepts either fields or projection, not both');
    }
    const payload = {
      op: 'lookup',
      coll: this.name,
      by: Array.isArray(by) ? Array.from(by) : by,
      values: Array.from(values),
    };
    if (fields !== undefined) {
      payload.fields = Array.from(fields);
    }
    if (projection !== undefined) {
      payload.projection = projection;
    }
    const result = await this.db.exec(payload);
    return result.results ?? [];
  }

  /** Return one document by `_id` or a unique index. */
  async lookupOne(by, value, options = {}) {
    const [document] = await this.lookup(by, [value], options);
    return document ?? null;
  }

  async getMany(ids) {
    const results = await this.lookup('_id', ids);
    return results.filter((document) => document != null);
  }

  /** List document revisions, newest first. History must be enabled on the daemon. */
  async history(id, { limit = 50, includeDocs = false } = {}) {
    const result = await this.db.exec({
      op: 'history',
      coll: this.name,
      id,
      limit,
      include_docs: includeDocs,
    });
    return result.revisions ?? [];
  }

  /** Read one exact revision, or null if it is not available. */
  async getVersion(id, version) {
    return this.db.exec({
      op: 'get_version',
      coll: this.name,
      id,
      version,
    });
  }

  async createTextIndex(field) {
    await this.db.exec({ op: 'create_text_index', coll: this.name, field });
    return true;
  }

  async dropTextIndex(field) {
    const result = await this.db.exec({
      op: 'drop_text_index',
      coll: this.name,
      field,
    });
    return result.dropped;
  }

  async listTextIndexes() {
    const result = await this.db.exec({
      op: 'list_text_indexes',
      coll: this.name,
    });
    return result.text_indexes ?? [];
  }

  async createFuzzyIndex(field) {
    await this.db.exec({ op: 'create_fuzzy_index', coll: this.name, field });
    return true;
  }

  async dropFuzzyIndex(field) {
    const result = await this.db.exec({
      op: 'drop_fuzzy_index',
      coll: this.name,
      field,
    });
    return result.dropped;
  }

  async listFuzzyIndexes() {
    const result = await this.db.exec({
      op: 'list_fuzzy_indexes',
      coll: this.name,
    });
    return result.fuzzy_indexes ?? [];
  }

  async createVectorIndex(field, dims, metric = 'cosine') {
    await this.db.exec({
      op: 'create_vector_index',
      coll: this.name,
      field,
      dims,
      metric,
    });
    return true;
  }

  async dropVectorIndex(field) {
    const result = await this.db.exec({
      op: 'drop_vector_index',
      coll: this.name,
      field,
    });
    return result.dropped;
  }

  async listVectorIndexes() {
    const result = await this.db.exec({
      op: 'list_vector_indexes',
      coll: this.name,
    });
    return result.vector_indexes ?? [];
  }

  /** Exact vector search. Results contain `_distance`, lowest first. */
  async vectorSearch(
    field,
    vector,
    { filter = {}, skip = 0, limit = 10 } = {},
  ) {
    const result = await this.db.exec({
      op: 'vector_search',
      coll: this.name,
      field,
      vector,
      filter,
      skip,
      limit,
    });
    return result.results ?? [];
  }

  /** BM25 full-text search. Results contain `_score`, highest first. */
  async search(field, query, { filter = {}, skip = 0, limit = 10 } = {}) {
    const result = await this.db.exec({
      op: 'search',
      coll: this.name,
      field,
      query,
      filter,
      skip,
      limit,
    });
    return result.results ?? [];
  }

  /** Fuzzy search. Results contain `_distance`, lowest first. */
  async fuzzy(
    field,
    query,
    { maxDistance = 2, filter = {}, limit = 10 } = {},
  ) {
    const result = await this.db.exec({
      op: 'fuzzy',
      coll: this.name,
      field,
      query,
      max_distance: maxDistance,
      filter,
      limit,
    });
    return result.results ?? [];
  }

  /** Find documents. `options` accepts projection, sort, skip and limit. */
  /** One long-poll batch from an append-only event collection. */
  async watchOnce(after = null, {
    limit = 100,
    timeoutMs = 1000,
    types,
    filter,
    projection,
  } = {}) {
    const payload = {
      op: 'watch',
      coll: this.name,
      limit,
      timeout_ms: timeoutMs,
    };
    if (after) payload.after = after;
    if (types != null) payload.types = Array.from(types);
    if (filter != null) payload.filter = filter;
    if (projection != null) payload.projection = projection;
    return this.db.exec(payload);
  }

  /**
   * Async generator over an append-only event collection.
   *
   * Reconnects after transport errors with exponential backoff. Throws
   * `stream_truncated` when the resume cursor is older than retained events.
   */
  async *watch(after = null, {
    limit = 100,
    timeoutMs = 1000,
    types,
    filter,
    projection,
    retry = true,
    retryDelayMs = 1000,
    maxRetryDelayMs = 30000,
  } = {}) {
    let cursor = after;
    let delay = Math.max(1, retryDelayMs);
    for (;;) {
      try {
        const batch = await this.watchOnce(cursor, {
          limit,
          timeoutMs,
          types,
          filter,
          projection,
        });
        if (batch.truncated) {
          const error = new NonoDBError(
            'stream_truncated',
            `events before ${batch.oldest ?? cursor} are no longer retained`,
          );
          error.oldest = batch.oldest ?? null;
          throw error;
        }
        for (const event of batch.events ?? []) yield event;
        cursor = batch.next || cursor;
        delay = Math.max(1, retryDelayMs);
      } catch (error) {
        if (
          !retry ||
          !(error instanceof NonoDBError) ||
          error.code !== 'connection'
        ) {
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, delay));
        delay = Math.min(maxRetryDelayMs, delay * 2);
      }
    }
  }

  async find(filter = {}, { projection, sort, skip = 0, limit } = {}) {
    const payload = {
      op: 'find',
      coll: this.name,
      filter,
      skip,
    };
    if (projection !== undefined) payload.projection = projection;
    if (sort !== undefined) payload.sort = sort;
    if (limit !== undefined) payload.limit = limit;
    return this.db.exec(payload);
  }

  async findOne(filter = {}, { projection } = {}) {
    const payload = { op: 'find_one', coll: this.name, filter };
    if (projection !== undefined) payload.projection = projection;
    const result = await this.db.exec(payload);
    return result ?? null;
  }

  async count(filter = {}) {
    const result = await this.db.exec({ op: 'count', coll: this.name, filter });
    return result.count;
  }

  /** Run a read-only aggregation pipeline. */
  async aggregate(pipeline) {
    const result = await this.db.exec({
      op: 'aggregate',
      coll: this.name,
      pipeline,
    });
    return result.results ?? [];
  }
}

module.exports = { NonoDB, NonoDBError, Collection };
