export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

export interface ClientOptions {
  socketPath?: string;
  baseUrl?: string;
  token?: string;
  timeoutMs?: number;
  verify?: boolean;
  clientCert?: string;
  clientKey?: string;
  caCert?: string;
}

export interface FindOptions {
  projection?: Record<string, 0 | 1>;
  sort?: Array<[string, 1 | -1]>;
  skip?: number;
  limit?: number;
}

export interface UpdateOptions {
  multi?: boolean;
  upsert?: boolean;
}

export interface IndexOptions {
  unique?: boolean;
  include?: string[];
}

export interface UpdateResult {
  matched: number;
  modified: number;
  upserted_id: string | null;
}

export class NonoDBError extends Error {
  code: string;
  statusCode: number;
}

export class NonoDB {
  constructor(options?: ClientOptions | string);
  close(): void;
  health(): Promise<boolean>;
  exec(payload: Record<string, unknown>): Promise<any>;
  collections(): Promise<string[]>;
  createCollection(name: string, profile?: string, ttlSeconds?: number | null, purgeHistory?: boolean | null): Promise<any>;
  setCollectionTtl(name: string, ttlSeconds: number | null, purgeHistory?: boolean | null): Promise<any>;
  collectionTtl(name: string): Promise<number | null>;
  batch(ops: Record<string, unknown>[]): Promise<any[]>;
  writeBatch(ops: Record<string, unknown>[]): Promise<any[]>;
  collection(name: string): Collection;
}

export interface BatchEntry {
  ok: boolean;
  result?: any;
  error?: { code: string; message: string };
}

export class Collection {
  readonly name: string;
  insert(doc: Record<string, unknown>): Promise<string>;
  insertMany(docs: Iterable<Record<string, unknown>>): Promise<string[]>;
  update(filter: Record<string, unknown>, update: Record<string, unknown>, options?: UpdateOptions): Promise<number>;
  updateResult(filter: Record<string, unknown>, update: Record<string, unknown>, options?: UpdateOptions): Promise<UpdateResult>;
  delete(filter: Record<string, unknown>, options?: { multi?: boolean }): Promise<number>;
  writeBatch(ops: Record<string, unknown>[]): Promise<any[]>;
  createIndex(fields: string | string[], options?: IndexOptions): Promise<boolean>;
  createFieldIndex(field: string, unique?: boolean): Promise<boolean>;
  dropIndex(field: string): Promise<boolean>;
  listIndexes(): Promise<any[]>;
  getMany(ids: Iterable<string>): Promise<any[]>;
  history(id: string, options?: { limit?: number; includeDocs?: boolean }): Promise<any[]>;
  getVersion(id: string, version: number): Promise<any | null>;
  createTextIndex(field: string): Promise<boolean>;
  dropTextIndex(field: string): Promise<boolean>;
  listTextIndexes(): Promise<string[]>;
  createFuzzyIndex(field: string): Promise<boolean>;
  dropFuzzyIndex(field: string): Promise<boolean>;
  listFuzzyIndexes(): Promise<string[]>;
  createVectorIndex(field: string, dims: number, metric?: 'cosine' | 'dot' | 'l2'): Promise<boolean>;
  dropVectorIndex(field: string): Promise<boolean>;
  listVectorIndexes(): Promise<Array<{ field: string; dims: number; metric: string; format: number }>>;
  vectorSearch(field: string, vector: number[], options?: { filter?: Record<string, unknown>; skip?: number; limit?: number }): Promise<any[]>;
  search(field: string, query: string, options?: { filter?: Record<string, unknown>; skip?: number; limit?: number }): Promise<any[]>;
  fuzzy(field: string, query: string, options?: { maxDistance?: number; filter?: Record<string, unknown>; limit?: number }): Promise<any[]>;
  find(filter?: Record<string, unknown>, options?: FindOptions): Promise<any[]>;
  findOne(filter?: Record<string, unknown>, options?: { projection?: Record<string, 0 | 1> }): Promise<any | null>;
  count(filter?: Record<string, unknown>): Promise<number>;
  aggregate(pipeline: Record<string, unknown>[]): Promise<any[]>;
}
