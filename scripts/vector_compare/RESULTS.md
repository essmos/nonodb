# Real-embedding vector engine comparison

Initial result set for NonoDB vs Qdrant vs Elasticsearch on real MS MARCO data.
Reproduce with the scripts in this directory; raw JSON results were produced by
`bench_*.py`.

## Setup

| Item | Value |
|---|---|
| Host | 16 vCPU Xeon D-2141I, 31 GiB RAM, ZFS, same host for all engines |
| NonoDB | `v0.27.0`, ANN format v2, 707 clusters |
| Qdrant | `1.12.4`, HNSW `m=16`, `ef_construct=100` |
| Elasticsearch | `8.15.0`, `dense_vector` HNSW `m=16`, `ef_construction=100` |
| Corpus | 500,000 MS MARCO passages (real text, first 500k rows) |
| Queries | 1,000 real MS MARCO training queries |
| Embeddings | `all-MiniLM-L6-v2`, 384 dimensions, L2-normalized, cosine |
| Ground truth | exact NumPy top-10 over all 500k vectors |
| Client | one sequential client per engine, 20 warmup queries, 1,000 measured |

## Latency and recall

Configurations were chosen to land near recall@10 ≈ 0.95–0.96.

| Engine | Config | p50 ms | p95 ms | p99 ms | QPS | Recall@10 |
|---|---|---:|---:|---:|---:|---:|
| NonoDB exact | no sidecar | 346.86 | 360.25 | 364.86 | 2.9 | 1.0000 |
| NonoDB ANN | `nprobe=80` | **8.06** | **9.46** | **10.50** | **121.4** | **0.9579** |
| Qdrant HNSW | `hnsw_ef=32` | **2.26** | **2.64** | **2.88** | **431.5** | **0.9637** |
| Elasticsearch HNSW | `num_candidates=64` | **4.47** | **5.06** | **5.36** | **221.1** | **0.9529** |

The selected latency rows are medians of three runs; recall is identical across
the repeats. At comparable recall, Qdrant is about **3.6× faster** than NonoDB
ANN at p50, and Elasticsearch about **1.8× faster**. NonoDB ANN is about
**43× faster** than its exact vector path.

## Tuning points

| Engine | Setting | p50 ms | p95 ms | Recall@10 |
|---|---|---:|---:|---:|
| NonoDB | `nprobe=32` | 6.41 | 7.35 | 0.9113 |
| NonoDB | `nprobe=64` | 7.58 | 9.06 | 0.9498 |
| NonoDB | `nprobe=80` | 8.06–8.78 | 9.46–10.27 | 0.9579 |
| NonoDB | `nprobe=96` | 8.70 | 10.29 | 0.9643 |
| NonoDB | `nprobe=128` | 10.35 | 12.30 | 0.9731 |
| Qdrant | `hnsw_ef=32` | 2.17–2.26 | 2.45–2.64 | 0.9637 |
| Qdrant | `hnsw_ef=64` | 2.59 | 2.92 | 0.9854 |
| Qdrant | `hnsw_ef=128` | 3.09 | 3.55 | 0.9947 |
| Elasticsearch | `num_candidates=32` | 8.94 | 19.79 | 0.9182 |
| Elasticsearch | `num_candidates=64` | 4.47–5.21 | 5.06–6.17 | 0.9529 |
| Elasticsearch | `num_candidates=128` | 5.41 | 6.66 | 0.9781 |

## Load, build, and storage

| Engine | Load / build | ANN / index | On-disk |
|---|---|---|---|
| NonoDB | 500k docs in 205 s; exact vector index 15.2 s | ANN sidecar built in 40 s at 707 clusters | 8.4 GB total; ANN sidecar 741 MB |
| Qdrant | 500k points in 114 s; HNSW ready +15 s | HNSW built during upsert | 854 MB (vectors only, no payload) |
| Elasticsearch | 500k docs in 852 s including force-merge | HNSW built during indexing | 3.7 GB (body + vector) |

Storage is not directly comparable: NonoDB also stores document bodies, history
metadata, the exact LMDB vector index, and the ANN sidecar; Qdrant was loaded
without payload text; Elasticsearch stores body and vector.

## Cluster-count experiment

The default builder uses `sqrt(entries)` clusters (707 for 500k). Forcing a
larger target with `NONODB_VECTOR_ANN_CLUSTERS=4096` did **not** help at this
scale: the clusters were too small and practical `nprobe` values lost recall.

| Clusters | nprobe | p50 ms | Recall@10 |
|---:|---:|---:|---:|
| 707 | 80 | **8.06** | **0.9579** |
| 4096 | 32 | 9.63 | 0.8610 |
| 4096 | 64 | 9.79 | 0.9070 |

The 4096-cluster build needed **210 s** after parallelizing k-means training
(previously it ran for over 20 minutes without finishing). The 707-cluster
build now takes **40 s**. The default `sqrt(entries)` layout therefore stays;
larger cluster counts need better centroid quality or another index family.

## Interpretation

- Real 384-dimensional embeddings change the picture completely compared with
  the 16-dimensional 100M sidecar test. Exact NonoDB scans become very
  expensive (347 ms p50), so ANN is essential.
- Qdrant's HNSW is the fastest option here and reaches higher recall at a much
  lower latency. Elasticsearch is in the middle and is the closest comparison
  to NonoDB functionally, because it also stores text and can do BM25 + vector
  hybrid search.
- NonoDB's ANN is slower at this recall level, but it still cuts p50 from
  347 ms to 8.3 ms and keeps the single-writer, MongoDB-like database model.
- At 707 clusters and `nprobe=80`, NonoDB scans roughly 11% of the corpus per
  query. More clusters or a different index family (HNSW/PQ) would be needed to
  close the gap.

## Not measured yet

- filtered vector search (NonoDB deliberately uses the exact path with filters),
- hybrid BM25 + vector ranking,
- concurrent clients and throughput at fixed latency,
- quantization / memory trade-offs,
- recall against MS MARCO qrels (this run measures ANN recall against exact
  neighbours, not end-to-end relevance).
