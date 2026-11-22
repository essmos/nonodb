# Vector engine comparison

Compares NonoDB's exact/IVF vector path with Qdrant (HNSW) and Elasticsearch
(`dense_vector`/HNSW) on real MS MARCO passages and real MS MARCO queries.
See [RESULTS.md](RESULTS.md) for the first 500k/384D result set and tuning
points.

The benchmark deliberately uses a smaller collection than the 100M synthetic
test: 500,000 passages with 384-dimensional MiniLM embeddings. The point is to
test a realistic embedding width and real text, not to reproduce the 100M
throughput number. The same harness accepts any `--limit`.

## What is measured

- latency p50 / p95 / p99 and sequential QPS through each engine's native client,
- recall@10 against an exact NumPy cosine top-10 ground truth,
- index/load time and recall for each engine's default-ish ANN configuration.

All vectors are L2-normalized, so cosine similarity equals a dot product.
Queries are the first 1,000 rows of `queries.train.tsv`; passages are the first
`--limit` rows of `collection.tsv`. Change the selection in
`prepare_msmarco.py` if you need a different slice.

Not covered yet:

- filtered vector search (NonoDB intentionally falls back to an exact scan when
  a filter is active, so it is not an apples-to-apples HNSW-filter comparison),
- hybrid BM25 + vector ranking,
- concurrent clients and multi-process throughput,
- quantization and recall/latency tuning sweeps.

## Data preparation

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install numpy sentence-transformers qdrant-client elasticsearch tqdm
# CPU-only torch is enough:
# pip install torch --index-url https://download.pytorch.org/whl/cpu

python prepare_msmarco.py \
  --collection /data/msmarco/collection.tsv \
  --queries /data/msmarco/queries.train.tsv \
  --out /data/vector-compare/500k \
  --limit 500000 --queries-limit 1000

python ground_truth.py --data /data/vector-compare/500k --top 10
```

`prepare_msmarco.py` writes the passages, query text, embeddings, ids, and
`meta.json`. `ground_truth.py` writes `ground_truth.npy` with row indices into
`embeddings.npy`/`pass_ids.txt`.

## Engines

Start the two containerized engines:

```bash
docker compose up -d
curl -fsS http://127.0.0.1:6333/healthz
curl -fsS http://127.0.0.1:9200
```

Run NonoDB on a dedicated data directory and socket. Build the ANN sidecar only
after the collection is loaded:

```bash
NONODB_DATA=/data/vector-compare/nonodb-data \
NONODB_SOCK=/data/vector-compare/nonodb.sock \
NONODB_TCP=127.0.0.1:7879 \
NONODB_CACHE_TTL=0 \
  /path/to/nonodb-daemon
```

Load NonoDB, create the exact vector index, and measure the exact baseline:

```bash
python bench_nonodb.py --data /data/vector-compare/500k \
  --socket /data/vector-compare/nonodb.sock \
  --collection vector_compare --load --create-index
python bench_nonodb.py --data /data/vector-compare/500k \
  --socket /data/vector-compare/nonodb.sock \
  --collection vector_compare --benchmark --label nonodb-exact
```

Restart the daemon with the sidecar builder enabled; it builds only this
collection because the collection list is explicit:

```bash
NONODB_DATA=/data/vector-compare/nonodb-data \
NONODB_SOCK=/data/vector-compare/nonodb.sock \
NONODB_TCP=127.0.0.1:7879 \
NONODB_CACHE_TTL=0 \
NONODB_BUILD_VECTOR_ANN=1 \
NONODB_VECTOR_ANN_COLLECTIONS=vector_compare \
NONODB_VECTOR_ANN_PROBE=80 \
NONODB_SEARCH_WORKERS=16 \
  /path/to/nonodb-daemon
```

Then measure the ANN path:

```bash
python bench_nonodb.py --data /data/vector-compare/500k \
  --socket /data/vector-compare/nonodb.sock \
  --collection vector_compare --benchmark --label nonodb-ann
```

Qdrant:

```bash
python bench_qdrant.py --data /data/vector-compare/500k \
  --url http://127.0.0.1:6333 --collection vector_compare \
  --load --benchmark --label qdrant-hnsw
```

Elasticsearch:

```bash
python bench_es.py --data /data/vector-compare/500k \
  --url http://127.0.0.1:9200 --index vector-compare \
  --load --benchmark --label elasticsearch-hnsw
```

Results are written as JSON files under `results/` and printed to stdout. Keep
the same `--data` directory, query set, warmup, and rounds for every engine.

## Fairness notes

- Run engines one at a time on the same host; do not benchmark while another
  engine is rebuilding an index.
- Warm up each query path before recording latency.
- Keep float32 vectors and cosine distance on every engine.
- Tune `nprobe`/`hnsw_ef`/`num_candidates` to the same recall target before
  comparing latency. The scripts expose those knobs.
- The exact NumPy ground truth is the reference; an engine's own exact scan is
  not used as ground truth.
