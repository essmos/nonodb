//! Immutable IVF sidecar for approximate vector search.
//!
//! The sidecar is built from the exact LMDB vector index. It contains a small
//! set of centroids, cluster offsets, and the original `_id`/vector pairs.
//! Queries probe the nearest clusters and keep a bounded top-K heap. The file
//! is memory-mapped lazily; there are no background threads. The mapping is
//! cached per sidecar and reused until the file changes.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;

use memmap2::Mmap;

use crate::vector::{self, Candidate, VectorMetric};

const MAGIC: &[u8; 8] = b"NONOANN1";
const VERSION: u32 = 2;
const HEADER_SIZE: usize = 40;
const DEFAULT_MAX_CLUSTERS: usize = 4_096;
const MAX_CLUSTERS_LIMIT: usize = 8_192;
const MAX_TRAINING: usize = 100_000;
const CHECKPOINT_INTERVAL: usize = 256;
const TRAINING_ITERATIONS: usize = 8;
const MIN_BYTES_PER_WORKER: usize = 1 << 20;
const MAX_SEARCH_WORKERS: usize = 64;

fn cluster_target(entries: usize, sample: usize, configured: Option<usize>) -> usize {
    let ceiling = configured
        .unwrap_or(DEFAULT_MAX_CLUSTERS)
        .clamp(1, MAX_CLUSTERS_LIMIT);
    let base = if configured.is_some() {
        ceiling
    } else {
        entries.max(1).isqrt().clamp(1, DEFAULT_MAX_CLUSTERS)
    };
    base.clamp(1, ceiling).min(sample.max(1))
}

fn search_worker_limit() -> usize {
    std::env::var("NONODB_SEARCH_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|value| value.get())
                .unwrap_or(1)
        })
        .clamp(1, MAX_SEARCH_WORKERS)
}

#[derive(Debug, Clone, Copy)]
pub struct AnnStats {
    pub entries: usize,
    pub clusters: usize,
    pub nprobe: usize,
    pub bytes: u64,
}

struct ClusterBuildMeta {
    meta_start: u64,
    entry_count: usize,
    data_start: u64,
    data_end: u64,
    checkpoints: Vec<u64>,
}

struct ClusterInfo {
    entry_count: usize,
    data_start: usize,
    data_end: usize,
    checkpoints_offset: usize,
    checkpoint_count: usize,
}

struct ScoredCluster {
    distance: f32,
    index: usize,
}

impl PartialEq for ScoredCluster {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for ScoredCluster {}
impl PartialOrd for ScoredCluster {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ScoredCluster {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.index.cmp(&other.index))
    }
}

fn norm_of(vector: &[f32]) -> f32 {
    vector.iter().map(|v| v * v).sum::<f32>().sqrt()
}

fn normalize(vector: &mut [f32]) {
    let norm = norm_of(vector);
    if norm > f32::EPSILON {
        for value in vector {
            *value /= norm;
        }
    }
}

fn distance(
    metric: VectorMetric,
    query: &[f32],
    query_norm: f32,
    stored: &[f32],
    stored_norm: f32,
) -> f32 {
    let value = match metric {
        VectorMetric::Cosine | VectorMetric::Dot => {
            let mut dot = 0.0f32;
            for (left, right) in query.iter().zip(stored.iter()) {
                dot += left * right;
            }
            if metric == VectorMetric::Dot {
                -dot
            } else if query_norm <= f32::EPSILON || stored_norm <= f32::EPSILON {
                1.0
            } else {
                1.0 - (dot / (query_norm * stored_norm)).clamp(-1.0, 1.0)
            }
        }
        VectorMetric::L2 => {
            let mut squared = 0.0f32;
            for (left, right) in query.iter().zip(stored.iter()) {
                let delta = left - right;
                squared += delta * delta;
            }
            squared
        }
    };
    if value.is_finite() {
        value
    } else {
        f32::INFINITY
    }
}

fn nearest_centroid(
    metric: VectorMetric,
    vector: &[f32],
    vector_norm: f32,
    centroids: &[Vec<f32>],
    centroid_norms: &[f32],
) -> usize {
    let mut best = 0usize;
    let mut best_distance = f32::INFINITY;
    for (index, centroid) in centroids.iter().enumerate() {
        let stored_norm = centroid_norms[index];
        let value = match metric {
            VectorMetric::Cosine | VectorMetric::Dot => {
                let mut dot = 0.0f32;
                for (left, right) in vector.iter().zip(centroid.iter()) {
                    dot += left * right;
                }
                if metric == VectorMetric::Dot {
                    -dot
                } else if vector_norm <= f32::EPSILON || stored_norm <= f32::EPSILON {
                    1.0
                } else {
                    1.0 - (dot / (vector_norm * stored_norm)).clamp(-1.0, 1.0)
                }
            }
            VectorMetric::L2 => {
                let mut squared = 0.0f32;
                for (left, right) in vector.iter().zip(centroid.iter()) {
                    let delta = left - right;
                    squared += delta * delta;
                }
                squared
            }
        };
        if value < best_distance {
            best = index;
            best_distance = value;
        }
    }
    best
}

#[allow(clippy::too_many_arguments)]
fn kmeans_iteration(
    metric: VectorMetric,
    sample: &[Vec<f32>],
    centroids: &[Vec<f32>],
    centroid_norms: &[f32],
    clusters: usize,
    dims: usize,
    workers: usize,
) -> (Vec<Vec<f32>>, Vec<usize>) {
    let workers = workers.min(sample.len().max(1));
    let chunk = sample.len().div_ceil(workers).max(1);
    let parts = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for start in (0..sample.len()).step_by(chunk) {
            let end = (start + chunk).min(sample.len());
            handles.push(scope.spawn(move || {
                let mut sums = vec![vec![0.0f32; dims]; clusters];
                let mut counts = vec![0usize; clusters];
                for vector in &sample[start..end] {
                    let norm = norm_of(vector);
                    let cluster = nearest_centroid(metric, vector, norm, centroids, centroid_norms);
                    counts[cluster] += 1;
                    for (sum, value) in sums[cluster].iter_mut().zip(vector) {
                        *sum += *value;
                    }
                }
                (sums, counts)
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("kmeans worker panicked"))
            .collect::<Vec<_>>()
    });
    let mut sums = vec![vec![0.0f32; dims]; clusters];
    let mut counts = vec![0usize; clusters];
    for (part_sums, part_counts) in parts {
        for cluster in 0..clusters {
            counts[cluster] += part_counts[cluster];
            for (sum, add) in sums[cluster].iter_mut().zip(&part_sums[cluster]) {
                *sum += *add;
            }
        }
    }
    (sums, counts)
}

fn training_vectors(metric: VectorMetric, vectors: &[(String, Vec<f32>)]) -> Vec<Vec<f32>> {
    let sample = vectors.len().min(MAX_TRAINING);
    if sample == 0 {
        return Vec::new();
    }
    let step = vectors.len().div_ceil(sample);
    let mut out = Vec::with_capacity(sample);
    for index in (0..vectors.len()).step_by(step).take(sample) {
        let mut values = vectors[index].1.clone();
        if metric == VectorMetric::Cosine {
            normalize(&mut values);
        }
        out.push(values);
    }
    out
}

fn write_u32(out: &mut impl Write, value: u32) -> io::Result<()> {
    out.write_all(&value.to_le_bytes())
}
fn write_u64(out: &mut impl Write, value: u64) -> io::Result<()> {
    out.write_all(&value.to_le_bytes())
}
fn read_u32(bytes: &[u8], position: &mut usize) -> io::Result<u32> {
    let end = *position + 4;
    if end > bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated ANN file",
        ));
    }
    let value = u32::from_le_bytes(bytes[*position..end].try_into().unwrap());
    *position = end;
    Ok(value)
}
fn read_u64(bytes: &[u8], position: &mut usize) -> io::Result<u64> {
    let end = *position + 8;
    if end > bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated ANN file",
        ));
    }
    let value = u64::from_le_bytes(bytes[*position..end].try_into().unwrap());
    *position = end;
    Ok(value)
}

/// Returns whether `path` is an ANN file this build can read.
pub fn is_compatible(path: &Path) -> bool {
    let mut header = [0u8; 12];
    File::open(path)
        .and_then(|mut file| file.read_exact(&mut header))
        .is_ok()
        && &header[..8] == MAGIC
        && u32::from_le_bytes(header[8..12].try_into().unwrap()) == VERSION
}

/// Builds an immutable IVF file atomically at `path`.
pub fn build(
    path: &Path,
    dims: usize,
    metric: VectorMetric,
    vectors: &[(String, Vec<f32>)],
    nprobe: usize,
) -> io::Result<AnnStats> {
    if dims == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "zero vector dimensions",
        ));
    }
    if let Some((id, _values)) = vectors.iter().find(|(_, values)| values.len() != dims) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("vector `{id}` has the wrong dimension"),
        ));
    }
    let sample = training_vectors(metric, vectors);
    let configured_clusters = std::env::var("NONODB_VECTOR_ANN_CLUSTERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok());
    let clusters = cluster_target(vectors.len(), sample.len(), configured_clusters);
    let nprobe = nprobe.clamp(1, clusters);

    let mut centroids: Vec<Vec<f32>> = (0..clusters)
        .map(|cluster| {
            if sample.is_empty() {
                vec![0.0; dims]
            } else {
                sample[cluster * sample.len() / clusters].clone()
            }
        })
        .collect();

    let training_workers = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
        .clamp(1, 16);
    for iteration in 0..TRAINING_ITERATIONS {
        if sample.is_empty() {
            break;
        }
        let centroid_norms: Vec<f32> = centroids.iter().map(|centroid| norm_of(centroid)).collect();
        let (sums, counts) = kmeans_iteration(
            metric,
            &sample,
            &centroids,
            &centroid_norms,
            clusters,
            dims,
            training_workers,
        );
        for (cluster, centroid) in centroids.iter_mut().enumerate() {
            if counts[cluster] == 0 {
                *centroid = sample[(iteration * 31 + cluster) % sample.len()].clone();
            } else {
                for (value, sum) in centroid.iter_mut().zip(&sums[cluster]) {
                    *value = *sum / counts[cluster] as f32;
                }
                if metric == VectorMetric::Cosine {
                    normalize(centroid);
                }
            }
        }
    }

    let centroid_norms: Vec<f32> = centroids.iter().map(|centroid| norm_of(centroid)).collect();
    let workers = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
        .clamp(1, 16.min(vectors.len().max(1)));
    let mut assigned = if workers <= 1 {
        let mut assigned: Vec<Vec<usize>> = vec![Vec::new(); clusters];
        for (index, (_, values)) in vectors.iter().enumerate() {
            let norm = norm_of(values);
            let cluster = nearest_centroid(metric, values, norm, &centroids, &centroid_norms);
            assigned[cluster].push(index);
        }
        assigned
    } else {
        let chunk = vectors.len().div_ceil(workers);
        let centroids_ref = &centroids;
        let centroid_norms_ref = &centroid_norms;
        let vectors_ref = vectors;
        let parts = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers);
            for start in (0..vectors.len()).step_by(chunk.max(1)) {
                let end = (start + chunk).min(vectors.len());
                handles.push(scope.spawn(move || {
                    let mut local: Vec<Vec<usize>> = vec![Vec::new(); clusters];
                    for (index, (_, values)) in vectors_ref[start..end].iter().enumerate() {
                        let norm = norm_of(values);
                        let cluster = nearest_centroid(
                            metric,
                            values,
                            norm,
                            centroids_ref,
                            centroid_norms_ref,
                        );
                        local[cluster].push(start + index);
                    }
                    local
                }));
            }
            handles
                .into_iter()
                .map(|handle| handle.join().expect("ANN assignment worker"))
                .collect::<Vec<_>>()
        });
        let mut assigned: Vec<Vec<usize>> = vec![Vec::new(); clusters];
        for part in parts {
            for (cluster, mut indexes) in part.into_iter().enumerate() {
                assigned[cluster].append(&mut indexes);
            }
        }
        assigned
    };
    for cluster in &mut assigned {
        cluster.sort_unstable_by(|left, right| vectors[*left].0.cmp(&vectors[*right].0));
    }

    let centroids_bytes = clusters * dims * 4;
    let offsets_offset = HEADER_SIZE + centroids_bytes;
    let data_offset = offsets_offset + (clusters + 1) * 8;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&temp)?;
    let mut out = BufWriter::new(file);
    out.write_all(MAGIC)?;
    write_u32(&mut out, VERSION)?;
    write_u32(&mut out, dims as u32)?;
    write_u32(&mut out, clusters as u32)?;
    write_u32(&mut out, nprobe as u32)?;
    write_u64(&mut out, vectors.len() as u64)?;
    out.write_all(&[match metric {
        VectorMetric::Cosine => 0,
        VectorMetric::Dot => 1,
        VectorMetric::L2 => 2,
    }])?;
    out.write_all(&[0u8; 7])?;
    for centroid in &centroids {
        for value in centroid {
            out.write_all(&value.to_le_bytes())?;
        }
    }
    for _ in 0..=clusters {
        write_u64(&mut out, 0)?;
    }

    let mut position = data_offset as u64;
    let mut metas = Vec::with_capacity(clusters);
    let mut offsets = Vec::with_capacity(clusters + 1);
    for cluster in &assigned {
        let checkpoint_count = cluster.len().div_ceil(CHECKPOINT_INTERVAL);
        let meta_size = 32 + checkpoint_count * 8;
        let meta_start = position;
        offsets.push(meta_start);
        let data_start = meta_start + meta_size as u64;
        out.write_all(&vec![0u8; meta_size])?;
        position = data_start;
        let mut checkpoints = Vec::with_capacity(checkpoint_count);
        for (entry, &index) in cluster.iter().enumerate() {
            if entry % CHECKPOINT_INTERVAL == 0 {
                checkpoints.push(position);
            }
            let (id, values) = &vectors[index];
            write_u32(&mut out, id.len() as u32)?;
            out.write_all(id.as_bytes())?;
            out.write_all(&vector::encode_vector(values))?;
            position += 4 + id.len() as u64 + 4 + values.len() as u64 * 4;
        }
        metas.push(ClusterBuildMeta {
            meta_start,
            entry_count: cluster.len(),
            data_start,
            data_end: position,
            checkpoints,
        });
    }
    offsets.push(position);
    out.flush()?;
    let mut file = out.into_inner().map_err(|error| error.into_error())?;
    file.seek(SeekFrom::Start(offsets_offset as u64))?;
    for offset in &offsets {
        write_u64(&mut file, *offset)?;
    }
    for meta in &metas {
        file.seek(SeekFrom::Start(meta.meta_start))?;
        write_u64(&mut file, meta.entry_count as u64)?;
        write_u64(&mut file, meta.data_start)?;
        write_u64(&mut file, meta.data_end)?;
        write_u64(&mut file, meta.checkpoints.len() as u64)?;
        for checkpoint in &meta.checkpoints {
            write_u64(&mut file, *checkpoint)?;
        }
    }
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temp, path)?;
    let bytes = std::fs::metadata(path)?.len();
    Ok(AnnStats {
        entries: vectors.len(),
        clusters,
        nprobe,
        bytes,
    })
}

/// Memory-mapped reader for one immutable IVF file.
pub struct AnnReader {
    map: Mmap,
    dims: usize,
    metric: VectorMetric,
    clusters: usize,
    nprobe: usize,
    entries: usize,
    centroids_offset: usize,
    offsets_offset: usize,
}

impl AnnReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let map = unsafe { Mmap::map(&file)? };
        if map.len() < HEADER_SIZE || &map[..8] != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not an ANN file",
            ));
        }
        let mut position = 8;
        let version = read_u32(&map, &mut position)?;
        if version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported ANN version",
            ));
        }
        let dims = read_u32(&map, &mut position)? as usize;
        let clusters = read_u32(&map, &mut position)? as usize;
        let nprobe = read_u32(&map, &mut position)? as usize;
        let entries = read_u64(&map, &mut position)? as usize;
        let metric = match map[position] {
            0 => VectorMetric::Cosine,
            1 => VectorMetric::Dot,
            2 => VectorMetric::L2,
            _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "bad ANN metric")),
        };
        let centroids_offset = HEADER_SIZE;
        let offsets_offset = centroids_offset + clusters * dims * 4;
        if offsets_offset + (clusters + 1) * 8 > map.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated ANN header",
            ));
        }
        Ok(Self {
            map,
            dims,
            metric,
            clusters,
            nprobe: nprobe.clamp(1, clusters.max(1)),
            entries,
            centroids_offset,
            offsets_offset,
        })
    }

    pub fn stats(&self) -> AnnStats {
        AnnStats {
            entries: self.entries,
            clusters: self.clusters,
            nprobe: self.nprobe,
            bytes: self.map.len() as u64,
        }
    }

    pub fn dimensions(&self) -> usize {
        self.dims
    }

    fn centroid(&self, cluster: usize) -> &[f32] {
        let start = self.centroids_offset + cluster * self.dims * 4;
        unsafe { std::slice::from_raw_parts(self.map[start..].as_ptr().cast::<f32>(), self.dims) }
    }

    fn offset(&self, cluster: usize) -> io::Result<u64> {
        let mut position = self.offsets_offset + cluster * 8;
        read_u64(&self.map, &mut position)
    }

    pub fn search(&self, query: &[f32], want: usize) -> io::Result<BinaryHeap<Candidate>> {
        self.search_with_nprobe(query, want, self.nprobe)
    }

    /// Searches with an explicit probe count. This allows tuning recall and
    /// latency without rebuilding an immutable sidecar.
    pub fn search_with_nprobe(
        &self,
        query: &[f32],
        want: usize,
        nprobe: usize,
    ) -> io::Result<BinaryHeap<Candidate>> {
        let mut heap: BinaryHeap<Candidate> = BinaryHeap::new();
        if want == 0 || self.entries == 0 || query.len() != self.dims {
            return Ok(heap);
        }
        let query_norm = norm_of(query);
        let mut probes = Vec::with_capacity(self.clusters);
        for cluster in 0..self.clusters {
            let info = self.cluster_info(cluster)?;
            if info.entry_count == 0 {
                continue;
            }
            let centroid = self.centroid(cluster);
            probes.push(ScoredCluster {
                distance: distance(self.metric, query, query_norm, centroid, norm_of(centroid)),
                index: cluster,
            });
        }
        probes.sort_unstable();
        probes.truncate(nprobe.clamp(1, self.clusters).min(probes.len()));
        let mut clusters = Vec::with_capacity(probes.len());
        let mut total_bytes = 0usize;
        for probe in probes {
            let info = self.cluster_info(probe.index)?;
            if info.entry_count == 0 {
                continue;
            }
            total_bytes = total_bytes.saturating_add(info.data_end - info.data_start);
            clusters.push(info);
        }
        if clusters.is_empty() {
            return Ok(heap);
        }

        // A query with a small nprobe would otherwise use only nprobe threads.
        // Split large clusters at their checkpoints so every core can
        // participate, but avoid spawning workers for tiny ranges where thread
        // overhead wins.
        let available = search_worker_limit();
        let useful = (total_bytes / MIN_BYTES_PER_WORKER).max(1);
        let workers = available.min(useful).clamp(1, MAX_SEARCH_WORKERS);
        let parts = workers.div_ceil(clusters.len()).max(1);
        let mut jobs: Vec<Vec<(usize, usize)>> = vec![Vec::new(); workers];
        let mut next_job = 0usize;
        for info in &clusters {
            for (range_start, range_end) in self.cluster_ranges(info, parts)? {
                if range_start >= range_end {
                    continue;
                }
                jobs[next_job % workers].push((range_start, range_end));
                next_job += 1;
            }
        }
        jobs.retain(|job| !job.is_empty());
        if jobs.len() <= 1 {
            let ranges = jobs.pop().unwrap_or_default();
            return self.scan_ranges(&ranges, query, query_norm, want);
        }
        let parts = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(jobs.len());
            for ranges in jobs {
                handles
                    .push(scope.spawn(move || self.scan_ranges(&ranges, query, query_norm, want)));
            }
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .unwrap_or_else(|_| Err(io::Error::other("ANN worker panicked")))
                })
                .collect::<Vec<_>>()
        });
        for part in parts {
            for candidate in part? {
                if heap.len() < want {
                    heap.push(candidate);
                } else if let Some(worst) = heap.peek() {
                    if candidate < *worst {
                        heap.pop();
                        heap.push(candidate);
                    }
                }
            }
        }
        Ok(heap)
    }

    fn cluster_info(&self, cluster: usize) -> io::Result<ClusterInfo> {
        let meta_start = self.offset(cluster)? as usize;
        let meta_end = self.offset(cluster + 1)? as usize;
        if meta_start > meta_end || meta_end > self.map.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad ANN cluster offset",
            ));
        }
        let mut position = meta_start;
        let entry_count = read_u64(&self.map, &mut position)? as usize;
        let data_start = read_u64(&self.map, &mut position)? as usize;
        let data_end = read_u64(&self.map, &mut position)? as usize;
        let checkpoint_count = read_u64(&self.map, &mut position)? as usize;
        let checkpoints_offset = position;
        let checkpoints_end =
            checkpoints_offset
                .checked_add(checkpoint_count.checked_mul(8).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "bad ANN checkpoints")
                })?)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad ANN checkpoints"))?;
        if checkpoints_end > meta_end
            || data_start < checkpoints_end
            || data_start > data_end
            || data_end > self.map.len()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad ANN cluster metadata",
            ));
        }
        if entry_count == 0 && (data_start != data_end || checkpoint_count != 0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad empty ANN cluster",
            ));
        }
        Ok(ClusterInfo {
            entry_count,
            data_start,
            data_end,
            checkpoints_offset,
            checkpoint_count,
        })
    }

    fn checkpoint(&self, info: &ClusterInfo, index: usize) -> io::Result<usize> {
        if index >= info.checkpoint_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad ANN checkpoint index",
            ));
        }
        let mut position = info.checkpoints_offset + index * 8;
        read_u64(&self.map, &mut position).map(|value| value as usize)
    }

    fn cluster_ranges(&self, info: &ClusterInfo, parts: usize) -> io::Result<Vec<(usize, usize)>> {
        if info.entry_count == 0 || info.data_start >= info.data_end {
            return Ok(Vec::new());
        }
        let splits = parts.min(info.checkpoint_count.max(1)).max(1);
        if splits <= 1 {
            return Ok(vec![(info.data_start, info.data_end)]);
        }
        let mut ranges = Vec::with_capacity(splits);
        for part in 0..splits {
            let start_index = part * info.checkpoint_count / splits;
            let end_index = (part + 1) * info.checkpoint_count / splits;
            let start = self.checkpoint(info, start_index)?;
            let end = if end_index < info.checkpoint_count {
                self.checkpoint(info, end_index)?
            } else {
                info.data_end
            };
            if start < end {
                ranges.push((start, end));
            }
        }
        Ok(ranges)
    }

    #[cfg(test)]
    fn next_record(&self, position: usize, end: usize) -> io::Result<usize> {
        let mut cursor = position;
        let id_len = read_u32(&self.map, &mut cursor)? as usize;
        let id_end = cursor
            .checked_add(id_len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated ANN entry"))?;
        let vector_end = id_end
            .checked_add(4 + self.dims * 4)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated ANN entry"))?;
        if vector_end > end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated ANN entry",
            ));
        }
        Ok(vector_end)
    }

    fn scan_ranges(
        &self,
        ranges: &[(usize, usize)],
        query: &[f32],
        query_norm: f32,
        want: usize,
    ) -> io::Result<BinaryHeap<Candidate>> {
        let mut heap: BinaryHeap<Candidate> = BinaryHeap::new();
        let vector_bytes = 4 + self.dims * 4;
        for &(start, end) in ranges {
            let mut position = start;
            while position < end {
                let id_len = read_u32(&self.map, &mut position)? as usize;
                let id_end = position + id_len;
                let vector_end = id_end + vector_bytes;
                if vector_end > end {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated ANN entry",
                    ));
                }
                let id = std::str::from_utf8(&self.map[position..id_end])
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                let value = &self.map[id_end..vector_end];
                if let Some(value_distance) =
                    vector::distance_from_bytes(self.metric, query, query_norm, value, self.dims)
                {
                    let better = match heap.peek() {
                        Some(worst) => {
                            value_distance < worst.distance
                                || (value_distance == worst.distance && id < worst.id.as_str())
                        }
                        None => true,
                    };
                    if heap.len() < want || better {
                        heap.push(Candidate {
                            distance: value_distance,
                            id: id.to_string(),
                        });
                        if heap.len() > want {
                            heap.pop();
                        }
                    }
                }
                position = vector_end;
            }
        }
        Ok(heap)
    }
}

struct CachedReader {
    len: u64,
    modified: Option<SystemTime>,
    reader: Arc<AnnReader>,
}

static READER_CACHE: OnceLock<Mutex<HashMap<PathBuf, CachedReader>>> = OnceLock::new();

fn reader_cache() -> &'static Mutex<HashMap<PathBuf, CachedReader>> {
    READER_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Opens a memory-mapped ANN reader, reusing the mapping while the file is
/// unchanged.
///
/// Mapping the multi-gigabyte sidecar per request discards the process page
/// tables for the probed cluster ranges and makes every query pay minor page
/// faults again. The cache keeps one mapping per sidecar and invalidates it
/// when the file length or modification time changes (for example after a
/// rebuild).
pub fn open_cached(path: &Path) -> io::Result<Arc<AnnReader>> {
    let metadata = std::fs::metadata(path)?;
    let len = metadata.len();
    let modified = metadata.modified().ok();
    let mut cache = reader_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(cached) = cache.get(path) {
        if cached.len == len && cached.modified == modified {
            return Ok(cached.reader.clone());
        }
    }
    let reader = Arc::new(AnnReader::open(path)?);
    cache.insert(
        path.to_path_buf(),
        CachedReader {
            len,
            modified,
            reader: reader.clone(),
        },
    );
    Ok(reader)
}

/// Drops a cached reader so the next access maps the file again.
pub fn invalidate_cache(path: &Path) {
    reader_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(path);
}

/// A tiny file-backed vector reader used by tests and tooling.
pub fn read_all(path: &Path) -> io::Result<Vec<(String, Vec<f32>)>> {
    let reader = AnnReader::open(path)?;
    let mut out = Vec::with_capacity(reader.entries);
    let vector_bytes = 4 + reader.dims * 4;
    for cluster in 0..reader.clusters {
        let info = reader.cluster_info(cluster)?;
        let mut position = info.data_start;
        while position < info.data_end {
            let len = read_u32(&reader.map, &mut position)? as usize;
            let id_end = position
                .checked_add(len)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated ANN entry"))?;
            let vector_end = id_end
                .checked_add(vector_bytes)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated ANN entry"))?;
            if vector_end > info.data_end {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated ANN entry",
                ));
            }
            let id = std::str::from_utf8(&reader.map[position..id_end])
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
                .to_string();
            let values = vector::decode_vector(&reader.map[id_end..vector_end], reader.dims)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "bad ANN vector payload")
                })?;
            out.push((id, values));
            position = vector_end;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn cluster_chunks_cover_records_without_gaps() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("vectors.ann");
        let vectors = (0..5000)
            .map(|index| {
                (
                    format!("id-{index:05}"),
                    vec![index as f32 / 5000.0, (index * 7 % 5000) as f32 / 5000.0],
                )
            })
            .collect::<Vec<_>>();
        let stats = build(&path, 2, VectorMetric::Cosine, &vectors, 1).unwrap();
        assert_eq!(stats.clusters, 70);
        let reader = AnnReader::open(&path).unwrap();
        let mut checked = 0usize;
        let mut largest = (0usize, 0usize);
        for cluster in 0..reader.clusters {
            let info = reader.cluster_info(cluster).unwrap();
            if info.entry_count == 0 {
                continue;
            }
            if info.entry_count > largest.1 {
                largest = (cluster, info.entry_count);
            }
            let ranges = reader.cluster_ranges(&info, 4).unwrap();
            assert!(!ranges.is_empty());
            assert_eq!(ranges.first().unwrap().0, info.data_start);
            assert_eq!(ranges.last().unwrap().1, info.data_end);
            for window in ranges.windows(2) {
                assert_eq!(window[0].1, window[1].0);
            }
            assert_eq!(
                ranges.iter().map(|range| range.1 - range.0).sum::<usize>(),
                info.data_end - info.data_start
            );
            for (start, end) in ranges {
                let mut position = start;
                while position < end {
                    position = reader.next_record(position, end).unwrap();
                }
                assert_eq!(position, end);
            }
            checked += 1;
        }
        assert!(checked > 0);
        let (largest_cluster, largest_count) = largest;
        assert!(largest_count >= 8);
        let info = reader.cluster_info(largest_cluster).unwrap();
        let query = [0.37, 0.61];
        let whole = reader
            .scan_ranges(
                &[(info.data_start, info.data_end)],
                &query,
                norm_of(&query),
                largest_count,
            )
            .unwrap();
        let ranges = reader.cluster_ranges(&info, 4).unwrap();
        let chunked = reader
            .scan_ranges(&ranges, &query, norm_of(&query), largest_count)
            .unwrap();
        assert_eq!(chunked.len(), whole.len());
        let mut whole_ids = whole
            .into_iter()
            .map(|candidate| candidate.id)
            .collect::<Vec<_>>();
        let mut chunked_ids = chunked
            .into_iter()
            .map(|candidate| candidate.id)
            .collect::<Vec<_>>();
        whole_ids.sort();
        chunked_ids.sort();
        assert_eq!(whole_ids, chunked_ids);
    }

    #[test]
    fn ann_finds_the_exact_nearest_vector_in_a_small_set() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("vectors.ann");
        let vectors = vec![
            ("a".to_string(), vec![1.0, 0.0]),
            ("b".to_string(), vec![0.9, 0.1]),
            ("c".to_string(), vec![0.0, 1.0]),
            ("d".to_string(), vec![-1.0, 0.0]),
        ];
        let stats = build(&path, 2, VectorMetric::Cosine, &vectors, 2).unwrap();
        assert_eq!(stats.entries, 4);
        assert!(is_compatible(&path));
        let reader = AnnReader::open(&path).unwrap();
        let heap = reader.search(&[1.0, 0.0], 2).unwrap();
        let mut found: Vec<_> = heap.into_iter().map(|candidate| candidate.id).collect();
        found.sort();
        assert_eq!(found, ["a", "b"]);
        assert_eq!(read_all(&path).unwrap().len(), 4);
    }

    #[test]
    fn configured_cluster_count_is_a_target() {
        assert_eq!(cluster_target(500_000, 100_000, None), 707);
        assert_eq!(cluster_target(500_000, 100_000, Some(4_096)), 4_096);
        assert_eq!(cluster_target(10, 10, Some(4_096)), 10);
        assert_eq!(cluster_target(100_000_000, 100_000, None), 4_096);
        assert_eq!(cluster_target(100_000_000, 100_000, Some(8_192)), 8_192);
    }

    #[test]
    fn cached_reader_is_reused_and_reloaded_after_rebuild() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("vectors.ann");
        let first = (0..32)
            .map(|index| (format!("id-{index:03}"), vec![index as f32, 1.0]))
            .collect::<Vec<_>>();
        build(&path, 2, VectorMetric::Cosine, &first, 1).unwrap();
        let cached = open_cached(&path).unwrap();
        assert_eq!(cached.stats().entries, 32);
        assert!(Arc::ptr_eq(&cached, &open_cached(&path).unwrap()));

        let second = (0..64)
            .map(|index| (format!("id-{index:03}"), vec![index as f32, 1.0]))
            .collect::<Vec<_>>();
        build(&path, 2, VectorMetric::Cosine, &second, 1).unwrap();
        assert_eq!(open_cached(&path).unwrap().stats().entries, 64);
    }
}
