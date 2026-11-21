//! Immutable IVF sidecar for approximate vector search.
//!
//! The sidecar is built from the exact LMDB vector index. It contains a small
//! set of centroids, cluster offsets, and the original `_id`/vector pairs.
//! Queries probe the nearest clusters and keep a bounded top-K heap. The file
//! is memory-mapped lazily; there are no background threads or warm caches.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use memmap2::Mmap;

use crate::vector::{self, Candidate, VectorMetric};

const MAGIC: &[u8; 8] = b"NONOANN1";
const VERSION: u32 = 1;
const HEADER_SIZE: usize = 40;
const MAX_CLUSTERS: usize = 512;
const MAX_TRAINING: usize = 100_000;
const TRAINING_ITERATIONS: usize = 8;

#[derive(Debug, Clone, Copy)]
pub struct AnnStats {
    pub entries: usize,
    pub clusters: usize,
    pub nprobe: usize,
    pub bytes: u64,
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
    let mut dot = 0.0f32;
    let mut squared = 0.0f32;
    for (left, right) in query.iter().zip(stored.iter()) {
        dot += left * right;
        let delta = left - right;
        squared += delta * delta;
    }
    let value = match metric {
        VectorMetric::Cosine => {
            if query_norm <= f32::EPSILON || stored_norm <= f32::EPSILON {
                1.0
            } else {
                1.0 - (dot / (query_norm * stored_norm)).clamp(-1.0, 1.0)
            }
        }
        VectorMetric::Dot => -dot,
        VectorMetric::L2 => squared,
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
) -> usize {
    let mut best = 0usize;
    let mut best_distance = f32::INFINITY;
    for (index, centroid) in centroids.iter().enumerate() {
        let value = distance(metric, vector, vector_norm, centroid, norm_of(centroid));
        if value < best_distance {
            best = index;
            best_distance = value;
        }
    }
    best
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
    let clusters = vectors
        .len()
        .max(1)
        .isqrt()
        .clamp(1, MAX_CLUSTERS)
        .min(sample.len().max(1));
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

    for iteration in 0..TRAINING_ITERATIONS {
        if sample.is_empty() {
            break;
        }
        let mut sums = vec![vec![0.0f32; dims]; clusters];
        let mut counts = vec![0usize; clusters];
        for vector in &sample {
            let norm = norm_of(vector);
            let cluster = nearest_centroid(metric, vector, norm, &centroids);
            counts[cluster] += 1;
            for (sum, value) in sums[cluster].iter_mut().zip(vector) {
                *sum += *value;
            }
        }
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

    let mut assigned: Vec<Vec<usize>> = vec![Vec::new(); clusters];
    for (index, (_, values)) in vectors.iter().enumerate() {
        let norm = norm_of(values);
        let cluster = nearest_centroid(metric, values, norm, &centroids);
        assigned[cluster].push(index);
    }
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

    let mut offsets = Vec::with_capacity(clusters + 1);
    let mut data_pos = 0u64;
    for cluster in &assigned {
        offsets.push(data_offset as u64 + data_pos);
        for &index in cluster {
            let (id, values) = &vectors[index];
            write_u32(&mut out, id.len() as u32)?;
            out.write_all(id.as_bytes())?;
            out.write_all(&vector::encode_vector(values))?;
            data_pos += 4 + id.len() as u64 + 4 + values.len() as u64 * 4;
        }
    }
    offsets.push(data_offset as u64 + data_pos);
    out.flush()?;
    let mut file = out.into_inner().map_err(|error| error.into_error())?;
    file.seek(SeekFrom::Start(offsets_offset as u64))?;
    for offset in &offsets {
        write_u64(&mut file, *offset)?;
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

    fn centroid(&self, cluster: usize) -> &[f32] {
        let start = self.centroids_offset + cluster * self.dims * 4;
        unsafe { std::slice::from_raw_parts(self.map[start..].as_ptr().cast::<f32>(), self.dims) }
    }

    fn offset(&self, cluster: usize) -> io::Result<u64> {
        let mut position = self.offsets_offset + cluster * 8;
        read_u64(&self.map, &mut position)
    }

    pub fn search(&self, query: &[f32], want: usize) -> io::Result<BinaryHeap<Candidate>> {
        let mut heap: BinaryHeap<Candidate> = BinaryHeap::new();
        if want == 0 || self.entries == 0 || query.len() != self.dims {
            return Ok(heap);
        }
        let query_norm = norm_of(query);
        let mut probes = Vec::with_capacity(self.clusters);
        for cluster in 0..self.clusters {
            let centroid = self.centroid(cluster);
            probes.push(ScoredCluster {
                distance: distance(self.metric, query, query_norm, centroid, norm_of(centroid)),
                index: cluster,
            });
        }
        probes.sort_unstable();
        probes.truncate(self.nprobe.min(probes.len()));
        let clusters: Vec<usize> = probes.into_iter().map(|probe| probe.index).collect();
        let workers = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1)
            .min(clusters.len())
            .max(1);
        if workers <= 1 {
            return self.scan_clusters(&clusters, query, query_norm, want);
        }
        let chunk = clusters.len().div_ceil(workers);
        let parts = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for slice in clusters.chunks(chunk) {
                handles
                    .push(scope.spawn(move || self.scan_clusters(slice, query, query_norm, want)));
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

    fn scan_clusters(
        &self,
        clusters: &[usize],
        query: &[f32],
        query_norm: f32,
        want: usize,
    ) -> io::Result<BinaryHeap<Candidate>> {
        let mut heap: BinaryHeap<Candidate> = BinaryHeap::new();
        let vector_bytes = 4 + self.dims * 4;
        for &cluster in clusters {
            let start = self.offset(cluster)? as usize;
            let end = self.offset(cluster + 1)? as usize;
            if start > end || end > self.map.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bad ANN cluster offset",
                ));
            }
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

/// A tiny file-backed vector reader used by tests and tooling.
pub fn read_all(path: &Path) -> io::Result<Vec<(String, Vec<f32>)>> {
    let bytes = std::fs::read(path)?;
    if bytes.len() < HEADER_SIZE || &bytes[..8] != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not an ANN file",
        ));
    }
    let dims = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let clusters = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let entries = u64::from_le_bytes(bytes[24..32].try_into().unwrap()) as usize;
    let mut out = Vec::with_capacity(entries);
    let vector_bytes = 4 + dims * 4;
    let mut position = HEADER_SIZE + clusters * dims * 4 + (clusters + 1) * 8;
    while position < bytes.len() && out.len() < entries {
        let len = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap()) as usize;
        position += 4;
        let id = std::str::from_utf8(&bytes[position..position + len])
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            .to_string();
        position += len;
        let value = &bytes[position..position + vector_bytes];
        let mut values = Vec::with_capacity(dims);
        for index in 0..dims {
            let start = 4 + index * 4;
            values.push(f32::from_le_bytes(
                value[start..start + 4].try_into().unwrap(),
            ));
        }
        out.push((id, values));
        position += vector_bytes;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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
        let reader = AnnReader::open(&path).unwrap();
        let heap = reader.search(&[1.0, 0.0], 2).unwrap();
        let mut found: Vec<_> = heap.into_iter().map(|candidate| candidate.id).collect();
        found.sort();
        assert_eq!(found, ["a", "b"]);
        assert_eq!(read_all(&path).unwrap().len(), 4);
    }
}
