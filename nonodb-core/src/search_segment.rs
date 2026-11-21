//! Immutable, block-based text-search segments.
//!
//! This is the first internal search backend for large text collections. It
//! deliberately has no background threads and no owned cache: a segment is a
//! single file read on demand, so an idle daemon pays only for the open file
//! descriptor and a small term-index header.
//!
//! Layout (little-endian):
//!
//! ```text
//! header (72 bytes)
//! doc ids: u32 count, (count + 1) x u32 offsets, concatenated UTF-8 ids
//! terms:   u16 len, term, u32 df, u64 postings offset, u32 postings len,
//!          u32 block count, f32 max term impact
//! index:   u16 len, first term, u64 term block offset, u32 term block len
//! postings: per term, blocks of at most 128 postings
//! ```
//!
//! A posting block is `u32 first_doc, u16 count, f32 max_tf_norm, u32 data_len`
//! followed by delta-encoded `(doc_delta, tf, doc_len)` varints. `max_tf_norm`
//! excludes IDF, so the reader can apply the term IDF without storing it in the
//! index. The format is intentionally append-only and rebuildable from LMDB.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::search::{self, BM25_B, BM25_K1};

const MAGIC: &[u8; 8] = b"NODBSEG1";
const VERSION: u32 = 2;
const HEADER_LEN: u64 = 72;
const TERM_BLOCK_TERMS: usize = 64;
const POSTING_BLOCK: usize = 128;
const CHUNK_RECORDS: usize = 4_000_000;

#[derive(Clone, Copy, Debug)]
struct Header {
    doc_count: u32,
    term_count: u32,
    term_block_count: u32,
    total_doc_len: u64,
    docids_off: u64,
    terms_off: u64,
    term_index_off: u64,
    postings_off: u64,
}

impl Header {
    fn read<R: Read>(mut r: R) -> io::Result<Self> {
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a NonoDB segment",
            ));
        }
        let version = read_u32(&mut r)?;
        if version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported segment version {version}"),
            ));
        }
        Ok(Self {
            doc_count: read_u32(&mut r)?,
            term_count: read_u32(&mut r)?,
            term_block_count: read_u32(&mut r)?,
            total_doc_len: read_u64(&mut r)?,
            docids_off: read_u64(&mut r)?,
            terms_off: read_u64(&mut r)?,
            term_index_off: read_u64(&mut r)?,
            postings_off: read_u64(&mut r)?,
        })
    }

    fn write<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(MAGIC)?;
        write_u32(w, VERSION)?;
        write_u32(w, self.doc_count)?;
        write_u32(w, self.term_count)?;
        write_u32(w, self.term_block_count)?;
        write_u64(w, self.total_doc_len)?;
        write_u64(w, self.docids_off)?;
        write_u64(w, self.terms_off)?;
        write_u64(w, self.term_index_off)?;
        write_u64(w, self.postings_off)?;
        w.write_all(&[0u8; 8])?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct PostingMeta {
    df: u32,
    offset: u64,
    len: u32,
    blocks: u32,
    max_tf_norm: f64,
}

#[derive(Clone, Copy, Debug)]
struct Record {
    term: u32,
    doc: u32,
    tf: u32,
    dl: u32,
}

#[derive(Clone, Copy, Debug)]
struct Posting {
    doc: u32,
    tf: u32,
    dl: u32,
}

#[derive(Debug)]
struct Chunk {
    path: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MergeItem {
    term: u32,
    doc: u32,
    tf: u32,
    dl: u32,
    chunk: usize,
}

impl Ord for MergeItem {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.term, self.doc, self.chunk).cmp(&(other.term, other.doc, other.chunk))
    }
}

impl PartialOrd for MergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Statistics returned by a successful build.
#[derive(Clone, Copy, Debug)]
pub struct SegmentStats {
    pub documents: u32,
    pub terms: u32,
    pub postings: u64,
    pub bytes: u64,
}

/// Builds one immutable segment file.
///
/// Documents must be added before [`finish`](Self::finish). Postings are spilled
/// in sorted chunks so the builder does not need to keep the full posting set in
/// memory. Temporary files live next to the destination, which is normally the
/// same ZFS filesystem as the benchmark data.
pub struct SegmentBuilder {
    path: PathBuf,
    work_dir: PathBuf,
    doc_ids: Vec<String>,
    doc_index: HashMap<String, u32>,
    terms: Vec<String>,
    term_index: HashMap<String, u32>,
    chunk: Vec<Record>,
    chunks: Vec<Chunk>,
    total_doc_len: u64,
}

impl SegmentBuilder {
    pub fn new(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let work_dir = path.with_extension("build");
        if work_dir.exists() {
            fs::remove_dir_all(&work_dir)?;
        }
        fs::create_dir_all(&work_dir)?;
        Ok(Self {
            path,
            work_dir,
            doc_ids: Vec::new(),
            doc_index: HashMap::new(),
            terms: Vec::new(),
            term_index: HashMap::new(),
            chunk: Vec::new(),
            chunks: Vec::new(),
            total_doc_len: 0,
        })
    }

    pub fn add_document(&mut self, id: &str, text: &str) -> io::Result<()> {
        if self.doc_index.contains_key(id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("duplicate document id `{id}`"),
            ));
        }
        let doc = self.doc_ids.len() as u32;
        self.doc_index.insert(id.to_string(), doc);
        self.doc_ids.push(id.to_string());

        let tokens = search::tokenize(text);
        let dl = tokens.len() as u32;
        self.total_doc_len += u64::from(dl);

        // A local map avoids allocating one String per posting record.
        let mut freqs: HashMap<String, u32> = HashMap::new();
        for token in tokens {
            *freqs.entry(token).or_insert(0) += 1;
        }
        for (term, tf) in freqs {
            let term_id = match self.term_index.get(&term) {
                Some(id) => *id,
                None => {
                    let id = self.terms.len() as u32;
                    self.term_index.insert(term.clone(), id);
                    self.terms.push(term);
                    id
                }
            };
            self.chunk.push(Record {
                term: term_id,
                doc,
                tf,
                dl,
            });
            if self.chunk.len() >= CHUNK_RECORDS {
                self.spill_chunk()?;
            }
        }
        Ok(())
    }

    fn spill_chunk(&mut self) -> io::Result<()> {
        if self.chunk.is_empty() {
            return Ok(());
        }
        self.chunk.sort_unstable_by_key(|r| (r.term, r.doc));
        let path = self
            .work_dir
            .join(format!("chunk-{:06}.bin", self.chunks.len()));
        let mut file = io::BufWriter::new(File::create(&path)?);
        for record in &self.chunk {
            write_u32(&mut file, record.term)?;
            write_u32(&mut file, record.doc)?;
            write_u32(&mut file, record.tf)?;
            write_u32(&mut file, record.dl)?;
        }
        file.flush()?;
        self.chunks.push(Chunk { path });
        self.chunk.clear();
        Ok(())
    }

    /// Finish the segment, write it atomically, and return its statistics.
    pub fn finish(mut self) -> io::Result<SegmentStats> {
        self.spill_chunk()?;
        let doc_count = self.doc_ids.len() as u32;
        let avgdl = if doc_count == 0 {
            1.0
        } else {
            self.total_doc_len as f64 / f64::from(doc_count)
        };

        let postings_path = self.work_dir.join("postings.tmp");
        let mut postings = File::create(&postings_path)?;
        let mut postings_pos = 0u64;
        let mut metas: Vec<PostingMeta> = Vec::with_capacity(self.terms.len());
        let mut current_term: Option<u32> = None;
        let mut term_start = 0u64;
        let mut term_df = 0u32;
        let mut term_blocks = 0u32;
        let mut term_max = 0.0f64;
        let mut block: Vec<Posting> = Vec::with_capacity(POSTING_BLOCK);

        let mut readers: Vec<io::BufReader<File>> = Vec::with_capacity(self.chunks.len());
        for chunk in &self.chunks {
            readers.push(io::BufReader::new(File::open(&chunk.path)?));
        }
        let mut heap: BinaryHeap<Reverse<MergeItem>> = BinaryHeap::new();
        for (idx, reader) in readers.iter_mut().enumerate() {
            if let Some(item) = read_record(reader, idx)? {
                heap.push(Reverse(item));
            }
        }

        while let Some(Reverse(item)) = heap.pop() {
            if current_term != Some(item.term) {
                flush_block(
                    &mut postings,
                    &mut postings_pos,
                    &mut block,
                    &mut term_blocks,
                    &mut term_max,
                    avgdl,
                )?;
                if let Some(term) = current_term {
                    let meta = PostingMeta {
                        df: term_df,
                        offset: term_start,
                        len: (postings_pos - term_start) as u32,
                        blocks: term_blocks,
                        max_tf_norm: term_max,
                    };
                    if metas.len() != term as usize {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "term order mismatch while building segment",
                        ));
                    }
                    metas.push(meta);
                }
                current_term = Some(item.term);
                term_start = postings_pos;
                term_df = 0;
                term_blocks = 0;
                term_max = 0.0;
            }

            term_df += 1;
            block.push(Posting {
                doc: item.doc,
                tf: item.tf,
                dl: item.dl,
            });
            if block.len() >= POSTING_BLOCK {
                flush_block(
                    &mut postings,
                    &mut postings_pos,
                    &mut block,
                    &mut term_blocks,
                    &mut term_max,
                    avgdl,
                )?;
            }

            if let Some(next) = read_record(&mut readers[item.chunk], item.chunk)? {
                heap.push(Reverse(next));
            }
        }

        flush_block(
            &mut postings,
            &mut postings_pos,
            &mut block,
            &mut term_blocks,
            &mut term_max,
            avgdl,
        )?;
        if let Some(term) = current_term {
            let meta = PostingMeta {
                df: term_df,
                offset: term_start,
                len: (postings_pos - term_start) as u32,
                blocks: term_blocks,
                max_tf_norm: term_max,
            };
            if metas.len() != term as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "term order mismatch while building segment",
                ));
            }
            metas.push(meta);
        }
        if metas.len() != self.terms.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not all terms produced postings",
            ));
        }
        postings.flush()?;

        let mut term_entries: Vec<(String, PostingMeta)> =
            self.terms.into_iter().zip(metas).collect();
        term_entries.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));

        let mut doc_offsets = Vec::with_capacity(self.doc_ids.len() + 1);
        let mut doc_strings = Vec::new();
        for id in &self.doc_ids {
            doc_offsets.push(doc_strings.len() as u32);
            doc_strings.extend_from_slice(id.as_bytes());
        }
        doc_offsets.push(doc_strings.len() as u32);
        let docids_size = 4u64 + (doc_offsets.len() as u64) * 4 + doc_strings.len() as u64;

        let terms_size = term_entries
            .iter()
            .map(|(term, _)| 2 + term.len() as u64 + 4 + 8 + 4 + 4 + 4)
            .sum::<u64>();
        let term_block_count = term_entries.len().div_ceil(TERM_BLOCK_TERMS) as u32;
        let mut index_size = 0u64;
        for chunk in term_entries.chunks(TERM_BLOCK_TERMS) {
            index_size += 2 + chunk[0].0.len() as u64 + 8 + 4;
        }

        let docids_off = HEADER_LEN;
        let terms_off = docids_off + docids_size;
        let term_index_off = terms_off + terms_size;
        let postings_off = term_index_off + index_size;
        let header = Header {
            doc_count,
            term_count: term_entries.len() as u32,
            term_block_count,
            total_doc_len: self.total_doc_len,
            docids_off,
            terms_off,
            term_index_off,
            postings_off,
        };

        let final_tmp = self.path.with_extension("tmp");
        let mut out = io::BufWriter::new(File::create(&final_tmp)?);
        header.write(&mut out)?;
        write_u32(&mut out, doc_offsets.len() as u32 - 1)?;
        for offset in &doc_offsets {
            write_u32(&mut out, *offset)?;
        }
        out.write_all(&doc_strings)?;

        let mut index_entries: Vec<(String, u64, u32)> =
            Vec::with_capacity(term_block_count as usize);
        let mut terms_written = 0u64;
        for chunk in term_entries.chunks(TERM_BLOCK_TERMS) {
            let first = chunk[0].0.clone();
            let block_start = terms_written;
            for (term, meta) in chunk {
                write_u16(&mut out, term.len() as u16)?;
                out.write_all(term.as_bytes())?;
                write_u32(&mut out, meta.df)?;
                write_u64(&mut out, postings_off + meta.offset)?;
                write_u32(&mut out, meta.len)?;
                write_u32(&mut out, meta.blocks)?;
                write_f32(&mut out, meta.max_tf_norm as f32)?;
                terms_written += 2 + term.len() as u64 + 4 + 8 + 4 + 4 + 4;
            }
            index_entries.push((
                first,
                terms_off + block_start,
                (terms_written - block_start) as u32,
            ));
        }
        for (first, offset, len) in &index_entries {
            write_u16(&mut out, first.len() as u16)?;
            out.write_all(first.as_bytes())?;
            write_u64(&mut out, *offset)?;
            write_u32(&mut out, *len)?;
        }
        out.flush()?;
        io::copy(&mut File::open(&postings_path)?, &mut out)?;
        out.flush()?;
        let file = out.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        drop(readers);
        drop(postings);
        fs::rename(&final_tmp, &self.path)?;
        let bytes = fs::metadata(&self.path)?.len();
        fs::remove_dir_all(&self.work_dir)?;

        Ok(SegmentStats {
            documents: doc_count,
            terms: header.term_count,
            postings: term_entries
                .iter()
                .map(|(_, meta)| u64::from(meta.df))
                .sum(),
            bytes,
        })
    }
}

fn read_record(reader: &mut io::BufReader<File>, chunk: usize) -> io::Result<Option<MergeItem>> {
    let mut buf = [0u8; 16];
    match reader.read_exact(&mut buf) {
        Ok(()) => Ok(Some(MergeItem {
            term: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            doc: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            tf: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            dl: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            chunk,
        })),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

fn flush_block(
    out: &mut File,
    out_pos: &mut u64,
    block: &mut Vec<Posting>,
    blocks: &mut u32,
    term_max: &mut f64,
    avgdl: f64,
) -> io::Result<()> {
    if block.is_empty() {
        return Ok(());
    }
    let first_doc = block[0].doc;
    let mut data = Vec::with_capacity(block.len() * 4);
    let mut prev_doc = first_doc;
    let mut max_tf_norm = 0.0f64;
    for (i, posting) in block.iter().enumerate() {
        let delta = if i == 0 { 0 } else { posting.doc - prev_doc };
        write_varint(&mut data, u64::from(delta));
        write_varint(&mut data, u64::from(posting.tf));
        write_varint(&mut data, u64::from(posting.dl));
        prev_doc = posting.doc;
        let tf = f64::from(posting.tf);
        let dl = f64::from(posting.dl);
        let norm = tf * (BM25_K1 + 1.0) / (tf + BM25_K1 * (1.0 - BM25_B + BM25_B * dl / avgdl));
        max_tf_norm = max_tf_norm.max(norm);
    }
    let last_doc = block[block.len() - 1].doc;
    write_u32(out, first_doc)?;
    write_u32(out, last_doc)?;
    write_u16(out, block.len() as u16)?;
    write_f32(out, max_tf_norm as f32)?;
    write_u32(out, data.len() as u32)?;
    write_u16(out, 0)?;
    out.write_all(&data)?;
    *out_pos += 20 + data.len() as u64;
    *blocks += 1;
    *term_max = term_max.max(max_tf_norm);
    block.clear();
    Ok(())
}

/// A single ranked hit. The caller resolves `id` to a document in LMDB.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchHit {
    pub id: String,
    pub score: f64,
}

#[derive(Clone, Debug)]
struct ScoredDoc {
    doc: u32,
    score: f64,
}

impl PartialEq for ScoredDoc {
    fn eq(&self, other: &Self) -> bool {
        self.doc == other.doc && self.score.total_cmp(&other.score) == Ordering::Equal
    }
}
impl Eq for ScoredDoc {}
impl Ord for ScoredDoc {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.doc.cmp(&other.doc))
    }
}
impl PartialOrd for ScoredDoc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug)]
struct TermEntry {
    df: u32,
    postings_off: u64,
    block_count: u32,
    max_tf_norm: f64,
}

#[derive(Clone, Debug)]
struct TermIndexEntry {
    first: String,
    offset: u64,
    len: u32,
}

/// Read-only handle to one immutable segment.
pub struct SegmentReader {
    file: File,
    header: Header,
    index: Vec<TermIndexEntry>,
}

impl SegmentReader {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        let mut header_reader = &file;
        let header = Header::read(&mut header_reader)?;
        let mut index_buf = vec![0u8; (header.postings_off - header.term_index_off) as usize];
        read_at(&file, header.term_index_off, &mut index_buf)?;
        let mut index = Vec::with_capacity(header.term_block_count as usize);
        let mut pos = 0usize;
        for _ in 0..header.term_block_count {
            let first = read_str(&index_buf, &mut pos)?;
            let offset = read_u64_buf(&index_buf, &mut pos)?;
            let len = read_u32_buf(&index_buf, &mut pos)?;
            index.push(TermIndexEntry { first, offset, len });
        }
        Ok(Self {
            file,
            header,
            index,
        })
    }

    pub fn documents(&self) -> u32 {
        self.header.doc_count
    }

    pub fn terms(&self) -> u32 {
        self.header.term_count
    }

    pub fn doc_id(&self, doc: u32) -> io::Result<String> {
        if doc >= self.header.doc_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "doc id out of range",
            ));
        }
        let count = self.header.doc_count as u64;
        let offsets_start = self.header.docids_off + 4;
        let strings_start = offsets_start + (count + 1) * 4;
        let mut a = [0u8; 4];
        let mut b = [0u8; 4];
        read_at(&self.file, offsets_start + u64::from(doc) * 4, &mut a)?;
        read_at(&self.file, offsets_start + u64::from(doc + 1) * 4, &mut b)?;
        let start = u32::from_le_bytes(a) as u64;
        let end = u32::from_le_bytes(b) as u64;
        if end < start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid doc id offsets",
            ));
        }
        let mut buf = vec![0u8; (end - start) as usize];
        read_at(&self.file, strings_start + start, &mut buf)?;
        String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn term(&self, term: &str) -> io::Result<Option<TermEntry>> {
        if self.index.is_empty() {
            return Ok(None);
        }
        let mut lo = 0usize;
        let mut hi = self.index.len();
        while lo + 1 < hi {
            let mid = (lo + hi) / 2;
            if self.index[mid].first.as_str() <= term {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        let block = &self.index[lo];
        let mut buf = vec![0u8; block.len as usize];
        read_at(&self.file, block.offset, &mut buf)?;
        let mut pos = 0usize;
        while pos < buf.len() {
            let name = read_str(&buf, &mut pos)?;
            let df = read_u32_buf(&buf, &mut pos)?;
            let postings_off = read_u64_buf(&buf, &mut pos)?;
            let _postings_len = read_u32_buf(&buf, &mut pos)?;
            let block_count = read_u32_buf(&buf, &mut pos)?;
            let max_tf_norm = f64::from(read_f32_buf(&buf, &mut pos)?);
            if name == term {
                return Ok(Some(TermEntry {
                    df,
                    postings_off,
                    block_count,
                    max_tf_norm,
                }));
            }
        }
        Ok(None)
    }

    pub fn search(&self, query: &str, limit: usize) -> io::Result<Vec<SearchHit>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut terms = search::tokenize(query);
        terms.sort();
        terms.dedup();
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        let avgdl = if self.header.doc_count == 0 {
            1.0
        } else {
            self.header.total_doc_len as f64 / f64::from(self.header.doc_count)
        };
        let n = f64::from(self.header.doc_count);
        let mut entries = Vec::with_capacity(terms.len());
        for term in &terms {
            match self.term(term)? {
                Some(entry) => {
                    let df = f64::from(entry.df);
                    let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
                    entries.push((term.clone(), entry, idf));
                }
                None => return Ok(Vec::new()),
            }
        }

        // Drive AND queries from the rarest term. The remaining cursors are
        // checked in increasing document-frequency order, so a candidate that
        // fails a moderately rare term is rejected before we touch common
        // stopword-like postings.
        entries.sort_unstable_by_key(|(_, entry, _)| entry.df);

        let mut heap: BinaryHeap<Reverse<ScoredDoc>> = BinaryHeap::with_capacity(limit + 1);
        if entries.len() == 1 {
            let (_, entry, idf) = entries.pop().unwrap();
            let mut cursor = Cursor::new(self, entry, idf)?;
            self.search_single(&mut cursor, avgdl, limit, &mut heap)?;
        } else {
            let mut cursors = Vec::with_capacity(entries.len());
            for (_, entry, idf) in entries {
                cursors.push(Cursor::new(self, entry, idf)?);
            }
            self.search_and_rarest(cursors, avgdl, limit, &mut heap)?;
        }

        let mut hits = heap
            .into_iter()
            .map(|Reverse(hit)| {
                self.doc_id(hit.doc).map(|id| SearchHit {
                    id,
                    score: hit.score,
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        hits.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
        Ok(hits)
    }

    fn search_single(
        &self,
        cursor: &mut Cursor<'_>,
        avgdl: f64,
        limit: usize,
        heap: &mut BinaryHeap<Reverse<ScoredDoc>>,
    ) -> io::Result<()> {
        loop {
            let threshold = if heap.len() >= limit {
                heap.peek().map(|r| r.0.score).unwrap_or(0.0)
            } else {
                0.0
            };
            if heap.len() >= limit && cursor.block_max() <= threshold {
                if !cursor.skip_block()? {
                    break;
                }
                continue;
            }
            let Some(posting) = cursor.current() else {
                break;
            };
            let score = bm25(cursor.idf, posting.tf, posting.dl, avgdl);
            push_hit(
                heap,
                ScoredDoc {
                    doc: posting.doc,
                    score,
                },
                limit,
            );
            cursor.advance()?;
        }
        Ok(())
    }

    fn search_wand(
        &self,
        mut cursors: Vec<Cursor<'_>>,
        avgdl: f64,
        limit: usize,
        heap: &mut BinaryHeap<Reverse<ScoredDoc>>,
    ) -> io::Result<()> {
        let term_count = cursors.len();
        loop {
            if cursors.iter().any(|cursor| cursor.current().is_none()) {
                break;
            }
            cursors.sort_unstable_by_key(|cursor| cursor.current().map(|p| p.doc));
            let threshold = if heap.len() >= limit {
                heap.peek().map(|r| r.0.score).unwrap_or(0.0)
            } else {
                0.0
            };
            let mut upper = 0.0f64;
            let mut pivot = 0u32;
            let mut pivot_idx = 0usize;
            let mut found = false;
            for (idx, cursor) in cursors.iter().enumerate() {
                upper += cursor.term_max;
                if upper > threshold {
                    pivot = cursor.current().unwrap().doc;
                    pivot_idx = idx;
                    found = true;
                    break;
                }
            }
            if !found {
                break;
            }
            let first_doc = cursors[0].current().unwrap().doc;
            if first_doc == pivot {
                let mut matched = 0usize;
                let mut score = 0.0f64;
                for cursor in cursors.iter_mut() {
                    if cursor.current().map(|p| p.doc) == Some(pivot) {
                        let posting = cursor.current().unwrap();
                        score += bm25(cursor.idf, posting.tf, posting.dl, avgdl);
                        matched += 1;
                        cursor.advance()?;
                    }
                }
                if matched == term_count {
                    push_hit(heap, ScoredDoc { doc: pivot, score }, limit);
                }
            } else {
                for cursor in cursors.iter_mut().take(pivot_idx) {
                    if cursor.current().map(|p| p.doc) < Some(pivot) {
                        cursor.skip_to(pivot)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn search_and_rarest(
        &self,
        mut cursors: Vec<Cursor<'_>>,
        avgdl: f64,
        limit: usize,
        heap: &mut BinaryHeap<Reverse<ScoredDoc>>,
    ) -> io::Result<()> {
        // Walk the shortest posting list and skip the other cursors to each
        // candidate. This turns an AND query into one scan of its rarest term
        // instead of touching every posting for every term.
        let doc_count = u64::from(self.header.doc_count);
        if u64::from(cursors[0].df).saturating_mul(20) > doc_count {
            return self.search_wand(cursors, avgdl, limit, heap);
        }
        let mut rare_cursor = cursors.remove(0);
        while let Some(posting) = rare_cursor.current() {
            // Align the next-rarest cursor with this rare block. If the block
            // ends before that cursor, no document in the block can match all
            // terms, so skip it without decoding or checking each posting.
            let block_last = rare_cursor.block_last;
            if let Some(second) = cursors.first_mut() {
                if !second.skip_to(posting.doc)? {
                    break;
                }
                if second.current().map(|p| p.doc).unwrap_or(u32::MAX) > block_last {
                    if !rare_cursor.skip_block_fast()? {
                        break;
                    }
                    continue;
                }
            }

            while let Some(posting) = rare_cursor.current() {
                if posting.doc > block_last {
                    break;
                }
                let doc = posting.doc;
                let mut score = bm25(rare_cursor.idf, posting.tf, posting.dl, avgdl);
                let mut matched = true;
                for cursor in cursors.iter_mut() {
                    if !cursor.skip_to(doc)? || cursor.current().map(|p| p.doc) != Some(doc) {
                        matched = false;
                        break;
                    }
                    let other = cursor.current().unwrap();
                    score += bm25(cursor.idf, other.tf, other.dl, avgdl);
                }
                if matched {
                    push_hit(heap, ScoredDoc { doc, score }, limit);
                }
                rare_cursor.advance()?;
            }
        }
        Ok(())
    }
}

fn push_hit(heap: &mut BinaryHeap<Reverse<ScoredDoc>>, hit: ScoredDoc, limit: usize) {
    if heap.len() < limit {
        heap.push(Reverse(hit));
    } else if let Some(Reverse(min)) = heap.peek() {
        if hit.score > min.score || (hit.score == min.score && hit.doc < min.doc) {
            heap.pop();
            heap.push(Reverse(hit));
        }
    }
}

fn bm25(idf: f64, tf: u32, dl: u32, avgdl: f64) -> f64 {
    let tf = f64::from(tf);
    let dl = f64::from(dl);
    idf * tf * (BM25_K1 + 1.0) / (tf + BM25_K1 * (1.0 - BM25_B + BM25_B * dl / avgdl))
}

struct Cursor<'a> {
    reader: &'a SegmentReader,
    idf: f64,
    df: u32,
    term_max: f64,
    next_block: u64,
    remaining_blocks: u32,
    block: Vec<Posting>,
    pos: usize,
    block_max: f64,
    block_first: u32,
    block_last: u32,
}

impl<'a> Cursor<'a> {
    fn new(reader: &'a SegmentReader, entry: TermEntry, idf: f64) -> io::Result<Self> {
        let mut cursor = Self {
            reader,
            idf,
            df: entry.df,
            term_max: idf * entry.max_tf_norm,
            next_block: entry.postings_off,
            remaining_blocks: entry.block_count,
            block: Vec::new(),
            pos: 0,
            block_max: idf * entry.max_tf_norm,
            block_first: 0,
            block_last: 0,
        };
        cursor.load_next_block()?;
        Ok(cursor)
    }

    fn current(&self) -> Option<Posting> {
        self.block.get(self.pos).copied()
    }

    fn block_max(&self) -> f64 {
        self.block_max
    }

    fn skip_to(&mut self, target: u32) -> io::Result<bool> {
        loop {
            if self.block.is_empty() && !self.load_next_block()? {
                return Ok(false);
            }
            if let Some(posting) = self.current() {
                if posting.doc >= target {
                    return Ok(true);
                }
            }
            if self.block_last < target {
                if !self.skip_block_fast()? {
                    return Ok(false);
                }
                continue;
            }
            let slice = &self.block[self.pos..];
            let advance = slice.partition_point(|posting| posting.doc < target);
            self.pos += advance;
            if self.pos < self.block.len() {
                return Ok(true);
            }
            if !self.load_next_block()? {
                return Ok(false);
            }
        }
    }

    fn advance(&mut self) -> io::Result<()> {
        self.pos += 1;
        if self.pos >= self.block.len() {
            self.load_next_block()?;
        }
        Ok(())
    }

    fn skip_block(&mut self) -> io::Result<bool> {
        self.skip_block_fast()
    }

    /// Advance to the next block using only its header. This is what makes
    /// `skip_to` cheap for common terms: we do not decode postings that cannot
    /// contain the target document.
    fn skip_block_fast(&mut self) -> io::Result<bool> {
        if self.remaining_blocks == 0 {
            self.block.clear();
            self.pos = 0;
            self.block_max = 0.0;
            self.block_first = 0;
            self.block_last = 0;
            return Ok(false);
        }
        let mut header = [0u8; 20];
        read_at(&self.reader.file, self.next_block, &mut header)?;
        let data_len = u32::from_le_bytes(header[14..18].try_into().unwrap()) as u64;
        self.next_block += 20 + data_len;
        self.remaining_blocks -= 1;
        self.block.clear();
        self.pos = 0;
        self.block_max = 0.0;
        self.block_first = 0;
        self.block_last = 0;
        Ok(self.remaining_blocks > 0)
    }

    fn load_next_block(&mut self) -> io::Result<bool> {
        if self.remaining_blocks == 0 {
            self.block.clear();
            self.pos = 0;
            self.block_max = 0.0;
            self.block_first = 0;
            self.block_last = 0;
            return Ok(false);
        }
        let mut header = [0u8; 20];
        read_at(&self.reader.file, self.next_block, &mut header)?;
        let first_doc = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let last_doc = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let count = u16::from_le_bytes(header[8..10].try_into().unwrap()) as usize;
        let max_tf_norm = f32::from_le_bytes(header[10..14].try_into().unwrap()) as f64;
        let data_len = u32::from_le_bytes(header[14..18].try_into().unwrap()) as usize;
        let mut data = vec![0u8; data_len];
        read_at(&self.reader.file, self.next_block + 20, &mut data)?;
        let mut postings = Vec::with_capacity(count);
        let mut pos = 0usize;
        let mut doc = first_doc;
        for i in 0..count {
            let delta = read_varint(&data, &mut pos)? as u32;
            if i != 0 {
                doc = doc
                    .checked_add(delta)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "doc id overflow"))?;
            }
            let tf = read_varint(&data, &mut pos)? as u32;
            let dl = read_varint(&data, &mut pos)? as u32;
            postings.push(Posting { doc, tf, dl });
        }
        if pos != data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailing posting data",
            ));
        }
        self.next_block += 20 + data_len as u64;
        self.remaining_blocks -= 1;
        self.block = postings;
        self.pos = 0;
        self.block_max = self.idf * max_tf_norm;
        self.block_first = first_doc;
        self.block_last = last_doc;
        Ok(true)
    }
}

fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0usize;
        while done < buf.len() {
            let n = file.seek_read(&mut buf[done..], offset + done as u64)?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
            }
            done += n;
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut clone = file.try_clone()?;
        clone.seek(SeekFrom::Start(offset))?;
        clone.read_exact(buf)
    }
}

fn read_str(buf: &[u8], pos: &mut usize) -> io::Result<String> {
    if *pos + 2 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated term"));
    }
    let len = u16::from_le_bytes(buf[*pos..*pos + 2].try_into().unwrap()) as usize;
    *pos += 2;
    if *pos + len > buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated term bytes",
        ));
    }
    let out = String::from_utf8(buf[*pos..*pos + len].to_vec())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    *pos += len;
    Ok(out)
}

fn read_u32_buf(buf: &[u8], pos: &mut usize) -> io::Result<u32> {
    if *pos + 4 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated u32"));
    }
    let value = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(value)
}

fn read_u64_buf(buf: &[u8], pos: &mut usize) -> io::Result<u64> {
    if *pos + 8 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated u64"));
    }
    let value = u64::from_le_bytes(buf[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(value)
}

fn read_f32_buf(buf: &[u8], pos: &mut usize) -> io::Result<f32> {
    if *pos + 4 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated f32"));
    }
    let value = f32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(value)
}

fn read_varint(buf: &[u8], pos: &mut usize) -> io::Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        if *pos >= buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated varint",
            ));
        }
        let byte = buf[*pos];
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint overflow",
            ));
        }
    }
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn write_u16<W: Write>(w: &mut W, value: u16) -> io::Result<()> {
    w.write_all(&value.to_le_bytes())
}
fn write_u32<W: Write>(w: &mut W, value: u32) -> io::Result<()> {
    w.write_all(&value.to_le_bytes())
}
fn write_u64<W: Write>(w: &mut W, value: u64) -> io::Result<()> {
    w.write_all(&value.to_le_bytes())
}
fn write_f32<W: Write>(w: &mut W, value: f32) -> io::Result<()> {
    w.write_all(&value.to_le_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(docs: &[(&str, &str)]) -> (tempfile::TempDir, SegmentReader) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg.nos");
        let mut builder = SegmentBuilder::new(&path).unwrap();
        for (id, text) in docs {
            builder.add_document(id, text).unwrap();
        }
        let stats = builder.finish().unwrap();
        assert_eq!(stats.documents as usize, docs.len());
        let reader = SegmentReader::open(&path).unwrap();
        (dir, reader)
    }

    #[test]
    fn build_and_search_single_term() {
        let (_dir, reader) = build(&[
            ("a", "rust rust database"),
            ("b", "rust web"),
            ("c", "python database"),
        ]);
        let hits = reader.search("rust", 10).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, "a");
        assert_eq!(hits[1].id, "b");
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn multi_term_requires_all_terms() {
        let (_dir, reader) = build(&[
            ("a", "rust database engine"),
            ("b", "rust web server"),
            ("c", "python database"),
        ]);
        let hits = reader.search("rust database", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "a");
    }

    #[test]
    fn missing_term_returns_empty() {
        let (_dir, reader) = build(&[("a", "rust database")]);
        assert!(reader.search("nonexistent", 10).unwrap().is_empty());
        assert!(reader.search("rust nonexistent", 10).unwrap().is_empty());
    }

    #[test]
    fn blocks_and_doc_ids_roundtrip() {
        let docs: Vec<(String, String)> = (0..300)
            .map(|i| (format!("id-{i}"), format!("common token{i} rust")))
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg.nos");
        let mut builder = SegmentBuilder::new(&path).unwrap();
        for (id, text) in &docs {
            builder.add_document(id, text).unwrap();
        }
        builder.finish().unwrap();
        let reader = SegmentReader::open(&path).unwrap();
        let hits = reader.search("common", 10).unwrap();
        assert_eq!(hits.len(), 10);
        for hit in &hits {
            assert!(hit.id.starts_with("id-"));
        }
        assert_eq!(reader.doc_id(299).unwrap(), "id-299");
    }

    #[test]
    fn multi_term_block_skip_keeps_sparse_intersections() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg.nos");
        let mut builder = SegmentBuilder::new(&path).unwrap();
        for i in 0..600 {
            let word = if i % 2 == 0 { "even" } else { "odd" };
            builder
                .add_document(&format!("d{i}"), &format!("common {word}"))
                .unwrap();
        }
        builder.finish().unwrap();
        let reader = SegmentReader::open(&path).unwrap();
        let hits = reader.search("common even", 10).unwrap();
        assert_eq!(hits.len(), 10);
        for hit in &hits {
            let n: usize = hit.id[1..].parse().unwrap();
            assert_eq!(n % 2, 0);
        }
    }

    #[test]
    fn empty_segment_is_valid() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg.nos");
        let builder = SegmentBuilder::new(&path).unwrap();
        builder.finish().unwrap();
        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.documents(), 0);
        assert!(reader.search("anything", 10).unwrap().is_empty());
    }
}
