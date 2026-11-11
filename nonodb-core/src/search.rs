//! Full-text (BM25) and approximate search (trigrams + Levenshtein).
//!
//! Data layout (LMDB sub-databases):
//! - `ft:<coll>:<field>`  - postings: `term \0 _id` -> `tf` (u32 LE),
//! - `ftl:<coll>:<field>`  - document lengths: `_id` -> `dl` (u32 LE),
//! - `fz:<coll>:<field>`  - trigrams: `trigram \0 _id` -> empty.
//!
//! Document count `N` = `ftl.stat().entries`, total length = meta
//! `fttotal:<coll>:<field>` (u64 LE).

use std::collections::{HashMap, HashSet};

use serde_json::Value;
use unicode_normalization::{char::is_combining_mark, UnicodeNormalization};

/// BM25 parameters.
pub const BM25_K1: f64 = 1.2;
pub const BM25_B: f64 = 0.75;
/// Default maximum Levenshtein distance.
pub const DEFAULT_FUZZY_DISTANCE: usize = 2;

// ----------------------------- names ---------------------------------

pub fn text_db_name(coll: &str, field: &str) -> String {
    format!("ft:{coll}:{field}")
}
pub fn text_len_db_name(coll: &str, field: &str) -> String {
    format!("ftl:{coll}:{field}")
}
pub fn fuzzy_db_name(coll: &str, field: &str) -> String {
    format!("fz:{coll}:{field}")
}
pub fn text_def_key(coll: &str, field: &str) -> String {
    format!("text:{coll}:{field}")
}
pub fn fuzzy_def_key(coll: &str, field: &str) -> String {
    format!("fuzzy:{coll}:{field}")
}
pub fn text_total_key(coll: &str, field: &str) -> String {
    format!("fttotal:{coll}:{field}")
}

// --------------------------- text / tokens --------------------------

/// Field value as text (scalars only; arrays/objects are skipped).
pub fn field_text(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        _ => crate::numeric::text_value(value),
    }
}

/// Folds diacritics to ASCII (NFD + removal of combining marks).
/// Also maps letters that NFD does not decompose (e.g. `ł`, `ø`, `đ`).
pub fn fold_diacritics(s: &str) -> String {
    let lower = s.to_lowercase();
    let mut out = String::with_capacity(lower.len());
    for ch in lower.nfd().filter(|c| !is_combining_mark(*c)) {
        out.push(match ch {
            'ł' => 'l',
            'ø' => 'o',
            'đ' => 'd',
            'ð' => 'd',
            'þ' => 't',
            'ß' => 's',
            other => other,
        });
    }
    out
}

/// Normalization for matching: lowercase, diacritic folding, single spaces.
pub fn normalize(s: &str) -> String {
    fold_diacritics(s)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Tokenization: alphanumeric runs, lowercase + folding.
pub fn tokenize(s: &str) -> Vec<String> {
    let folded = fold_diacritics(s);
    let mut out = Vec::new();
    let mut cur = String::new();
    for ch in folded.chars() {
        if ch.is_alphanumeric() {
            cur.push(ch);
        } else if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Quoted phrases, e.g. `ala "ma kota"` -> `[["ma", "kota"]]`.
pub fn parse_phrases(query: &str) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    let mut rest = query;
    while let Some(start) = rest.find('"') {
        let after = &rest[start + 1..];
        let Some(end) = after.find('"') else {
            break;
        };
        let phrase = tokenize(&after[..end]);
        if !phrase.is_empty() {
            out.push(phrase);
        }
        rest = &after[end + 1..];
    }
    out
}

/// Does `needle` occur in `haystack` as a contiguous substring?
pub fn contains_phrase(haystack: &[String], needle: &[String]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Term frequencies in a document.
pub fn term_freqs(tokens: &[String]) -> HashMap<String, u32> {
    let mut map = HashMap::new();
    for t in tokens {
        *map.entry(t.clone()).or_insert(0) += 1;
    }
    map
}

// ----------------------------- trigrams ------------------------------

/// Unique normalized trigrams, with two leading spaces and one trailing space.
pub fn trigrams(s: &str) -> Vec<String> {
    let padded: Vec<char> = format!("  {} ", normalize(s)).chars().collect();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    if padded.len() < 3 {
        return out;
    }
    for w in padded.windows(3) {
        let g: String = w.iter().collect();
        if seen.insert(g.clone()) {
            out.push(g);
        }
    }
    out
}

/// Levenshtein distance with early cutoff (returns `None` when > `max`).
pub fn levenshtein(a: &str, b: &str, max: usize) -> Option<usize> {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.len().abs_diff(b.len()) > max {
        return None;
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0usize; b.len() + 1];
    for i in 1..=a.len() {
        cur[0] = i;
        let mut row_min = cur[0];
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            let v = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + cost);
            cur[j] = v;
            row_min = row_min.min(v);
        }
        if row_min > max {
            return None;
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    let d = prev[b.len()];
    (d <= max).then_some(d)
}

/// Best distance for a query: the whole text or a single token.
pub fn fuzzy_distance(value: &str, query: &str, max: usize) -> Option<usize> {
    let v = normalize(value);
    if let Some(d) = levenshtein(&v, query, max) {
        return Some(d);
    }
    let mut best: Option<usize> = None;
    for tok in tokenize(&v) {
        if let Some(d) = levenshtein(&tok, query, max) {
            best = Some(best.map_or(d, |b: usize| b.min(d)));
        }
    }
    best
}

// ----------------------------- keys ----------------------------------

/// Posting/trigram key: `term \0 id`.
pub fn postings_key(term: &str, id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(term.len() + 1 + id.len());
    k.extend_from_slice(term.as_bytes());
    k.push(0);
    k.extend_from_slice(id.as_bytes());
    k
}

/// Prefix for all postings of a term/trigram.
pub fn posting_prefix(term: &str) -> Vec<u8> {
    let mut k = term.as_bytes().to_vec();
    k.push(0);
    k
}

/// Extracts `_id` from a posting key.
pub fn parse_posting(key: &[u8]) -> Option<&str> {
    let pos = key.iter().position(|&b| b == 0)?;
    std::str::from_utf8(&key[pos + 1..]).ok()
}

pub fn read_u32(b: &[u8]) -> Option<u32> {
    b.try_into().ok().map(u32::from_le_bytes)
}

pub fn read_u64(b: &[u8]) -> Option<u64> {
    b.try_into().ok().map(u64::from_le_bytes)
}

// ----------------------------- BM25 ----------------------------------

pub fn idf(n_docs: f64, df: f64) -> f64 {
    ((n_docs - df + 0.5) / (df + 0.5) + 1.0).ln()
}

pub fn bm25_term(tf: f64, dl: f64, avgdl: f64, idf: f64) -> f64 {
    let avgdl = if avgdl > 0.0 { avgdl } else { 1.0 };
    idf * (tf * (BM25_K1 + 1.0)) / (tf + BM25_K1 * (1.0 - BM25_B + BM25_B * dl / avgdl))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tokenize_lowercases_and_splits() {
        assert_eq!(
            tokenize("Ala ma kota, a Kot ma ALĘ!"),
            vec!["ala", "ma", "kota", "a", "kot", "ma", "ale"]
        );
        assert!(tokenize("...").is_empty());
    }

    #[test]
    fn folds_diacritics_and_phrases() {
        assert_eq!(
            tokenize("Zażółć gęślą jaźń"),
            vec!["zazolc", "gesla", "jazn"]
        );
        assert_eq!(normalize("ŁÓDŹ"), "lodz");
        let phrases = parse_phrases("ala \"ma kota\" i psa");
        assert_eq!(phrases, vec![vec!["ma".to_string(), "kota".to_string()]]);
        let doc = tokenize("Ala ma kota");
        assert!(contains_phrase(&doc, &phrases[0]));
        assert!(!contains_phrase(&tokenize("Ala kota ma"), &phrases[0]));
    }

    #[test]
    fn term_freqs_counts() {
        let tf = term_freqs(&tokenize("a b a a"));
        assert_eq!(tf["a"], 3);
        assert_eq!(tf["b"], 1);
    }

    #[test]
    fn trigrams_are_padded_and_unique() {
        let g = trigrams("ala");
        assert!(g.contains(&"  a".to_string()));
        assert!(g.contains(&"ala".to_string()));
        let set: HashSet<_> = g.iter().collect();
        assert_eq!(set.len(), g.len());
    }

    #[test]
    fn levenshtein_basic() {
        assert_eq!(levenshtein("ala", "ala", 0), Some(0));
        assert_eq!(levenshtein("ala", "ola", 1), Some(1));
        assert_eq!(levenshtein("ala", "kota", 2), None);
        assert_eq!(levenshtein("", "abc", 3), Some(3));
    }

    #[test]
    fn fuzzy_matches_token_and_whole_string() {
        assert_eq!(fuzzy_distance("quick brown fox", "quik", 2), Some(1));
        assert_eq!(fuzzy_distance("Ala", "ala", 1), Some(0));
        assert_eq!(fuzzy_distance("Ala", "kota", 1), None);
    }

    #[test]
    fn field_text_covers_extended_scalars() {
        assert_eq!(field_text(&json!("Ala")), Some("Ala".into()));
        assert_eq!(field_text(&json!(true)), Some("true".into()));
        assert_eq!(field_text(&json!(1.25)), Some("1.25".into()));
        assert_eq!(
            field_text(&json!({"$numberLong": "9007199254740993"})),
            Some("9007199254740993".into())
        );
        assert_eq!(
            field_text(&json!({"$numberDecimal": "199.99"})),
            Some("199.99".into())
        );
        assert_eq!(
            field_text(&json!({"$numberDouble": "NaN"})),
            Some("NaN".into())
        );
        assert_eq!(field_text(&json!({"$date": {"$numberLong": "0"}})), None);
        assert_eq!(
            field_text(&json!({"$oid": "000000000000000000000001"})),
            None
        );
        assert_eq!(
            field_text(&json!({"$binary": {"base64": "YQ==", "subType": "00"}})),
            None
        );
    }

    #[test]
    fn bm25_grows_with_tf_and_idf() {
        let a = bm25_term(1.0, 10.0, 10.0, 1.0);
        let b = bm25_term(3.0, 10.0, 10.0, 1.0);
        assert!(b > a);
        assert!(idf(100.0, 1.0) > idf(100.0, 50.0));
    }
}
