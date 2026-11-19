//! Full-text (BM25) and approximate search (trigrams + Levenshtein).
//!
//! Search indexes are collection-level definitions with one or more fields.
//! Data layout (LMDB sub-databases):
//! - `ft:<coll>:<index>:<field>`  - postings: `term \0 _id` -> `tf` (u32 LE) + optional `dl` (u32 LE),
//! - `ftl:<coll>:<index>:<field>` - document lengths: `_id` -> `dl` (u32 LE),
//! - `fz:<coll>:<index>:<field>`  - trigrams: `trigram \0 _id` -> empty.
//!
//! Document count `N` = `ftl.stat().entries`, total length = meta
//! `fttotal:<coll>:<index>:<field>` (u64 LE).

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use unicode_normalization::{char::is_combining_mark, UnicodeNormalization};

/// BM25 parameters.
pub const BM25_K1: f64 = 1.2;
pub const BM25_B: f64 = 0.75;
/// Default maximum Levenshtein distance.
pub const DEFAULT_FUZZY_DISTANCE: usize = 2;

// ----------------------------- definitions ---------------------------

/// Search index kind. Text uses term postings and BM25; fuzzy uses trigrams.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchKind {
    Text,
    Fuzzy,
}

impl SearchKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Fuzzy => "fuzzy",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "text" => Some(Self::Text),
            "fuzzy" => Some(Self::Fuzzy),
            _ => None,
        }
    }
}

fn default_weight() -> f64 {
    1.0
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchField {
    pub path: String,
    #[serde(default = "default_weight")]
    pub weight: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchIndexDef {
    pub name: String,
    pub kind: SearchKind,
    pub fields: Vec<SearchField>,
}

impl SearchIndexDef {
    pub fn single(kind: SearchKind, field: &str) -> Self {
        Self {
            name: field.to_string(),
            kind,
            fields: vec![SearchField {
                path: field.to_string(),
                weight: 1.0,
            }],
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("search index name cannot be empty".into());
        }
        if self.name.len() > 127 || self.name.contains('\0') {
            return Err(
                "search index name must be at most 127 bytes and must not contain NUL".into(),
            );
        }
        if self.fields.is_empty() {
            return Err("search index must contain at least one field".into());
        }
        let mut seen = HashSet::new();
        for field in &self.fields {
            if field.path.is_empty() {
                return Err("search index field cannot be empty".into());
            }
            if field.path.contains('\0') {
                return Err("search index field must not contain NUL".into());
            }
            if !seen.insert(field.path.as_str()) {
                return Err(format!("duplicate search index field `{}`", field.path));
            }
            if !field.weight.is_finite() || field.weight <= 0.0 {
                return Err(format!(
                    "search index weight for `{}` must be positive and finite",
                    field.path
                ));
            }
        }
        Ok(())
    }

    pub fn field_paths(&self) -> Vec<String> {
        self.fields.iter().map(|field| field.path.clone()).collect()
    }
}

/// Query parsing mode. Native search requires all quoted phrases and at least
/// one unquoted term. MongoDB text search treats terms and phrases as ORed
/// positive clauses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextQueryMode {
    Native,
    Mongo,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TextQuery {
    pub positive_terms: Vec<String>,
    pub phrases: Vec<Vec<String>>,
    pub negative_terms: Vec<String>,
    pub negative_phrases: Vec<Vec<String>>,
}

// ----------------------------- names ---------------------------------

pub fn text_db_name(coll: &str, index: &str, field: &str) -> String {
    format!("ft:{coll}:{index}:{field}")
}
pub fn text_len_db_name(coll: &str, index: &str, field: &str) -> String {
    format!("ftl:{coll}:{index}:{field}")
}
pub fn fuzzy_db_name(coll: &str, index: &str, field: &str) -> String {
    format!("fz:{coll}:{index}:{field}")
}
pub fn def_key(coll: &str, kind: SearchKind, name: &str) -> String {
    format!("search:{coll}:{}:{name}", kind.as_str())
}
pub fn def_prefix(coll: &str) -> String {
    format!("search:{coll}:")
}
pub fn text_total_key(coll: &str, index: &str, field: &str) -> String {
    format!("fttotal:{coll}:{index}:{field}")
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
    parse_text_query(query).phrases
}

/// Parses text search syntax with quoted phrases and `-` negation.
pub fn parse_text_query(query: &str) -> TextQuery {
    let mut out = TextQuery::default();
    let mut word = String::new();
    let mut phrase = String::new();
    let mut in_quote = false;
    let mut negative = false;
    let mut at_start = true;

    let flush_word = |word: &mut String, negative: bool, out: &mut TextQuery| {
        for term in tokenize(word) {
            if negative {
                out.negative_terms.push(term);
            } else {
                out.positive_terms.push(term);
            }
        }
        word.clear();
    };

    for ch in query.chars() {
        if in_quote {
            if ch == '"' {
                let tokens = tokenize(&phrase);
                if !tokens.is_empty() {
                    if negative {
                        out.negative_phrases.push(tokens);
                    } else {
                        out.phrases.push(tokens);
                    }
                }
                phrase.clear();
                in_quote = false;
                negative = false;
                at_start = true;
            } else {
                phrase.push(ch);
            }
            continue;
        }
        match ch {
            '"' => {
                flush_word(&mut word, negative, &mut out);
                negative = false;
                in_quote = true;
            }
            '-' if at_start && word.is_empty() => {
                negative = true;
                at_start = false;
            }
            ch if ch.is_alphanumeric() => {
                word.push(ch);
                at_start = false;
            }
            _ => {
                flush_word(&mut word, negative, &mut out);
                negative = false;
                at_start = true;
            }
        }
    }
    if in_quote {
        let tokens = tokenize(&phrase);
        if !tokens.is_empty() {
            if negative {
                out.negative_phrases.push(tokens);
            } else {
                out.phrases.push(tokens);
            }
        }
    }
    flush_word(&mut word, negative, &mut out);
    out.positive_terms.sort();
    out.positive_terms.dedup();
    out.negative_terms.sort();
    out.negative_terms.dedup();
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

/// Text posting value. New indexes store term frequency and document length
/// together so scoring does not need a second random lookup for every hit.
pub fn text_posting_value(tf: u32, dl: u32) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[..4].copy_from_slice(&tf.to_le_bytes());
    out[4..].copy_from_slice(&dl.to_le_bytes());
    out
}

/// Term frequency stored in a text posting, old or new format.
pub fn text_posting_tf(value: &[u8]) -> Option<u32> {
    value.get(..4).and_then(read_u32)
}

/// Document length stored in a text posting, if this is a new-format value.
pub fn text_posting_dl(value: &[u8]) -> Option<u32> {
    value.get(4..8).and_then(read_u32)
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
