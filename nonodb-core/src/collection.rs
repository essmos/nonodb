//! Operacje na kolekcji dokumentów.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use heed::types::Bytes;
use heed::{Database, Env, RoTxn, RwTxn};
use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::index::{self, IndexDef};
use crate::query::{apply_update, apply_upsert, matches, path, seed_from_filter};
use crate::search;
use crate::store::MAX_DOCUMENT_BYTES;

/// Od ilu dokumentów pełny skan z sortowaniem idzie na wiele wątków.
#[cfg(not(test))]
const PARALLEL_SCAN_MIN_ENTRIES: usize = 50_000;
#[cfg(test)]
const PARALLEL_SCAN_MIN_ENTRIES: usize = 200;

/// Maksymalne `skip + limit` dla ścieżki równoległej. Przy głębokim `skip`
/// każdy wątek musiałby trzymać własny duży TopK — wtedy lepiej zostać przy
/// jednym wątku i ograniczonym zużyciu pamięci.
const PARALLEL_SCAN_MAX_WANT: usize = 50_000;

/// Od jakiego `skip + limit` opłaca się równoległy skan bez sortowania.
/// Dla `find_one` i bardzo małych limitów wczesne wyjście jest zwykle szybsze.
const PARALLEL_UNSORTED_MIN_WANT: usize = 16;

/// Rozmiar porcji surowych bajtów przekazywanej wątkom roboczym.
const PARALLEL_SCAN_CHUNK: usize = 8192;

/// Porcja skanu: jeden bufor bajtów + offsety dokumentów (mniej alokacji).
struct ScanChunk {
    idx: usize,
    data: Vec<u8>,
    docs: Vec<(usize, u32, u32)>,
}

/// Tani prefiltr pełnego skanu: dla prostych równości string/bool sprawdza,
/// czy w surowym JSON w ogóle występuje klucz i wartość. Fałszywe trafienia są
/// akceptowane (potem i tak działa matcher), ale nie może być fałszywych
/// pominięć. Liczby i `null` są pomijane, bo ich semantyka/różnice formatowania
/// wymagają pełnego parsera.
#[derive(Debug, Clone, Default)]
struct RawPrefilter {
    needles: Vec<Vec<u8>>,
}

impl RawPrefilter {
    fn matches(&self, bytes: &[u8]) -> bool {
        self.needles
            .iter()
            .all(|needle| memchr::memmem::find(bytes, needle).is_some())
    }
}

/// Buduje prefiltr dla bezpiecznych warunków równości. Warunki, których nie
/// umiemy sprawdzić po bajtach, są ignorowane (filtr to AND, więc to bezpieczne).
fn raw_prefilter(filter: &Value) -> Option<RawPrefilter> {
    let obj = filter.as_object()?;
    let mut needles = Vec::new();
    for (key, cond) in obj {
        if key.starts_with('$') {
            // `$or` / `$not` / `$nor` nie da się bezpiecznie zignorować.
            return None;
        }
        if key.contains('.') || key == "_id" {
            continue;
        }
        let Some(value) = raw_equality_value(cond) else {
            continue;
        };
        let key_bytes = serde_json::to_vec(&Value::String(key.clone())).ok()?;
        let value_bytes = serde_json::to_vec(value).ok()?;
        if !needles.contains(&key_bytes) {
            needles.push(key_bytes);
        }
        if !needles.contains(&value_bytes) {
            needles.push(value_bytes);
        }
    }
    if needles.is_empty() {
        None
    } else {
        Some(RawPrefilter { needles })
    }
}

/// Zwraca wartość równości, jeśli da się ją bezpiecznie znaleźć w surowym
/// JSON. Obsługujemy tylko stringi i boole; resztę zostawiamy parserowi.
fn raw_equality_value(cond: &Value) -> Option<&Value> {
    match cond {
        Value::String(_) | Value::Bool(_) => Some(cond),
        Value::Object(ops) if ops.len() == 1 => match ops.get("$eq") {
            Some(Value::String(_)) | Some(Value::Bool(_)) => ops.get("$eq"),
            _ => None,
        },
        _ => None,
    }
}

/// Parsuje dokument tylko wtedy, gdy prefiltr go nie odrzucił.
fn parse_prefiltered(bytes: &[u8], raw: Option<&RawPrefilter>) -> Result<Option<Value>> {
    if raw.is_some_and(|r| !r.matches(bytes)) {
        return Ok(None);
    }
    parse_json(bytes).map(Some)
}

/// Szybkie parsowanie JSON (SIMD) do `serde_json::Value`.
fn parse_json(bytes: &[u8]) -> Result<Value> {
    sonic_rs::from_slice::<Value>(bytes).map_err(|e| Error::JsonFast(e.to_string()))
}

type TextBackfillEntry = (String, Vec<(String, u32)>, u32);

/// Wynik operacji `update` (z opcjonalnym upsertem).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateOutcome {
    pub matched: usize,
    pub modified: usize,
    pub upserted_id: Option<String>,
}
type IndexIter<'a> = Box<dyn Iterator<Item = heed::Result<(&'a [u8], &'a [u8])>> + 'a>;
type IdBounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// Porcja backfillu: bufor z `_id` + dokumentami.
struct BackfillChunk {
    data: Vec<u8>,
    docs: Vec<(u32, u32, u32, u32)>, // id_start, id_len, doc_start, doc_len
}

/// Wybrany indeks i zakres kluczy dla filtra.
struct IndexPlan {
    db: Database<Bytes, Bytes>,
    bounds: index::RangeBytes,
    def: IndexDef,
}

/// Parametry zapytania `find`.
#[derive(Debug, Clone, Default)]
pub struct FindQuery {
    /// Filtr w stylu MongoDB; `Null`/`{}` = wszystko.
    pub filter: Value,
    /// Projekcja pól (na razie płaska, top-level).
    pub projection: Option<Value>,
    /// Klucze sortowania: `(ścieżka, 1 | -1)`.
    pub sort: Vec<(String, i32)>,
    pub skip: usize,
    pub limit: Option<usize>,
}

pub struct Collection<'e> {
    db: Database<Bytes, Bytes>,
    meta: Database<Bytes, Bytes>,
    name: String,
    env: &'e Env,
}

impl<'e> Collection<'e> {
    pub(crate) fn new(
        env: &'e Env,
        db: Database<Bytes, Bytes>,
        meta: Database<Bytes, Bytes>,
        name: String,
    ) -> Self {
        Self {
            db,
            meta,
            name,
            env,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    // ----------------------------- indeksy -----------------------------

    /// Definicje indeksów tej kolekcji (odczyt w podanej transakcji).
    fn index_defs(&self, txn: &RoTxn<'_>) -> Result<Vec<IndexDef>> {
        let prefix = format!("index:{}:", self.name);
        let mut defs = Vec::new();
        for item in self.meta.prefix_iter(txn, prefix.as_bytes())? {
            let (_key, bytes) = item?;
            defs.push(serde_json::from_slice(bytes)?);
        }
        Ok(defs)
    }

    fn index_db(&self, txn: &RoTxn<'_>, def: &IndexDef) -> Result<Option<Database<Bytes, Bytes>>> {
        Ok(self
            .env
            .open_database(txn, Some(&index::db_name(&self.name, &def.name())))?)
    }

    fn index_db_in(&self, wtxn: &mut RwTxn<'_>, def: &IndexDef) -> Result<Database<Bytes, Bytes>> {
        Ok(self
            .env
            .create_database(&mut *wtxn, Some(&index::db_name(&self.name, &def.name())))?)
    }

    /// Tworzy indeks wtórny (pojedyncze pole) i uzupełnia go istniejącymi
    /// dokumentami.
    pub fn create_index(&self, field: &str, unique: bool) -> Result<()> {
        self.create_index_fields(&[field.to_string()], unique, &[])
    }

    pub fn create_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str, unique: bool) -> Result<()> {
        self.create_index_fields_txn(wtxn, &[field.to_string()], unique, &[])
    }

    /// Tworzy indeks złożony. Kolejność pól ma znaczenie: prefiks pól może
    /// obsłużyć filtr, a pełny indeks również sortowanie.
    pub fn create_index_fields(
        &self,
        fields: &[String],
        unique: bool,
        include: &[String],
    ) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.create_index_fields_txn(&mut wtxn, fields, unique, include)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn create_index_fields_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        fields: &[String],
        unique: bool,
        include: &[String],
    ) -> Result<()> {
        if fields.is_empty() {
            return Err(Error::Query("index requires at least one field".into()));
        }
        if fields.iter().any(|f| f.is_empty() || f.contains('+')) {
            return Err(Error::Query(
                "index fields must be non-empty and must not contain `+`".into(),
            ));
        }
        let def = IndexDef::compound(fields, unique, include);
        let key = index::def_key(&self.name, &def.name());
        self.meta.put(
            &mut *wtxn,
            key.as_bytes(),
            serde_json::to_vec(&def)?.as_slice(),
        )?;
        let idx_db = self.index_db_in(wtxn, &def)?;

        // Backfill: równoległe parsowanie/kodowanie, zapis w jednej transakcji.
        let fields = def.effective_fields();
        let def_ref = &def;
        let entries: Vec<(Vec<u8>, Vec<u8>, String)> =
            self.parallel_backfill(wtxn, |id, doc| {
                Ok(index::encode_fields(&doc, &fields)
                    .map(|vb| {
                        let value = index::encode_index_value(def_ref, &doc, &id);
                        vec![(vb, value, id)]
                    })
                    .unwrap_or_default())
            })?;

        let mut seen: HashSet<Vec<u8>> = HashSet::new();

        for (vb, value, id) in entries {
            if unique && !seen.insert(vb.clone()) {
                return Err(Error::DuplicateKey(id));
            }
            let mut key = vb;
            key.extend_from_slice(id.as_bytes());
            idx_db.put(&mut *wtxn, key.as_slice(), value.as_slice())?;
        }
        Ok(())
    }

    /// Usuwa indeks (czyści dane i definicję).
    pub fn drop_index(&self, field: &str) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let existed = self.drop_index_txn(&mut wtxn, field)?;
        wtxn.commit()?;
        Ok(existed)
    }

    pub fn drop_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<bool> {
        let key = index::def_key(&self.name, field);
        let existed = self.meta.delete(&mut *wtxn, key.as_bytes())?;
        if existed {
            if let Some(db) = self
                .env
                .open_database::<Bytes, Bytes>(&*wtxn, Some(&index::db_name(&self.name, field)))?
            {
                // Usuwamy całą nazwaną sub-bazę, żeby nie zajmowała slotu
                // `MAX_DBS` po kolejnych create/drop.
                db.clear(&mut *wtxn)?;
            }
        }
        Ok(existed)
    }

    /// Lista indeksów `(nazwa, unikalny)`; nazwa złożonego to `a+b`.
    pub fn list_indexes(&self) -> Result<Vec<(String, bool)>> {
        let rtxn = self.read_txn()?;
        let out = self.list_indexes_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn list_indexes_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<(String, bool)>> {
        Ok(self
            .index_defs(rtxn)?
            .into_iter()
            .map(|d| (d.name(), d.unique))
            .collect())
    }

    /// Pełne definicje indeksów w podanej transakcji.
    pub fn index_defs_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<IndexDef>> {
        self.index_defs(rtxn)
    }

    /// Pełne definicje indeksów (dla API/CLI).
    pub fn list_index_defs(&self) -> Result<Vec<IndexDef>> {
        let rtxn = self.read_txn()?;
        let out = self.index_defs(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Sprawdza unikalność nowej wartości we wszystkich indeksach unikalnych.
    fn validate_unique(&self, txn: &RoTxn<'_>, doc: &Value, id: &str) -> Result<()> {
        for def in self.index_defs(txn)? {
            if !def.unique {
                continue;
            }
            let fields = def.effective_fields();
            let Some(vb) = index::encode_fields(doc, &fields) else {
                continue;
            };
            let Some(db) = self.index_db(txn, &def)? else {
                continue;
            };
            for item in db.prefix_iter(txn, vb.as_slice())? {
                let (_key, existing) = item?;
                if existing != id.as_bytes() {
                    return Err(Error::DuplicateKey(id.to_string()));
                }
            }
        }
        Ok(())
    }

    /// Aktualizuje wpisy indeksów dla zmiany `old -> new` (insert: `old=None`,
    /// delete: `new=None`).
    fn apply_index_entries(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Option<&Value>,
        new: Option<&Value>,
        id: &str,
    ) -> Result<()> {
        let defs = self.index_defs(&*wtxn)?;
        for def in defs {
            let fields = def.effective_fields();
            let old_vb = old.and_then(|d| index::encode_fields(d, &fields));
            let new_vb = new.and_then(|d| index::encode_fields(d, &fields));
            // Przy `include` trzeba też porównać kopiowane pola, bo mogą się
            // zmienić bez zmiany klucza indeksu.
            let old_value = old.map(|d| index::encode_index_value(&def, d, id));
            let new_value = new.map(|d| index::encode_index_value(&def, d, id));
            if old_vb == new_vb && old_value == new_value {
                continue;
            }
            let idx_db = self.index_db_in(wtxn, &def)?;
            if let Some(vb) = old_vb {
                let mut key = vb;
                key.extend_from_slice(id.as_bytes());
                idx_db.delete(&mut *wtxn, key.as_slice())?;
            }
            if let Some(vb) = new_vb {
                let mut key = vb;
                key.extend_from_slice(id.as_bytes());
                let value = new_value.expect("nowy dokument");
                idx_db.put(&mut *wtxn, key.as_slice(), value.as_slice())?;
            }
        }
        Ok(())
    }

    /// Ustala sub-bazę indeksu i zakres kluczy dla filtra.
    ///
    /// Indeks złożony `[a, b, c]` jest używany, gdy filtr zawiera warunki dla
    /// wszystkich pól, przy czym wszystkie oprócz ostatniego muszą być
    /// równościowe (`a: 1` lub `a: {$eq: 1}`). Ostatnie pole może mieć zakres.
    fn index_plan(&self, rtxn: &RoTxn<'_>, filter: &Value) -> Result<Option<IndexPlan>> {
        let Some(obj) = filter.as_object() else {
            return Ok(None);
        };
        if obj.is_empty() || obj.keys().any(|k| k.starts_with('$')) {
            return Ok(None);
        }

        // Najpierw indeksy z najdłuższym prefiksem pól (najbardziej selektywne).
        let mut defs = self.index_defs(rtxn)?;
        defs.sort_by_key(|d| std::cmp::Reverse(d.effective_fields().len()));

        for def in defs {
            let Some(bounds) = bounds_for_index(&def, obj) else {
                continue;
            };
            let Some(db) = self.index_db(rtxn, &def)? else {
                continue;
            };
            return Ok(Some(IndexPlan { db, bounds, def }));
        }
        Ok(None)
    }

    fn index_candidates(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
        max: Option<usize>,
    ) -> Result<Option<Vec<String>>> {
        let Some(plan) = self.index_plan(rtxn, filter)? else {
            return Ok(None);
        };
        let (lower, upper) = &plan.bounds;
        let range = (as_bound(lower), as_bound(upper));

        let mut ids = Vec::new();
        for item in plan.db.range(rtxn, &range)? {
            let (_key, value) = item?;
            if let Some((id, _)) = index::decode_index_value(value) {
                ids.push(id);
            }
            if max.is_some_and(|m| ids.len() >= m) {
                break;
            }
        }
        Ok(Some(ids))
    }

    /// Liczy wpisy indeksu w zakresie bez alokowania listy `_id`.
    fn index_count_plan(&self, rtxn: &RoTxn<'_>, plan: &IndexPlan) -> Result<usize> {
        let (lower, upper) = &plan.bounds;
        let range = (as_bound(lower), as_bound(upper));
        let mut n = 0usize;
        for item in plan.db.range(rtxn, &range)? {
            item?;
            n += 1;
        }
        Ok(n)
    }

    // --------------------------- full-text ----------------------------

    fn text_defs(&self, txn: &RoTxn<'_>) -> Result<Vec<String>> {
        self.field_defs(txn, "text")
    }

    fn fuzzy_defs(&self, txn: &RoTxn<'_>) -> Result<Vec<String>> {
        self.field_defs(txn, "fuzzy")
    }

    fn field_defs(&self, txn: &RoTxn<'_>, kind: &str) -> Result<Vec<String>> {
        let prefix = format!("{kind}:{}:", self.name);
        let mut out = Vec::new();
        for item in self.meta.prefix_iter(txn, prefix.as_bytes())? {
            let (key, _) = item?;
            let s = std::str::from_utf8(key).map_err(|e| Error::Query(e.to_string()))?;
            if let Some(field) = s.strip_prefix(&prefix) {
                out.push(field.to_string());
            }
        }
        Ok(out)
    }

    fn text_postings(
        &self,
        txn: &RoTxn<'_>,
        field: &str,
    ) -> Result<Option<Database<Bytes, Bytes>>> {
        Ok(self
            .env
            .open_database(txn, Some(&search::text_db_name(&self.name, field)))?)
    }

    fn text_postings_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        field: &str,
    ) -> Result<Database<Bytes, Bytes>> {
        Ok(self
            .env
            .create_database(&mut *wtxn, Some(&search::text_db_name(&self.name, field)))?)
    }

    fn text_len_db(&self, txn: &RoTxn<'_>, field: &str) -> Result<Option<Database<Bytes, Bytes>>> {
        Ok(self
            .env
            .open_database(txn, Some(&search::text_len_db_name(&self.name, field)))?)
    }

    fn text_len_db_in(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<Database<Bytes, Bytes>> {
        Ok(self.env.create_database(
            &mut *wtxn,
            Some(&search::text_len_db_name(&self.name, field)),
        )?)
    }

    fn fuzzy_db(&self, txn: &RoTxn<'_>, field: &str) -> Result<Option<Database<Bytes, Bytes>>> {
        Ok(self
            .env
            .open_database(txn, Some(&search::fuzzy_db_name(&self.name, field)))?)
    }

    fn fuzzy_db_in(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<Database<Bytes, Bytes>> {
        Ok(self
            .env
            .create_database(&mut *wtxn, Some(&search::fuzzy_db_name(&self.name, field)))?)
    }

    /// Tworzy indeks full-text (BM25) i uzupełnia go istniejącymi dokumentami.
    pub fn create_text_index(&self, field: &str) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.create_text_index_txn(&mut wtxn, field)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn create_text_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<()> {
        let def_key = search::text_def_key(&self.name, field);
        if self.meta.get(&*wtxn, def_key.as_bytes())?.is_some() {
            return Ok(());
        }

        // Backfill: równoległa tokenizacja, zapis w jednej transakcji.
        let entries: Vec<TextBackfillEntry> = self.parallel_backfill(wtxn, |id, doc| {
            let Some(text) = index::field_value(&doc, field).and_then(search::field_text) else {
                return Ok(Vec::new());
            };
            let tokens = search::tokenize(&text);
            let dl = tokens.len() as u32;
            let tf = search::term_freqs(&tokens).into_iter().collect();
            Ok(vec![(id, tf, dl)])
        })?;

        let postings = self.text_postings_in(wtxn, field)?;
        let lens = self.text_len_db_in(wtxn, field)?;
        postings.clear(&mut *wtxn)?;
        lens.clear(&mut *wtxn)?;

        let mut total = 0u64;
        for (id, tf, dl) in entries {
            lens.put(&mut *wtxn, id.as_bytes(), &dl.to_le_bytes())?;
            total += u64::from(dl);
            for (term, tf) in tf {
                postings.put(
                    &mut *wtxn,
                    search::postings_key(&term, &id).as_slice(),
                    &tf.to_le_bytes(),
                )?;
            }
        }

        self.meta.put(&mut *wtxn, def_key.as_bytes(), b"1")?;
        self.meta.put(
            &mut *wtxn,
            search::text_total_key(&self.name, field).as_bytes(),
            &total.to_le_bytes(),
        )?;
        Ok(())
    }

    /// Usuwa indeks full-text.
    pub fn drop_text_index(&self, field: &str) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let existed = self.drop_text_index_txn(&mut wtxn, field)?;
        wtxn.commit()?;
        Ok(existed)
    }

    pub fn drop_text_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<bool> {
        let def_key = search::text_def_key(&self.name, field);
        let existed = self.meta.delete(&mut *wtxn, def_key.as_bytes())?;
        if existed {
            if let Some(db) = self.text_postings(&*wtxn, field)? {
                db.clear(&mut *wtxn)?;
            }
            if let Some(db) = self.text_len_db(&*wtxn, field)? {
                db.clear(&mut *wtxn)?;
            }
            self.meta.delete(
                &mut *wtxn,
                search::text_total_key(&self.name, field).as_bytes(),
            )?;
        }
        Ok(existed)
    }

    pub fn list_text_indexes(&self) -> Result<Vec<String>> {
        let rtxn = self.read_txn()?;
        let out = self.list_text_indexes_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn list_text_indexes_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<String>> {
        self.text_defs(rtxn)
    }

    /// Tworzy indeks trigramowy (fuzzy) i uzupełnia go istniejącymi dokumentami.
    pub fn create_fuzzy_index(&self, field: &str) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.create_fuzzy_index_txn(&mut wtxn, field)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn create_fuzzy_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<()> {
        let def_key = search::fuzzy_def_key(&self.name, field);
        if self.meta.get(&*wtxn, def_key.as_bytes())?.is_some() {
            return Ok(());
        }

        let entries: Vec<(String, Vec<String>)> = self.parallel_backfill(wtxn, |id, doc| {
            let Some(text) = index::field_value(&doc, field).and_then(search::field_text) else {
                return Ok(Vec::new());
            };
            Ok(vec![(id, search::trigrams(&text))])
        })?;

        let db = self.fuzzy_db_in(wtxn, field)?;
        db.clear(&mut *wtxn)?;
        for (id, grams) in entries {
            for gram in grams {
                db.put(&mut *wtxn, search::postings_key(&gram, &id).as_slice(), &[])?;
            }
        }

        self.meta.put(&mut *wtxn, def_key.as_bytes(), b"1")?;
        Ok(())
    }

    pub fn drop_fuzzy_index(&self, field: &str) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let existed = self.drop_fuzzy_index_txn(&mut wtxn, field)?;
        wtxn.commit()?;
        Ok(existed)
    }

    pub fn drop_fuzzy_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<bool> {
        let def_key = search::fuzzy_def_key(&self.name, field);
        let existed = self.meta.delete(&mut *wtxn, def_key.as_bytes())?;
        if existed {
            if let Some(db) = self.fuzzy_db(&*wtxn, field)? {
                db.clear(&mut *wtxn)?;
            }
        }
        Ok(existed)
    }

    pub fn list_fuzzy_indexes(&self) -> Result<Vec<String>> {
        let rtxn = self.read_txn()?;
        let out = self.list_fuzzy_indexes_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn list_fuzzy_indexes_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<String>> {
        self.fuzzy_defs(rtxn)
    }

    /// Utrzymuje indeksy full-text przy zmianie `old -> new`.
    fn apply_text_entries(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Option<&Value>,
        new: Option<&Value>,
        id: &str,
    ) -> Result<()> {
        for field in self.text_defs(&*wtxn)? {
            let old_text = old
                .and_then(|d| index::field_value(d, &field))
                .and_then(search::field_text);
            let new_text = new
                .and_then(|d| index::field_value(d, &field))
                .and_then(search::field_text);
            if old_text == new_text {
                continue;
            }
            let postings = self.text_postings_in(wtxn, &field)?;
            let lens = self.text_len_db_in(wtxn, &field)?;
            let total_key = search::text_total_key(&self.name, &field);
            let mut total = self
                .meta
                .get(&*wtxn, total_key.as_bytes())?
                .and_then(search::read_u64)
                .unwrap_or(0);

            let old_tokens = old_text.as_ref().map(|t| search::tokenize(t));
            let new_tokens = new_text.as_ref().map(|t| search::tokenize(t));
            let old_len = old_tokens.as_ref().map(|t| t.len() as u64).unwrap_or(0);
            let new_len = new_tokens.as_ref().map(|t| t.len() as u64).unwrap_or(0);

            if let Some(tokens) = &old_tokens {
                for term in search::term_freqs(tokens).into_keys() {
                    postings.delete(&mut *wtxn, search::postings_key(&term, id).as_slice())?;
                }
            }
            if let Some(tokens) = &new_tokens {
                for (term, tf) in search::term_freqs(tokens) {
                    postings.put(
                        &mut *wtxn,
                        search::postings_key(&term, id).as_slice(),
                        &tf.to_le_bytes(),
                    )?;
                }
                lens.put(&mut *wtxn, id.as_bytes(), &(new_len as u32).to_le_bytes())?;
            } else {
                lens.delete(&mut *wtxn, id.as_bytes())?;
            }

            total = total.saturating_sub(old_len) + new_len;
            self.meta
                .put(&mut *wtxn, total_key.as_bytes(), &total.to_le_bytes())?;
        }
        Ok(())
    }

    /// Utrzymuje indeksy trigramowe przy zmianie `old -> new`.
    fn apply_fuzzy_entries(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Option<&Value>,
        new: Option<&Value>,
        id: &str,
    ) -> Result<()> {
        for field in self.fuzzy_defs(&*wtxn)? {
            let old_text = old
                .and_then(|d| index::field_value(d, &field))
                .and_then(search::field_text)
                .map(|t| search::normalize(&t));
            let new_text = new
                .and_then(|d| index::field_value(d, &field))
                .and_then(search::field_text)
                .map(|t| search::normalize(&t));
            if old_text == new_text {
                continue;
            }
            let db = self.fuzzy_db_in(wtxn, &field)?;
            if let Some(text) = &old_text {
                for gram in search::trigrams(text) {
                    db.delete(&mut *wtxn, search::postings_key(&gram, id).as_slice())?;
                }
            }
            if let Some(text) = &new_text {
                for gram in search::trigrams(text) {
                    db.put(&mut *wtxn, search::postings_key(&gram, id).as_slice(), &[])?;
                }
            }
        }
        Ok(())
    }

    /// Wyszukiwanie full-text (BM25). Wyniki mają dodatkowe pole `_score`.
    pub fn search(
        &self,
        field: &str,
        query: &str,
        filter: &Value,
        skip: usize,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let rtxn = self.read_txn()?;
        let out = self.search_in(&rtxn, field, query, filter, skip, limit)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn search_in(
        &self,
        rtxn: &RoTxn<'_>,
        field: &str,
        query: &str,
        filter: &Value,
        skip: usize,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        if self
            .meta
            .get(rtxn, search::text_def_key(&self.name, field).as_bytes())?
            .is_none()
        {
            return Err(Error::Query(format!("no text index on field `{field}`")));
        }
        let Some(postings) = self.text_postings(rtxn, field)? else {
            return Err(Error::Query(format!("no text index on field `{field}`")));
        };
        let Some(lens) = self.text_len_db(rtxn, field)? else {
            return Err(Error::Query(format!("no text index on field `{field}`")));
        };

        let n_docs = lens.stat(rtxn)?.entries as f64;
        if n_docs == 0.0 {
            return Ok(Vec::new());
        }
        let total = self
            .meta
            .get(rtxn, search::text_total_key(&self.name, field).as_bytes())?
            .and_then(search::read_u64)
            .unwrap_or(0) as f64;
        let avgdl = total / n_docs;

        let phrases = search::parse_phrases(query);
        let mut terms = search::tokenize(query);
        terms.sort();
        terms.dedup();
        if terms.is_empty() {
            return Ok(Vec::new());
        }

        let limit = limit.unwrap_or(10).min(1000);
        let mut scores: HashMap<String, f64> = HashMap::new();
        for term in &terms {
            let prefix = search::posting_prefix(term);
            let mut hits: Vec<(String, f64)> = Vec::new();
            for item in postings.prefix_iter(rtxn, prefix.as_slice())? {
                let (key, tf) = item?;
                if let Some(id) = search::parse_posting(key) {
                    hits.push((id.to_string(), search::read_u32(tf).unwrap_or(1) as f64));
                }
            }
            let idf = search::idf(n_docs, hits.len() as f64);
            for (id, tf) in hits {
                let dl = lens
                    .get(rtxn, id.as_bytes())?
                    .and_then(search::read_u32)
                    .unwrap_or(0) as f64;
                *scores.entry(id).or_insert(0.0) += search::bm25_term(tf, dl, avgdl, idf);
            }
        }

        let mut ranked: Vec<(String, f64)> = scores.into_iter().collect();
        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        let mut out = Vec::new();
        let mut skipped = 0usize;
        for (id, score) in ranked {
            if out.len() >= limit {
                break;
            }
            let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                continue;
            };
            let mut doc: Value = parse_json(bytes)?;
            if !matches(&doc, filter) {
                continue;
            }
            if !phrases.is_empty() {
                let text = index::field_value(&doc, field)
                    .and_then(search::field_text)
                    .unwrap_or_default();
                let doc_tokens = search::tokenize(&text);
                if !phrases
                    .iter()
                    .all(|p| search::contains_phrase(&doc_tokens, p))
                {
                    continue;
                }
            }
            if skipped < skip {
                skipped += 1;
                continue;
            }
            if let Some(obj) = doc.as_object_mut() {
                obj.insert("_score".to_string(), serde_json::json!(score));
            }
            out.push(doc);
        }
        Ok(out)
    }

    /// Wyszukiwanie przybliżone (trigramy + Levenshtein). Wyniki mają `_distance`.
    pub fn fuzzy_search(
        &self,
        field: &str,
        query: &str,
        max_distance: usize,
        filter: &Value,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let rtxn = self.read_txn()?;
        let out = self.fuzzy_search_in(&rtxn, field, query, max_distance, filter, limit)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn fuzzy_search_in(
        &self,
        rtxn: &RoTxn<'_>,
        field: &str,
        query: &str,
        max_distance: usize,
        filter: &Value,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        if self
            .meta
            .get(rtxn, search::fuzzy_def_key(&self.name, field).as_bytes())?
            .is_none()
        {
            return Err(Error::Query(format!("no fuzzy index on field `{field}`")));
        }
        let Some(db) = self.fuzzy_db(rtxn, field)? else {
            return Err(Error::Query(format!("no fuzzy index on field `{field}`")));
        };
        let query = search::normalize(query);
        if query.is_empty() {
            return Ok(Vec::new());
        }

        // Kandydaci: dokumenty współdzielące co najmniej jeden trigram.
        let mut counts: HashMap<String, u32> = HashMap::new();
        for gram in search::trigrams(&query) {
            let prefix = search::posting_prefix(&gram);
            for item in db.prefix_iter(rtxn, prefix.as_slice())? {
                let (key, _) = item?;
                if let Some(id) = search::parse_posting(key) {
                    *counts.entry(id.to_string()).or_insert(0) += 1;
                }
            }
        }
        let mut candidates: Vec<(String, u32)> = counts.into_iter().collect();
        candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        candidates.truncate(5000);

        let mut scored: Vec<(usize, String, Value)> = Vec::new();
        for (id, _) in candidates {
            let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                continue;
            };
            let doc: Value = parse_json(bytes)?;
            let Some(text) = index::field_value(&doc, field).and_then(search::field_text) else {
                continue;
            };
            let Some(distance) = search::fuzzy_distance(&text, &query, max_distance) else {
                continue;
            };
            if !matches(&doc, filter) {
                continue;
            }
            scored.push((distance, id, doc));
        }

        scored.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let limit = limit.unwrap_or(10).min(1000);
        Ok(scored
            .into_iter()
            .take(limit)
            .map(|(distance, _, mut doc)| {
                if let Some(obj) = doc.as_object_mut() {
                    obj.insert("_distance".to_string(), serde_json::json!(distance));
                }
                doc
            })
            .collect())
    }

    // ----------------------------- zapis -----------------------------

    /// Wstawia dokument w osobnej transakcji (wygodne w testach / bez daemona).
    pub fn insert_one(&self, doc: Value) -> Result<String> {
        let mut wtxn = self.write_txn()?;
        let id = self.insert_one_txn(&mut wtxn, doc)?;
        wtxn.commit()?;
        Ok(id)
    }

    /// Wstawia dokument w podanej transakcji. Nadaje `_id` (UUID v4), jeśli brak.
    pub fn insert_one_txn(&self, wtxn: &mut RwTxn<'_>, mut doc: Value) -> Result<String> {
        let obj = doc
            .as_object_mut()
            .ok_or_else(|| Error::Query("document must be an object".into()))?;

        let id = match obj.get("_id") {
            Some(Value::String(s)) => s.clone(),
            Some(_) => return Err(Error::InvalidId),
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                obj.insert("_id".to_string(), Value::String(id.clone()));
                id
            }
        };

        if self.db.get(&*wtxn, id.as_bytes())?.is_some() {
            return Err(Error::DuplicateKey(id));
        }
        self.validate_unique(&*wtxn, &doc, &id)?;

        let bytes = serde_json::to_vec(&doc)?;
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(Error::DocumentTooLarge(bytes.len(), MAX_DOCUMENT_BYTES));
        }
        self.db.put(&mut *wtxn, id.as_bytes(), &bytes)?;
        self.apply_index_entries(wtxn, None, Some(&doc), &id)?;
        self.apply_text_entries(wtxn, None, Some(&doc), &id)?;
        self.apply_fuzzy_entries(wtxn, None, Some(&doc), &id)?;
        Ok(id)
    }

    /// Aktualizacja z opcjonalnym upsertem; zwraca pełny wynik operacji.
    pub fn update_opts_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
    ) -> Result<UpdateOutcome> {
        let keys = self.matching_keys(&*wtxn, filter, multi)?;
        if keys.is_empty() {
            if !upsert {
                return Ok(UpdateOutcome {
                    matched: 0,
                    modified: 0,
                    upserted_id: None,
                });
            }
            let mut doc = seed_from_filter(filter);
            let filter_id = doc.get("_id").cloned();
            apply_upsert(&mut doc, update)?;
            // `_id` z filtra jest niezmienne; nie pozwól go zgubić przez update.
            if let Some(id) = filter_id {
                doc["_id"] = id;
            }
            let id = self.insert_one_txn(wtxn, doc)?;
            return Ok(UpdateOutcome {
                matched: 0,
                modified: 0,
                upserted_id: Some(id),
            });
        }

        let matched = keys.len();
        let mut modified = 0usize;
        for key in keys {
            let id = std::str::from_utf8(&key)
                .map_err(|e| Error::Query(e.to_string()))?
                .to_string();
            let raw = self
                .db
                .get(&*wtxn, key.as_slice())?
                .ok_or_else(|| Error::Query("document disappeared".into()))?
                .to_vec();
            let old: Value = parse_json(&raw)?;
            let mut new = old.clone();
            apply_update(&mut new, update)?;

            self.validate_unique(&*wtxn, &new, &id)?;
            let bytes = serde_json::to_vec(&new)?;
            if bytes.len() > MAX_DOCUMENT_BYTES {
                return Err(Error::DocumentTooLarge(bytes.len(), MAX_DOCUMENT_BYTES));
            }
            self.db.put(&mut *wtxn, key.as_slice(), &bytes)?;
            self.apply_index_entries(wtxn, Some(&old), Some(&new), &id)?;
            self.apply_text_entries(wtxn, Some(&old), Some(&new), &id)?;
            self.apply_fuzzy_entries(wtxn, Some(&old), Some(&new), &id)?;
            modified += 1;
        }
        Ok(UpdateOutcome {
            matched,
            modified,
            upserted_id: None,
        })
    }

    /// Stara sygnatura: zwraca tylko liczbę zmodyfikowanych dokumentów.
    pub fn update_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        update: &Value,
        multi: bool,
    ) -> Result<usize> {
        Ok(self
            .update_opts_txn(wtxn, filter, update, multi, false)?
            .modified)
    }

    pub fn update_opts(
        &self,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
    ) -> Result<UpdateOutcome> {
        let mut wtxn = self.write_txn()?;
        let out = self.update_opts_txn(&mut wtxn, filter, update, multi, upsert)?;
        wtxn.commit()?;
        Ok(out)
    }

    pub fn update(&self, filter: &Value, update: &Value, multi: bool) -> Result<usize> {
        Ok(self.update_opts(filter, update, multi, false)?.modified)
    }

    /// Usuwa pasujące dokumenty. Zwraca liczbę usuniętych.
    pub fn delete_txn(&self, wtxn: &mut RwTxn<'_>, filter: &Value, multi: bool) -> Result<usize> {
        let keys = self.matching_keys(&*wtxn, filter, multi)?;
        for key in &keys {
            let id = std::str::from_utf8(key)
                .map_err(|e| Error::Query(e.to_string()))?
                .to_string();
            let raw = self
                .db
                .get(&*wtxn, key.as_slice())?
                .ok_or_else(|| Error::Query("document disappeared".into()))?
                .to_vec();
            let old: Value = parse_json(&raw)?;
            self.db.delete(&mut *wtxn, key.as_slice())?;
            self.apply_index_entries(wtxn, Some(&old), None, &id)?;
            self.apply_text_entries(wtxn, Some(&old), None, &id)?;
            self.apply_fuzzy_entries(wtxn, Some(&old), None, &id)?;
        }
        Ok(keys.len())
    }

    pub fn delete(&self, filter: &Value, multi: bool) -> Result<usize> {
        let mut wtxn = self.write_txn()?;
        let n = self.delete_txn(&mut wtxn, filter, multi)?;
        wtxn.commit()?;
        Ok(n)
    }

    /// Zbiera klucze (`_id`) pasujące do filtra.
    ///
    /// Korzysta z tych samych szybkich ścieżek co `find`: dokładne `_id`,
    /// indeks wtórny, a dopiero na końcu pełny skan.
    fn matching_keys(&self, txn: &RoTxn<'_>, filter: &Value, multi: bool) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        let raw = raw_prefilter(filter);

        if let Some(id) = direct_id(filter) {
            if self.db.get(txn, id.as_bytes())?.is_some() {
                keys.push(id.as_bytes().to_vec());
            }
            return Ok(keys);
        }

        if let Some(ids) = self.index_candidates(txn, filter, None)? {
            for id in ids {
                let Some(bytes) = self.db.get(txn, id.as_bytes())? else {
                    continue;
                };
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, filter) {
                    keys.push(id.into_bytes());
                    if !multi {
                        break;
                    }
                }
            }
            return Ok(keys);
        }

        if let Some((lower, upper)) = id_bounds(filter) {
            let range = (as_bound(&lower), as_bound(&upper));
            for item in self.db.range(txn, &range)? {
                let (key, bytes) = item?;
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, filter) {
                    keys.push(key.to_vec());
                    if !multi {
                        break;
                    }
                }
            }
            return Ok(keys);
        }

        for item in self.db.iter(txn)? {
            let (key, bytes) = item?;
            let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                continue;
            };
            if matches(&doc, filter) {
                keys.push(key.to_vec());
                if !multi {
                    break;
                }
            }
        }
        Ok(keys)
    }

    // ----------------------------- odczyt -----------------------------

    pub fn find(&self, query: &FindQuery) -> Result<Vec<Value>> {
        let rtxn = self.read_txn()?;
        let limit = query.limit.unwrap_or(usize::MAX);
        let want = query.skip.saturating_add(limit);

        // Sortowanie całego zbioru to operacja CPU-bound. Gdy filtr nie ma
        // indeksu, parsowanie JSON i TopK można rozłożyć na wszystkie rdzenie.
        // Snapshot pozostaje spójny: wątek główny czyta jeden `RoTxn` i wysyła
        // wątkom tylko surowe bajty (nie współdzielimy kursora LMDB).
        let sorted = !query.sort.is_empty();
        let parallel_base = limit > 0
            && want <= PARALLEL_SCAN_MAX_WANT
            && self.db.stat(&rtxn)?.entries >= PARALLEL_SCAN_MIN_ENTRIES
            && direct_id(&query.filter).is_none()
            && id_bounds(&query.filter).is_none()
            && self.index_plan(&rtxn, &query.filter)?.is_none();
        let parallel = parallel_base
            && (sorted || (!is_match_all(&query.filter) && want >= PARALLEL_UNSORTED_MIN_WANT));

        let out = if parallel {
            let docs = if sorted {
                self.parallel_sorted_scan(&rtxn, query, want)?
            } else {
                self.parallel_unsorted_scan(&rtxn, query, want)?
            };
            docs.into_iter()
                .skip(query.skip)
                .take(limit)
                .map(|doc| match &query.projection {
                    Some(projection) => project(&doc, projection),
                    None => doc,
                })
                .collect()
        } else {
            self.find_in(&rtxn, query)?
        };
        rtxn.commit()?;
        Ok(out)
    }

    /// Równoległy backfill indeksów: wątek główny czyta jeden `RwTxn`,
    /// a wątki robocze parsują JSON i przygotowują wpisy. Zapis do LMDB
    /// pozostaje w wątku głównym (jedna transakcja).
    fn parallel_backfill<T, F>(&self, wtxn: &RwTxn<'_>, f: F) -> Result<Vec<T>>
    where
        T: Send,
        F: Fn(String, Value) -> Result<Vec<T>> + Sync,
    {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(16);
        if workers <= 1 {
            return self.backfill_serial(wtxn, f);
        }

        let (job_tx, job_rx) = std::sync::mpsc::sync_channel::<BackfillChunk>(workers * 2);
        let job_rx = Arc::new(Mutex::new(job_rx));
        let (res_tx, res_rx) = std::sync::mpsc::channel::<Result<Vec<T>>>();
        let func = &f;

        std::thread::scope(|scope| -> Result<Vec<T>> {
            for _ in 0..workers {
                let rx = Arc::clone(&job_rx);
                let tx = res_tx.clone();
                scope.spawn(move || loop {
                    let job = {
                        let guard = rx.lock().expect("kolejka backfillu");
                        guard.recv()
                    };
                    match job {
                        Ok(chunk) => {
                            let mut out = Vec::new();
                            for (id_start, id_len, doc_start, doc_len) in chunk.docs {
                                let id = std::str::from_utf8(
                                    &chunk.data[id_start as usize..][..id_len as usize],
                                )
                                .map_err(|e| Error::Query(e.to_string()))
                                .and_then(|id| {
                                    let doc = serde_json::from_slice::<Value>(
                                        &chunk.data[doc_start as usize..][..doc_len as usize],
                                    )?;
                                    func(id.to_string(), doc)
                                });
                                match id {
                                    Ok(entries) => out.extend(entries),
                                    Err(e) => {
                                        let _ = tx.send(Err(e));
                                        return;
                                    }
                                }
                            }
                            let _ = tx.send(Ok(out));
                        }
                        Err(_) => break,
                    }
                });
            }
            drop(res_tx);

            let mut chunk = BackfillChunk {
                data: Vec::new(),
                docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
            };
            for item in self.db.iter(&*wtxn)? {
                let (id_bytes, doc_bytes) = item?;
                let id_start = chunk.data.len() as u32;
                chunk.data.extend_from_slice(id_bytes);
                let id_len = id_bytes.len() as u32;
                let doc_start = chunk.data.len() as u32;
                chunk.data.extend_from_slice(doc_bytes);
                let doc_len = doc_bytes.len() as u32;
                chunk.docs.push((id_start, id_len, doc_start, doc_len));
                if chunk.docs.len() >= PARALLEL_SCAN_CHUNK {
                    let full = std::mem::replace(
                        &mut chunk,
                        BackfillChunk {
                            data: Vec::new(),
                            docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
                        },
                    );
                    let _ = job_tx.send(full);
                }
            }
            if !chunk.docs.is_empty() {
                let _ = job_tx.send(chunk);
            }
            drop(job_tx);

            let mut all = Vec::new();
            for res in res_rx {
                all.extend(res?);
            }
            Ok(all)
        })
    }

    fn backfill_serial<T, F>(&self, wtxn: &RwTxn<'_>, f: F) -> Result<Vec<T>>
    where
        F: Fn(String, Value) -> Result<Vec<T>>,
    {
        let mut out = Vec::new();
        for item in self.db.iter(wtxn)? {
            let (id_bytes, doc_bytes) = item?;
            let id = std::str::from_utf8(id_bytes)
                .map_err(|e| Error::Query(e.to_string()))?
                .to_string();
            let doc: Value = parse_json(doc_bytes)?;
            out.extend(f(id, doc)?);
        }
        Ok(out)
    }

    /// Równoległy pełny skan z sortowaniem.
    ///
    /// Wątek główny iteruje po jednym `RoTxn` (spójny snapshot) i wrzuca porcje
    /// surowych bajtów do kolejki. Wątki robocze parsują JSON, filtrują i liczą
    /// lokalne TopK; na końcu wynik jest scalany z zachowaniem kolejności `seq`.
    fn parallel_sorted_scan(
        &self,
        rtxn: &RoTxn<'_>,
        query: &FindQuery,
        want: usize,
    ) -> Result<Vec<Value>> {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(16);
        if workers <= 1 || want == 0 {
            return self.scan_sorted_serial(rtxn, query, want);
        }

        let (job_tx, job_rx) = std::sync::mpsc::sync_channel::<ScanChunk>(workers * 2);
        let job_rx = Arc::new(Mutex::new(job_rx));
        let (res_tx, res_rx) = std::sync::mpsc::channel::<Result<Vec<(usize, Value)>>>();
        let sort = query.sort.as_slice();
        let filter = &query.filter;
        let raw = raw_prefilter(filter);
        let raw = raw.as_ref();

        std::thread::scope(|scope| -> Result<Vec<Value>> {
            for _ in 0..workers {
                let rx = Arc::clone(&job_rx);
                let tx = res_tx.clone();
                scope.spawn(move || {
                    let mut top = TopK::new(sort, want);
                    let mut failed = false;
                    loop {
                        let job = {
                            let guard = rx.lock().expect("kolejka zadań");
                            guard.recv()
                        };
                        match job {
                            Ok(chunk) => {
                                for (seq, start, len) in chunk.docs {
                                    let bytes = &chunk.data[start as usize..][..len as usize];
                                    match parse_prefiltered(bytes, raw) {
                                        Ok(Some(doc)) => {
                                            if matches(&doc, filter) {
                                                top.push_seq(doc, seq);
                                            }
                                        }
                                        Ok(None) => {}
                                        Err(e) => {
                                            if !failed {
                                                failed = true;
                                                let _ = tx.send(Err(e));
                                            }
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    if !failed {
                        let _ = tx.send(Ok(top.into_sorted_seq()));
                    }
                });
            }
            drop(res_tx);

            let mut chunk = ScanChunk {
                idx: 0,
                data: Vec::new(),
                docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
            };
            for (seq, item) in self.db.iter(rtxn)?.enumerate() {
                let (_key, bytes) = item?;
                let start = chunk.data.len() as u32;
                chunk.data.extend_from_slice(bytes);
                chunk.docs.push((seq, start, bytes.len() as u32));
                if chunk.docs.len() >= PARALLEL_SCAN_CHUNK {
                    let full = std::mem::replace(
                        &mut chunk,
                        ScanChunk {
                            idx: 0,
                            data: Vec::new(),
                            docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
                        },
                    );
                    let _ = job_tx.send(full);
                }
            }
            if !chunk.docs.is_empty() {
                let _ = job_tx.send(chunk);
            }
            drop(job_tx);

            let mut merged = TopK::new(sort, want);
            for res in res_rx {
                for (seq, doc) in res? {
                    merged.push_seq(doc, seq);
                }
            }
            Ok(merged.into_sorted())
        })
    }

    /// Równoległy pełny skan bez sortowania.
    ///
    /// Porcje są wysyłane falami: po jednej do każdego wątku. Koordynator
    /// odbiera całą falę i składa wyniki w kolejności `_id`. Gdy uzbiera
    /// `want` trafień, przestaje wysyłać kolejne porcje. Dzięki temu szerokie
    /// filtry zachowują wczesne wyjście, a selektywne skanują równolegle.
    fn parallel_unsorted_scan(
        &self,
        rtxn: &RoTxn<'_>,
        query: &FindQuery,
        want: usize,
    ) -> Result<Vec<Value>> {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(16);
        if workers <= 1 || want == 0 {
            return self.find_in(rtxn, query);
        }

        let mut job_txs = Vec::with_capacity(workers);
        let mut job_rxs = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, rx) = std::sync::mpsc::sync_channel::<ScanChunk>(1);
            job_txs.push(tx);
            job_rxs.push(rx);
        }
        let (res_tx, res_rx) = std::sync::mpsc::channel::<Result<(usize, Vec<(usize, Value)>)>>();
        let filter = &query.filter;
        let raw = raw_prefilter(filter);
        let raw = raw.as_ref();
        // Utwórz iterator przed startem wątków: błąd LMDB nie może zostawić
        // workerów czekających na kanał, który nigdy nie zostanie zamknięty.
        let mut iter = self.db.iter(rtxn)?;

        std::thread::scope(|scope| -> Result<Vec<Value>> {
            for rx in job_rxs {
                let tx = res_tx.clone();
                scope.spawn(move || {
                    let mut failed = false;
                    loop {
                        let job = rx.recv();
                        let Ok(chunk) = job else {
                            break;
                        };
                        let mut out = Vec::new();
                        if !failed {
                            for (seq, start, len) in chunk.docs {
                                let bytes = &chunk.data[start as usize..][..len as usize];
                                match parse_prefiltered(bytes, raw) {
                                    Ok(Some(doc)) => {
                                        if matches(&doc, filter) {
                                            out.push((seq, doc));
                                            if out.len() >= want {
                                                break;
                                            }
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(e) => {
                                        failed = true;
                                        let _ = tx.send(Err(e));
                                        break;
                                    }
                                }
                            }
                        }
                        if !failed {
                            let _ = tx.send(Ok((chunk.idx, out)));
                        }
                    }
                });
            }
            drop(res_tx);

            let mut merged = TopK::new(&[], want);
            let mut seq = 0usize;
            let mut next_chunk = 0usize;
            let mut expected = 0usize;
            let mut exhausted = false;
            let mut error: Option<Error> = None;

            // Pierwsza fala jest mała, żeby szeroki filtr szybko się zatrzymał.
            // Dopiero gdy nie wystarczy, zwiększamy falę i amortyzujemy
            // synchronizację przy selektywnym pełnym skanie.
            let mut rounds = 1usize;
            'scan: loop {
                let mut sent = 0usize;
                'fill: for _ in 0..rounds {
                    for tx in &job_txs {
                        if exhausted {
                            break 'fill;
                        }
                        let mut chunk = ScanChunk {
                            idx: next_chunk,
                            data: Vec::new(),
                            docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
                        };
                        while chunk.docs.len() < PARALLEL_SCAN_CHUNK {
                            match iter.next() {
                                Some(item) => match item {
                                    Ok((_key, bytes)) => {
                                        let start = chunk.data.len() as u32;
                                        chunk.data.extend_from_slice(bytes);
                                        chunk.docs.push((seq, start, bytes.len() as u32));
                                        seq += 1;
                                    }
                                    Err(e) => {
                                        error = Some(e.into());
                                        break 'scan;
                                    }
                                },
                                None => {
                                    exhausted = true;
                                    break;
                                }
                            }
                        }
                        if chunk.docs.is_empty() {
                            break 'fill;
                        }
                        next_chunk += 1;
                        if tx.send(chunk).is_err() {
                            error = Some(Error::Query("worker skanujący zakończył pracę".into()));
                            break 'scan;
                        }
                        sent += 1;
                    }
                }
                if sent == 0 {
                    break;
                }

                let mut slots: Vec<Option<Vec<(usize, Value)>>> = vec![None; sent];
                for _ in 0..sent {
                    let msg = match res_rx.recv() {
                        Ok(msg) => msg,
                        Err(_) => {
                            error = Some(Error::Query("brak wyniku skanu równoległego".into()));
                            break 'scan;
                        }
                    };
                    let (idx, docs) = match msg {
                        Ok(v) => v,
                        Err(e) => {
                            error = Some(e);
                            break 'scan;
                        }
                    };
                    let pos = idx.checked_sub(expected).unwrap_or(usize::MAX);
                    if pos >= sent {
                        error = Some(Error::Query("nieoczekiwana porcja skanu".into()));
                        break 'scan;
                    }
                    slots[pos] = Some(docs);
                }
                for docs in slots.into_iter().flatten() {
                    for (seq, doc) in docs {
                        merged.push_seq(doc, seq);
                    }
                    expected += 1;
                    if merged.heap.len() >= want {
                        break 'scan;
                    }
                }
                rounds = if want <= 1024 {
                    4
                } else if want <= 4096 {
                    2
                } else {
                    1
                };
            }
            drop(job_txs);
            match error {
                Some(e) => Err(e),
                None => Ok(merged.into_sorted()),
            }
        })
    }

    /// Wersja jednowątkowa używana jako fallback (np. 1 rdzeń).
    fn scan_sorted_serial(
        &self,
        rtxn: &RoTxn<'_>,
        query: &FindQuery,
        want: usize,
    ) -> Result<Vec<Value>> {
        let mut top = TopK::new(&query.sort, want);
        let raw = raw_prefilter(&query.filter);
        for (seq, item) in self.db.iter(rtxn)?.enumerate() {
            let (_key, bytes) = item?;
            let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                continue;
            };
            if matches(&doc, &query.filter) {
                top.push_seq(doc, seq);
            }
        }
        Ok(top.into_sorted())
    }

    pub fn find_in(&self, rtxn: &RoTxn<'_>, query: &FindQuery) -> Result<Vec<Value>> {
        let limit = query.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let sorted = !query.sort.is_empty();
        // Bez sortowania wystarczy `skip + limit` trafień (wczesne wyjście).
        // Ze sortowaniem TopK trzyma tylko `want` najlepszych dokumentów.
        let want = query.skip.saturating_add(limit);
        let mut acc = Acc::new(sorted, &query.sort, want);
        let mut post_skip = query.skip;
        let raw = raw_prefilter(&query.filter);

        // 1) Dokładne `_id` -> bezpośredni get.
        if let Some(id) = direct_id(&query.filter) {
            if let Some(bytes) = self.db.get(rtxn, id.as_bytes())? {
                acc.push(parse_json(bytes)?);
            }
        }
        // 2) Indeks wtórny. Najpierw szybka ścieżka „index-only”: jeśli
        //    sortowanie dokładnie odpowiada kolejności indeksu, idziemy po nim
        //    i pobieramy tylko `skip + limit` dokumentów, zamiast całego zakresu.
        else if let Some(plan) = self.index_plan(rtxn, &query.filter)? {
            if sorted {
                if let Some(reverse) = index_matches_sort(&plan.def, &query.sort) {
                    let (lower, upper) = &plan.bounds;
                    let range = (as_bound(lower), as_bound(upper));
                    let mut skipped = 0usize;
                    let mut out = Vec::new();
                    let iter: IndexIter<'_> = if reverse {
                        Box::new(plan.db.rev_range(rtxn, &range)?)
                    } else {
                        Box::new(plan.db.range(rtxn, &range)?)
                    };
                    for item in iter {
                        let (_key, value) = item?;
                        let Some((id, inc)) = index::decode_index_value(value) else {
                            continue;
                        };
                        let doc = if let Some(doc) =
                            covering_doc(&id, inc.as_ref(), &query.filter, &query.projection)
                        {
                            doc
                        } else {
                            let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                                continue;
                            };
                            let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                                continue;
                            };
                            doc
                        };
                        if !matches(&doc, &query.filter) {
                            continue;
                        }
                        if skipped < query.skip {
                            skipped += 1;
                            continue;
                        }
                        out.push(doc);
                        if out.len() >= limit {
                            break;
                        }
                    }
                    return Ok(out
                        .into_iter()
                        .map(|doc| match &query.projection {
                            Some(projection) => project(&doc, projection),
                            None => doc,
                        })
                        .collect());
                }
            }

            let ids = self.index_candidates(rtxn, &query.filter, (!sorted).then_some(want))?;
            for id in ids.into_iter().flatten() {
                if !sorted && acc.len() >= want {
                    break;
                }
                if let Some(bytes) = self.db.get(rtxn, id.as_bytes())? {
                    let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                        continue;
                    };
                    // Residual filter: indeks może zwrócić nadmiarowe wpisy.
                    if matches(&doc, &query.filter) {
                        acc.push(doc);
                    }
                }
            }
        }
        // 3) Zakres `_id` po kluczu głównym. To nie jest indeks wtórny, ale
        //    pozwala zawęzić pełny skan do ciągłego przedziału B+drzewa.
        else if let Some((lower, upper)) = id_bounds(&query.filter) {
            let range = (as_bound(&lower), as_bound(&upper));
            let reverse = id_sort_direction(&query.sort).unwrap_or(false);
            let iter: IndexIter<'_> = if reverse {
                Box::new(self.db.rev_range(rtxn, &range)?)
            } else {
                Box::new(self.db.range(rtxn, &range)?)
            };

            if !sorted || id_sort_direction(&query.sort).is_some() {
                let mut skipped = 0usize;
                let mut out = Vec::new();
                for item in iter {
                    let (_key, bytes) = item?;
                    let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                        continue;
                    };
                    if !matches(&doc, &query.filter) {
                        continue;
                    }
                    if skipped < query.skip {
                        skipped += 1;
                        continue;
                    }
                    out.push(doc);
                    if out.len() >= limit {
                        break;
                    }
                }
                return Ok(out
                    .into_iter()
                    .map(|doc| match &query.projection {
                        Some(projection) => project(&doc, projection),
                        None => doc,
                    })
                    .collect());
            }

            let mut top = TopK::new(&query.sort, want);
            for item in iter {
                let (_key, bytes) = item?;
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, &query.filter) {
                    top.push(doc);
                }
            }
            return Ok(top
                .into_sorted()
                .into_iter()
                .skip(query.skip)
                .take(limit)
                .map(|doc| match &query.projection {
                    Some(projection) => project(&doc, projection),
                    None => doc,
                })
                .collect());
        }
        // 4) Pełny skan bez sortowania: pomijamy `skip` od razu, bez zbierania
        //     pominiętych dokumentów i bez końcowego sortowania całej paczki.
        else if !sorted {
            post_skip = 0;
            if is_match_all(&query.filter) {
                // Naturalna kolejność = `_id`; pominiętych nie trzeba parsować.
                for item in self.db.iter(rtxn)?.skip(query.skip).take(limit) {
                    let (_key, bytes) = item?;
                    acc.push(parse_json(bytes)?);
                }
            } else {
                let mut skipped = 0usize;
                for item in self.db.iter(rtxn)? {
                    let (_key, bytes) = item?;
                    let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                        continue;
                    };
                    if matches(&doc, &query.filter) {
                        if skipped < query.skip {
                            skipped += 1;
                            continue;
                        }
                        acc.push(doc);
                        if acc.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }
        // 3b) Pełny skan z sortowaniem: zbieramy trafienia przez TopK.
        else {
            for item in self.db.iter(rtxn)? {
                let (_key, bytes) = item?;
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, &query.filter) {
                    acc.push(doc);
                }
            }
        }

        let docs = acc.finish();
        Ok(docs
            .into_iter()
            .skip(post_skip)
            .take(limit)
            .map(|doc| match &query.projection {
                Some(projection) => project(&doc, projection),
                None => doc,
            })
            .collect())
    }

    pub fn find_one(&self, filter: &Value) -> Result<Option<Value>> {
        let query = FindQuery {
            filter: filter.clone(),
            sort: Vec::new(),
            skip: 0,
            limit: Some(1),
            projection: None,
        };
        Ok(self.find(&query)?.into_iter().next())
    }

    pub fn find_one_in(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
        projection: Option<&Value>,
    ) -> Result<Option<Value>> {
        let query = FindQuery {
            filter: filter.clone(),
            sort: Vec::new(),
            skip: 0,
            limit: Some(1),
            projection: projection.cloned(),
        };
        Ok(self.find_in(rtxn, &query)?.into_iter().next())
    }

    pub fn count(&self, filter: &Value) -> Result<usize> {
        let rtxn = self.read_txn()?;
        let n = self.count_in(&rtxn, filter)?;
        rtxn.commit()?;
        Ok(n)
    }

    pub fn count_in(&self, rtxn: &RoTxn<'_>, filter: &Value) -> Result<usize> {
        // `count({})` / `count(null)` -> O(1) z metadanych B+drzewa.
        if is_match_all(filter) {
            return Ok(self.db.stat(rtxn)?.entries);
        }
        // Dokładne `_id` -> bezpośredni `get`.
        if let Some(id) = direct_id(filter) {
            return Ok(usize::from(self.db.get(rtxn, id.as_bytes())?.is_some()));
        }
        // Zakres `_id` po kluczu głównym. Gdy filtr zawiera tylko warunki na
        // `_id`, wystarczy policzyć wpisy w zakresie bez parsowania JSON.
        if let Some((lower, upper)) = id_bounds(filter) {
            let range = (as_bound(&lower), as_bound(&upper));
            if filter.as_object().is_some_and(|o| o.len() == 1) {
                let mut n = 0usize;
                for item in self.db.range(rtxn, &range)? {
                    item?;
                    n += 1;
                }
                return Ok(n);
            }
            let mut n = 0usize;
            let raw = raw_prefilter(filter);
            for item in self.db.range(rtxn, &range)? {
                let (_key, bytes) = item?;
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, filter) {
                    n += 1;
                }
            }
            return Ok(n);
        }
        // Filtr w całości pokryty indeksem: policz klucze, bez parsowania JSON.
        if let Some(plan) = self.index_plan(rtxn, filter)? {
            if index_filter_exact(&plan.def, filter) {
                return self.index_count_plan(rtxn, &plan);
            }
        }
        let mut n = 0;
        let raw = raw_prefilter(filter);
        for item in self.db.iter(rtxn)? {
            let (_key, bytes) = item?;
            let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                continue;
            };
            if matches(&doc, filter) {
                n += 1;
            }
        }
        Ok(n)
    }

    fn read_txn(&self) -> Result<RoTxn<'_>> {
        Ok(self.env.read_txn()?)
    }

    fn write_txn(&self) -> Result<RwTxn<'_>> {
        Ok(self.env.write_txn()?)
    }
}

// ----------------------------- pomocnicze -----------------------------

/// Wartość równościowa z warunku (`5` albo `{"$eq": 5}`).
fn eq_value(cond: &Value) -> Option<&Value> {
    if let Some(obj) = cond.as_object() {
        if obj.len() == 1 {
            return obj.get("$eq");
        }
        return None;
    }
    Some(cond)
}

/// Buduje zakres indeksu dla filtra, jeśli wszystkie pola definicji mają
/// warunki, a prefiks (wszystko poza ostatnim polem) jest równościowy.
fn bounds_for_index(def: &IndexDef, filter: &Map<String, Value>) -> Option<index::RangeBytes> {
    let fields = def.effective_fields();
    if fields.is_empty() {
        return None;
    }
    let mut prefix = Vec::new();
    for (i, field) in fields.iter().enumerate() {
        let cond = filter.get(field)?;
        // `null` pasuje też do braku pola, a braków nie ma w indeksie.
        if cond.is_null()
            || cond
                .as_object()
                .and_then(|o| o.get("$eq"))
                .is_some_and(Value::is_null)
        {
            return None;
        }
        if i + 1 == fields.len() {
            let bounds = index::bounds_from_cond(cond)?;
            return Some(if prefix.is_empty() {
                bounds
            } else {
                index::prefix_bounds(&prefix, bounds)
            });
        }
        let v = eq_value(cond)?;
        prefix.extend(index::encode_value(v)?);
    }
    Some(index::exact_prefix(&prefix))
}

fn is_match_all(filter: &Value) -> bool {
    filter.is_null() || filter.as_object().is_some_and(|o| o.is_empty())
}

/// Czy `count` może zaufać indeksowi bez czytania dokumentów.
///
/// Wykluczamy `null` (matcher traktuje brak pola jak `null`, a braków nie ma
/// w indeksie) oraz operatory, których indeks nie obsługuje.
fn count_cond_safe(cond: &Value) -> bool {
    match cond {
        Value::Null => false,
        Value::Object(ops) => {
            if ops.is_empty() || ops.values().any(Value::is_null) {
                return false;
            }
            // `$eq` tylko samodzielnie; zakresy można łączyć (`$gte` + `$lt`).
            if ops.contains_key("$eq") && ops.len() != 1 {
                return false;
            }
            ops.keys()
                .all(|op| matches!(op.as_str(), "$eq" | "$gt" | "$gte" | "$lt" | "$lte"))
        }
        _ => true,
    }
}

/// Filtr dokładnie pokrywa wszystkie pola indeksu (bez nadmiarowych warunków),
/// więc liczba wpisów w zakresie równa się liczbie pasujących dokumentów.
fn index_filter_exact(def: &IndexDef, filter: &Value) -> bool {
    let Some(obj) = filter.as_object() else {
        return false;
    };
    let fields = def.effective_fields();
    obj.len() == fields.len()
        && fields
            .iter()
            .all(|f| obj.get(f).is_some_and(count_cond_safe))
}

fn as_bound(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn value_at<'a>(doc: &'a Value, path_str: &str) -> Option<&'a Value> {
    path::get(doc, path_str)
}

/// Rozpoznaje filtr równoważny wyszukiwaniu po `_id`.
fn direct_id(filter: &Value) -> Option<&str> {
    let obj = filter.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    match obj.get("_id")? {
        Value::String(id) => Some(id),
        Value::Object(op) if op.len() == 1 => op.get("$eq")?.as_str(),
        _ => None,
    }
}

/// Buduje zakres klucza głównego `_id` z warunków `$gt/$gte/$lt/$lte`.
/// Dokładny `_id` (string albo `{"$eq": ...}`) też jest zakresem, co pozwala
/// łączyć go z dodatkowymi warunkami bez pełnego skanu.
fn id_bounds(filter: &Value) -> Option<IdBounds> {
    let cond = filter.as_object()?.get("_id")?;
    if let Some(id) = cond.as_str() {
        let key = id.as_bytes().to_vec();
        return Some((Bound::Included(key.clone()), Bound::Included(key)));
    }
    let ops = cond.as_object()?;
    if ops.is_empty() {
        return None;
    }
    if let Some(eq) = ops.get("$eq") {
        if ops.len() != 1 {
            return None;
        }
        let key = eq.as_str()?.as_bytes().to_vec();
        return Some((Bound::Included(key.clone()), Bound::Included(key)));
    }

    let mut lower = Bound::Unbounded;
    let mut upper = Bound::Unbounded;
    let mut saw = false;
    for (op, value) in ops {
        let key = value.as_str()?.as_bytes().to_vec();
        match op.as_str() {
            "$gt" => lower = tighter_lower(lower, Bound::Excluded(key)),
            "$gte" => lower = tighter_lower(lower, Bound::Included(key)),
            "$lt" => upper = tighter_upper(upper, Bound::Excluded(key)),
            "$lte" => upper = tighter_upper(upper, Bound::Included(key)),
            _ => return None,
        }
        saw = true;
    }
    saw.then_some((lower, upper))
}

fn tighter_lower(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (a, b) {
        (Bound::Unbounded, b) => b,
        (a, Bound::Unbounded) => a,
        (Bound::Included(a), Bound::Included(b)) => Bound::Included(a.max(b)),
        (Bound::Excluded(a), Bound::Excluded(b)) => Bound::Excluded(a.max(b)),
        (Bound::Included(a), Bound::Excluded(b)) => {
            if a > b {
                Bound::Included(a)
            } else {
                Bound::Excluded(b)
            }
        }
        (Bound::Excluded(a), Bound::Included(b)) => {
            if b > a {
                Bound::Included(b)
            } else {
                Bound::Excluded(a)
            }
        }
    }
}

fn tighter_upper(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (a, b) {
        (Bound::Unbounded, b) => b,
        (a, Bound::Unbounded) => a,
        (Bound::Included(a), Bound::Included(b)) => Bound::Included(a.min(b)),
        (Bound::Excluded(a), Bound::Excluded(b)) => Bound::Excluded(a.min(b)),
        (Bound::Included(a), Bound::Excluded(b)) => {
            if a < b {
                Bound::Included(a)
            } else {
                Bound::Excluded(b)
            }
        }
        (Bound::Excluded(a), Bound::Included(b)) => {
            if b < a {
                Bound::Included(b)
            } else {
                Bound::Excluded(a)
            }
        }
    }
}

/// Czy sortowanie to pojedyncze `_id`? Zwraca `Some(reverse)` dla `-1`.
fn id_sort_direction(sort: &[(String, i32)]) -> Option<bool> {
    if sort.len() == 1 && sort[0].0 == "_id" {
        Some(sort[0].1 < 0)
    } else {
        None
    }
}

fn compare_values(a: Option<&Value>, b: Option<&Value>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater, // brak pola na końcu
        (Some(_), None) => Ordering::Less,
        (Some(a), Some(b)) => match (a, b) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater,
            (_, Value::Null) => Ordering::Less,
            (Value::Number(x), Value::Number(y)) => x
                .as_f64()
                .partial_cmp(&y.as_f64())
                .unwrap_or(Ordering::Equal),
            (Value::String(x), Value::String(y)) => x.cmp(y),
            (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
            _ => Ordering::Equal,
        },
    }
}

/// Buduje dokument z pól pokrywających indeksu, jeśli filtr i projekcja
/// mieszczą się w `inc`. W przeciwnym razie zwraca `None` (trzeba pobrać dokument).
fn covering_doc(
    id: &str,
    inc: Option<&Map<String, Value>>,
    filter: &Value,
    projection: &Option<Value>,
) -> Option<Value> {
    let inc = inc?;
    let proj = projection.as_ref()?.as_object()?;
    // Tylko projekcja inkluzywna: przy wykluczającej trzeba mieć cały dokument.
    if !proj.iter().any(|(k, v)| k.as_str() != "_id" && truthy(v)) {
        return None;
    }
    for key in proj.keys().filter(|k| k.as_str() != "_id") {
        if key.contains('.') || key.starts_with('$') || !inc.contains_key(key) {
            return None;
        }
    }
    for key in filter.as_object()?.keys() {
        if key != "_id" && (key.contains('.') || !inc.contains_key(key)) {
            return None;
        }
    }
    let mut obj = inc.clone();
    obj.insert("_id".to_string(), Value::String(id.to_string()));
    Some(Value::Object(obj))
}

/// Czy kolejność indeksu odpowiada sortowaniu? Zwraca `Some(reverse)` gdy
/// sortowanie używa dokładnie tych samych pól (w tej samej kolejności) i
/// wszystkich kierunków dodatnich (`false`) albo wszystkich ujemnych (`true`).
fn index_matches_sort(def: &IndexDef, sort: &[(String, i32)]) -> Option<bool> {
    let fields = def.effective_fields();
    if fields.len() != sort.len() || sort.is_empty() {
        return None;
    }
    if fields.iter().zip(sort).any(|(f, (s, _))| f != s) {
        return None;
    }
    let positive = sort[0].1 >= 0;
    if sort.iter().any(|(_, d)| (*d >= 0) != positive) {
        return None;
    }
    Some(!positive)
}

fn compare_docs(a: &Value, b: &Value, sort: &[(String, i32)]) -> Ordering {
    for (field, dir) in sort {
        let ord = compare_values(value_at(a, field), value_at(b, field));
        if ord != Ordering::Equal {
            return if *dir >= 0 { ord } else { ord.reverse() };
        }
    }
    Ordering::Equal
}

/// Akumulator wyników `find`: bez sortowania zwykła lista, z sortowaniem TopK.
enum Acc<'a> {
    All(Vec<Value>),
    Top(TopK<'a>),
}

impl<'a> Acc<'a> {
    fn new(sorted: bool, sort: &'a [(String, i32)], want: usize) -> Self {
        if sorted {
            Acc::Top(TopK::new(sort, want))
        } else {
            Acc::All(Vec::new())
        }
    }

    fn push(&mut self, doc: Value) {
        match self {
            Acc::All(docs) => docs.push(doc),
            Acc::Top(top) => top.push(doc),
        }
    }

    fn len(&self) -> usize {
        match self {
            Acc::All(docs) => docs.len(),
            Acc::Top(top) => top.heap.len(),
        }
    }

    fn finish(self) -> Vec<Value> {
        match self {
            Acc::All(mut docs) => {
                // Deterministyczna kolejność zwróconego zbioru.
                docs.sort_by(|a, b| {
                    let ka = a.get("_id").and_then(Value::as_str).unwrap_or("");
                    let kb = b.get("_id").and_then(Value::as_str).unwrap_or("");
                    ka.cmp(kb)
                });
                docs
            }
            Acc::Top(top) => top.into_sorted(),
        }
    }
}

/// Dokument w kopcu TopK; `seq` zachowuje stabilność przy równych kluczach.
struct TopDoc<'a> {
    doc: Value,
    seq: usize,
    sort: &'a [(String, i32)],
}

impl PartialEq for TopDoc<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for TopDoc<'_> {}

impl PartialOrd for TopDoc<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopDoc<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Większy = gorszy, więc na szczycie kopca jest najgorszy kandydat.
        match compare_docs(&self.doc, &other.doc, self.sort) {
            Ordering::Equal => self.seq.cmp(&other.seq),
            ord => ord,
        }
    }
}

/// Ograniczony wybór `want` najlepszych dokumentów bez sortowania całości.
struct TopK<'a> {
    sort: &'a [(String, i32)],
    want: usize,
    seq: usize,
    heap: BinaryHeap<TopDoc<'a>>,
}

impl<'a> TopK<'a> {
    fn new(sort: &'a [(String, i32)], want: usize) -> Self {
        Self {
            sort,
            want,
            seq: 0,
            heap: BinaryHeap::new(),
        }
    }

    fn push(&mut self, doc: Value) {
        self.seq += 1;
        self.push_seq(doc, self.seq);
    }

    /// Wstawia dokument z globalnym numerem kolejności (stabilność sortowania).
    fn push_seq(&mut self, doc: Value, seq: usize) {
        if self.want == 0 {
            return;
        }
        let entry = TopDoc {
            doc,
            seq,
            sort: self.sort,
        };
        if self.heap.len() < self.want {
            self.heap.push(entry);
        } else if self
            .heap
            .peek()
            .is_some_and(|worst| entry.cmp(worst) == Ordering::Less)
        {
            self.heap.pop();
            self.heap.push(entry);
        }
    }

    fn into_sorted(self) -> Vec<Value> {
        self.into_sorted_seq()
            .into_iter()
            .map(|(_, doc)| doc)
            .collect()
    }

    fn into_sorted_seq(self) -> Vec<(usize, Value)> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|e| (e.seq, e.doc))
            .collect()
    }
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_i64().unwrap_or(0) != 0,
        Value::Null => false,
        _ => true,
    }
}

/// Projekcja top-level: tryb include albo exclude (jak MongoDB).
fn project(doc: &Value, projection: &Value) -> Value {
    let Some(p) = projection.as_object() else {
        return doc.clone();
    };
    if p.is_empty() {
        return doc.clone();
    }

    let include_mode = p.iter().any(|(k, v)| k.as_str() != "_id" && truthy(v));

    if include_mode {
        let mut out = Map::new();
        // `_id` jest domyślnie włączone, chyba że projekcja jawnie daje `_id: 0`.
        let include_id = p.get("_id").map(truthy).unwrap_or(true);
        if include_id {
            if let Some(id) = doc.get("_id") {
                out.insert("_id".to_string(), id.clone());
            }
        }
        for (k, v) in p {
            if k == "_id" || !truthy(v) {
                continue;
            }
            if let Some(val) = doc.get(k) {
                out.insert(k.clone(), val.clone());
            }
        }
        Value::Object(out)
    } else {
        let mut out = doc.as_object().cloned().unwrap_or_default();
        for (k, v) in p {
            if !truthy(v) {
                out.remove(k);
            }
        }
        Value::Object(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;
    use serde_json::json;

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn insert_find_update_delete() {
        let (_dir, store) = store();
        let c = store.collection("users").unwrap();

        let id = c.insert_one(json!({"name": "Ala", "age": 30})).unwrap();
        c.insert_one(json!({"name": "Bob", "age": 17})).unwrap();
        assert!(c.insert_one(json!({"_id": id, "name": "dup"})).is_err());

        let q = FindQuery {
            filter: json!({"age": {"$gte": 18}}),
            ..Default::default()
        };
        let found = c.find(&q).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0]["name"], "Ala");

        let n = c
            .update(&json!({"name": "Ala"}), &json!({"$inc": {"age": 1}}), false)
            .unwrap();
        assert_eq!(n, 1);
        let ala = c.find_one(&json!({"name": "Ala"})).unwrap().unwrap();
        assert_eq!(ala["age"], 31);

        let d = c.delete(&json!({"age": {"$lt": 18}}), true).unwrap();
        assert_eq!(d, 1);
        assert_eq!(c.count(&json!({})).unwrap(), 1);
    }

    #[test]
    fn raw_prefilter_avoids_false_negatives() {
        let filter = json!({"name": "a\"b\n", "active": true});
        let raw = raw_prefilter(&filter).unwrap();
        let matching = json!({"_id": "1", "name": "a\"b\n", "active": true, "junk": "x"});
        let bytes = serde_json::to_vec(&matching).unwrap();
        assert!(raw.matches(&bytes));

        let wrong_bool = json!({"_id": "1", "name": "a\"b\n", "active": false});
        assert!(!raw.matches(&serde_json::to_vec(&wrong_bool).unwrap()));

        let wrong_string = json!({"_id": "1", "name": "other", "active": true});
        assert!(!raw.matches(&serde_json::to_vec(&wrong_string).unwrap()));

        // `$or` nie może być użyty jako prefiltr.
        assert!(raw_prefilter(&json!({"$or": [{"name": "a"}, {"name": "b"}]})).is_none());
        // Liczba sama w sobie wymaga parsera (np. 1 vs 1.0).
        assert!(raw_prefilter(&json!({"n": 1})).is_none());
    }

    #[test]
    fn raw_prefilter_preserves_string_and_number_semantics() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        c.insert_one(json!({"_id": "a", "name": "a\"b\n", "n": 1, "tags": ["x"]}))
            .unwrap();
        c.insert_one(json!({"_id": "b", "name": "other", "n": 2, "tags": ["y"]}))
            .unwrap();

        assert_eq!(
            c.find_one(&json!({"name": "a\"b\n"})).unwrap().unwrap()["_id"],
            "a"
        );
        assert_eq!(
            c.find_one(&json!({"name": {"$eq": "other"}}))
                .unwrap()
                .unwrap()["_id"],
            "b"
        );
        assert_eq!(
            c.find_one(&json!({"tags": "x"})).unwrap().unwrap()["_id"],
            "a"
        );
        // Prefiltr używa tylko stringa; liczba nadal jest porównywana przez matcher.
        assert_eq!(
            c.find_one(&json!({"name": "a\"b\n", "n": 1.0}))
                .unwrap()
                .unwrap()["_id"],
            "a"
        );
        assert!(c
            .find_one(&json!({"name": "a\"b\n", "n": 2}))
            .unwrap()
            .is_none());
    }

    #[test]
    fn parallel_sorted_scan_matches_serial() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..1000 {
            c.insert_one(json!({
                "_id": format!("id{i:04}"),
                "g": i % 7,
                "n": i,
                "junk": "x",
            }))
            .unwrap();
        }
        let q = FindQuery {
            filter: json!({"n": {"$gte": 100}}),
            projection: Some(json!({"g": 1, "n": 1, "_id": 0})),
            sort: vec![("g".to_string(), 1), ("n".to_string(), -1)],
            skip: 7,
            limit: Some(31),
        };
        // `find` bierze ścieżkę równoległą (1000 wpisów > próg testowy).
        let parallel = c.find(&q).unwrap();
        // `find_in` to ten sam skan, ale jednowątkowy.
        let rtxn = store.read_txn().unwrap();
        let serial = c.find_in(&rtxn, &q).unwrap();
        rtxn.commit().unwrap();
        assert_eq!(parallel, serial);
        assert_eq!(parallel.len(), 31);
        for pair in parallel.windows(2) {
            let a = (
                pair[0]["g"].as_i64().unwrap(),
                -pair[0]["n"].as_i64().unwrap(),
            );
            let b = (
                pair[1]["g"].as_i64().unwrap(),
                -pair[1]["n"].as_i64().unwrap(),
            );
            assert!(a <= b, "niestabilna kolejność: {a:?} > {b:?}");
        }
    }

    #[test]
    fn parallel_unsorted_scan_matches_serial() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..1000 {
            c.insert_one(json!({
                "_id": format!("id{i:04}"),
                "g": i % 7,
                "n": i,
                "junk": "x",
            }))
            .unwrap();
        }
        let q = FindQuery {
            filter: json!({"n": {"$gte": 100}}),
            projection: Some(json!({"g": 1, "n": 1, "_id": 0})),
            skip: 7,
            limit: Some(31),
            ..Default::default()
        };
        // `find` bierze równoległy skan bez sortowania.
        let parallel = c.find(&q).unwrap();
        let rtxn = store.read_txn().unwrap();
        let serial = c.find_in(&rtxn, &q).unwrap();
        rtxn.commit().unwrap();
        assert_eq!(parallel, serial);
        assert_eq!(parallel.len(), 31);
    }

    #[test]
    fn id_range_uses_primary_key_order() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..20 {
            c.insert_one(json!({"_id": format!("id{i:02}"), "n": i}))
                .unwrap();
        }

        // Zakres `_id` + dodatkowy warunek; naturalna kolejność `_id`.
        let q = FindQuery {
            filter: json!({"_id": {"$gte": "id05", "$lt": "id09"}, "n": {"$gte": 6}}),
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        let ids: Vec<_> = got.iter().map(|d| d["_id"].as_str().unwrap()).collect();
        assert_eq!(ids, ["id06", "id07", "id08"]);

        // Sortowanie po `_id` malejąco nadal korzysta z zakresu.
        let q = FindQuery {
            filter: json!({"_id": {"$gt": "id05", "$lte": "id09"}}),
            sort: vec![("_id".to_string(), -1)],
            limit: Some(2),
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        let ids: Vec<_> = got.iter().map(|d| d["_id"].as_str().unwrap()).collect();
        assert_eq!(ids, ["id09", "id08"]);

        // Dokładny `_id` z dodatkowym warunkiem nie wymaga pełnego skanu.
        assert!(c
            .find_one(&json!({"_id": "id07", "n": 7}))
            .unwrap()
            .is_some());
        assert!(c
            .find_one(&json!({"_id": "id07", "n": 8}))
            .unwrap()
            .is_none());

        assert_eq!(
            c.count(&json!({"_id": {"$gte": "id05", "$lt": "id09"}}))
                .unwrap(),
            4
        );
        assert_eq!(
            c.count(&json!({"_id": {"$gte": "id05", "$lt": "id09"}, "n": {"$gte": 7}}))
                .unwrap(),
            2
        );

        // Pusty lub odwrócony zakres nie może wejść w pełny skan.
        assert!(c
            .find(&FindQuery {
                filter: json!({"_id": {"$gt": "id09", "$lt": "id05"}}),
                limit: Some(10),
                ..Default::default()
            })
            .unwrap()
            .is_empty());
        assert_eq!(
            c.count(&json!({"_id": {"$gt": "id09", "$lt": "id05"}}))
                .unwrap(),
            0
        );
    }

    #[test]
    fn id_range_update_delete() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..20 {
            c.insert_one(json!({"_id": format!("id{i:02}"), "n": i}))
                .unwrap();
        }
        let n = c
            .update(
                &json!({"_id": {"$gte": "id05", "$lt": "id09"}}),
                &json!({"$set": {"flag": true}}),
                true,
            )
            .unwrap();
        assert_eq!(n, 4);
        assert_eq!(c.count(&json!({"flag": true})).unwrap(), 4);

        let n = c
            .delete(&json!({"_id": {"$gte": "id06", "$lte": "id08"}}), true)
            .unwrap();
        assert_eq!(n, 3);
        assert_eq!(c.count(&json!({})).unwrap(), 17);
    }

    #[test]
    fn sort_projection_pagination() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..5 {
            c.insert_one(json!({"i": i, "even": i % 2 == 0, "junk": "x"}))
                .unwrap();
        }
        let q = FindQuery {
            filter: json!({}),
            projection: Some(json!({"i": 1, "_id": 0})),
            sort: vec![("i".to_string(), -1)],
            skip: 1,
            limit: Some(2),
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got, vec![json!({"i": 3}), json!({"i": 2})]);
    }

    #[test]
    fn secondary_index_equality_and_range() {
        let (_dir, store) = store();
        let c = store.collection("users").unwrap();
        for i in 0..10 {
            c.insert_one(json!({"name": format!("u{i}"), "age": i}))
                .unwrap();
        }
        c.create_index("age", false).unwrap();
        assert_eq!(c.list_indexes().unwrap(), vec![("age".to_string(), false)]);

        let eq = c.find_one(&json!({"age": 5})).unwrap().unwrap();
        assert_eq!(eq["name"], "u5");

        let q = FindQuery {
            filter: json!({"age": {"$gte": 7}}),
            sort: vec![("age".to_string(), 1)],
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0]["age"], 7);
        assert_eq!(got[2]["age"], 9);
    }

    #[test]
    fn compound_index_filters_sorts_and_maintains() {
        let (_dir, store) = store();
        let c = store.collection("events").unwrap();
        for g in 0..3 {
            for age in 0..10 {
                c.insert_one(json!({
                    "_id": format!("g{g}a{age}"),
                    "g": g,
                    "age": age,
                    "name": format!("{g}-{age}"),
                }))
                .unwrap();
            }
        }
        c.create_index_fields(&["g".to_string(), "age".to_string()], false, &[])
            .unwrap();
        assert!(c
            .list_indexes()
            .unwrap()
            .contains(&("g+age".to_string(), false)));

        // Zakres na ostatnim polu + sortowanie po indeksie.
        let q = FindQuery {
            filter: json!({"g": 1, "age": {"$gte": 4, "$lt": 8}}),
            sort: vec![("age".to_string(), 1)],
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0]["age"], 4);
        assert_eq!(got[3]["age"], 7);

        // Równość wszystkich pól + count przez indeks.
        let q = FindQuery {
            filter: json!({"g": 2, "age": 3}),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 1);
        assert_eq!(
            c.count(&json!({"g": 1, "age": {"$gte": 4, "$lt": 8}}))
                .unwrap(),
            4
        );
        // Nadmiarowy warunek nie może użyć samego indeksu do count.
        assert_eq!(
            c.count(&json!({"g": 1, "age": {"$gte": 4, "$lt": 8}, "name": "1-4"}))
                .unwrap(),
            1
        );

        // Update przenosi wpis w indeksie.
        c.update(
            &json!({"_id": "g1a5"}),
            &json!({"$set": {"age": 99}}),
            false,
        )
        .unwrap();
        assert_eq!(
            c.count(&json!({"g": 1, "age": {"$gte": 4, "$lt": 8}}))
                .unwrap(),
            3
        );
        assert!(c.find_one(&json!({"g": 1, "age": 99})).unwrap().is_some());

        // Delete czyści wpis.
        c.delete(&json!({"_id": "g2a3"}), false).unwrap();
        assert!(c.find_one(&json!({"g": 2, "age": 3})).unwrap().is_none());
    }

    #[test]
    fn covering_index_serves_projection_without_document_fetch() {
        let (_dir, store) = store();
        let c = store.collection("cover").unwrap();
        c.create_index_fields(&["g".to_string()], false, &["v".to_string()])
            .unwrap();
        c.insert_one(json!({"_id": "a", "g": 1, "v": 42, "other": "x"}))
            .unwrap();
        c.update(&json!({"_id": "a"}), &json!({"$set": {"v": 43}}), false)
            .unwrap();
        // Projekcja wykluczająca nie może być obsłużona tylko z indeksu.
        let excl = FindQuery {
            filter: json!({"g": 1}),
            projection: Some(json!({"other": 0})),
            sort: vec![("g".to_string(), 1)],
            ..Default::default()
        };
        let got = c.find(&excl).unwrap();
        assert_eq!(got[0]["v"], 43);
        assert!(got[0].get("other").is_none());
        // Usuwamy dokument, zostawiając wpis indeksu — pokrycie nie może
        // sięgać do kolekcji, więc wynik dowodzi użycia wartości indeksu.
        let mut wtxn = c.write_txn().unwrap();
        c.db.delete(&mut wtxn, b"a").unwrap();
        wtxn.commit().unwrap();

        let q = FindQuery {
            filter: json!({"g": 1}),
            projection: Some(json!({"g": 1, "v": 1, "_id": 0})),
            sort: vec![("g".to_string(), 1)],
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got, vec![json!({"g": 1, "v": 43})]);
    }

    #[test]
    fn update_upsert_and_array_operators() {
        let (_dir, store) = store();
        let c = store.collection("cfg").unwrap();
        let out = c
            .update_opts(
                &json!({"tenant": "a", "key": "mail"}),
                &json!({
                    "$set": {"host": "smtp"},
                    "$setOnInsert": {"created": true},
                    "$addToSet": {"tags": {"$each": ["x", "x", "y"]}},
                    "$push": {"ports": {"$each": [25, 587]}},
                }),
                false,
                true,
            )
            .unwrap();
        assert_eq!(out.matched, 0);
        assert_eq!(out.modified, 0);
        let id = out.upserted_id.expect("upserted id");
        let doc = c.find_one(&json!({"_id": id})).unwrap().unwrap();
        assert_eq!(doc["tenant"], "a");
        assert_eq!(doc["key"], "mail");
        assert_eq!(doc["host"], "smtp");
        assert_eq!(doc["created"], true);
        assert_eq!(doc["tags"], json!(["x", "y"]));
        assert_eq!(doc["ports"], json!([25, 587]));

        // Drugi upsert trafia istniejący dokument: brak nowego `_id`,
        // `$setOnInsert` nie działa.
        let out = c
            .update_opts(
                &json!({"tenant": "a", "key": "mail"}),
                &json!({
                    "$set": {"host": "smtp2"},
                    "$setOnInsert": {"created": false},
                    "$addToSet": {"tags": "z"},
                }),
                false,
                true,
            )
            .unwrap();
        assert_eq!(out.matched, 1);
        assert_eq!(out.modified, 1);
        assert!(out.upserted_id.is_none());
        let doc = c.find_one(&json!({"_id": id})).unwrap().unwrap();
        assert_eq!(doc["host"], "smtp2");
        assert_eq!(doc["created"], true);
        assert_eq!(doc["tags"], json!(["x", "y", "z"]));

        // Replacement upsert dokłada equalities z filtra.
        let out = c
            .update_opts(
                &json!({"tenant": "b", "key": "db"}),
                &json!({"value": 1}),
                false,
                true,
            )
            .unwrap();
        let doc = c
            .find_one(&json!({"_id": out.upserted_id.unwrap()}))
            .unwrap()
            .unwrap();
        assert_eq!(doc["tenant"], "b");
        assert_eq!(doc["key"], "db");
        assert_eq!(doc["value"], 1);
    }

    #[test]
    fn upsert_without_match_and_no_upsert_is_noop() {
        let (_dir, store) = store();
        let c = store.collection("cfg2").unwrap();
        let out = c
            .update_opts(&json!({"a": 1}), &json!({"$set": {"b": 2}}), false, false)
            .unwrap();
        assert_eq!(
            out,
            UpdateOutcome {
                matched: 0,
                modified: 0,
                upserted_id: None
            }
        );
        assert_eq!(c.count(&json!({})).unwrap(), 0);
    }

    #[test]
    fn compound_unique_index_checks_full_key() {
        let (_dir, store) = store();
        let c = store.collection("u").unwrap();
        c.create_index_fields(&["a".to_string(), "b".to_string()], true, &[])
            .unwrap();
        c.insert_one(json!({"a": 1, "b": 1})).unwrap();
        c.insert_one(json!({"a": 1, "b": 2})).unwrap();
        let err = c.insert_one(json!({"a": 1, "b": 1})).unwrap_err();
        assert!(matches!(err, Error::DuplicateKey(_)));
    }

    #[test]
    fn index_maintained_on_update_and_delete() {
        let (_dir, store) = store();
        let c = store.collection("users").unwrap();
        let id = c.insert_one(json!({"name": "Ala", "age": 30})).unwrap();
        c.create_index("age", true).unwrap();

        // Zmiana indeksowanego pola aktualizuje wpis.
        c.update(&json!({"_id": id}), &json!({"$set": {"age": 31}}), false)
            .unwrap();
        assert!(c.find_one(&json!({"age": 30})).unwrap().is_none());
        assert_eq!(
            c.find_one(&json!({"age": 31})).unwrap().unwrap()["name"],
            "Ala"
        );

        // Unikalność.
        c.insert_one(json!({"name": "Bob", "age": 40})).unwrap();
        assert!(c.insert_one(json!({"name": "Cezary", "age": 40})).is_err());

        // Delete usuwa wpis z indeksu.
        c.delete(&json!({"age": 40}), true).unwrap();
        assert!(c.find_one(&json!({"age": 40})).unwrap().is_none());
    }

    #[test]
    fn rejects_oversized_document() {
        let (_dir, store) = store();
        let c = store.collection("big").unwrap();
        let big = "x".repeat(crate::MAX_DOCUMENT_BYTES + 10);
        let err = c.insert_one(json!({ "payload": big })).unwrap_err();
        assert!(matches!(err, Error::DocumentTooLarge(_, _)));
    }

    #[test]
    fn unique_index_backfill_detects_duplicates() {
        let (_dir, store) = store();
        let c = store.collection("users").unwrap();
        c.insert_one(json!({"name": "Ala"})).unwrap();
        c.insert_one(json!({"name": "Ala"})).unwrap();
        assert!(c.create_index("name", true).is_err());
    }

    #[test]
    fn text_search_ranks_and_stays_in_sync() {
        let (_dir, store) = store();
        let c = store.collection("docs").unwrap();
        c.insert_one(json!({"title": "rust database engine", "tag": "db"}))
            .unwrap();
        c.insert_one(json!({"title": "rust rust rust async runtime", "tag": "db"}))
            .unwrap();
        c.insert_one(json!({"title": "python web framework", "tag": "web"}))
            .unwrap();
        c.create_text_index("title").unwrap();
        assert_eq!(c.list_text_indexes().unwrap(), vec!["title".to_string()]);

        let hits = c.search("title", "rust", &json!({}), 0, Some(10)).unwrap();
        assert_eq!(hits.len(), 2);
        // Dokument z większą częstością `rust` ma wyższy score.
        assert!(hits[0]["title"].as_str().unwrap().contains("rust rust"));
        assert!(hits[0]["_score"].as_f64().unwrap() > hits[1]["_score"].as_f64().unwrap());

        // Filtr residualny działa.
        let only_db = c
            .search("title", "rust", &json!({"tag": "db"}), 0, Some(10))
            .unwrap();
        assert_eq!(only_db.len(), 2);

        // Update i delete utrzymują indeks.
        c.update(
            &json!({"title": "python web framework"}),
            &json!({"$set": {"title": "rust web framework"}}),
            false,
        )
        .unwrap();
        let hits = c.search("title", "rust", &json!({}), 0, Some(10)).unwrap();
        assert_eq!(hits.len(), 3);

        c.delete(&json!({"title": "rust database engine"}), false)
            .unwrap();
        let hits = c
            .search("title", "database", &json!({}), 0, Some(10))
            .unwrap();
        assert!(hits.is_empty());

        c.drop_text_index("title").unwrap();
        assert!(c.search("title", "rust", &json!({}), 0, None).is_err());
    }

    #[test]
    fn fuzzy_search_finds_typos_and_orders_by_distance() {
        let (_dir, store) = store();
        let c = store.collection("people").unwrap();
        c.insert_one(json!({"name": "Aleksandra"})).unwrap();
        c.insert_one(json!({"name": "Aleksander"})).unwrap();
        c.insert_one(json!({"name": "Bartosz"})).unwrap();
        c.create_fuzzy_index("name").unwrap();

        let hits = c
            .fuzzy_search("name", "aleksander", 2, &json!({}), Some(10))
            .unwrap();
        assert!(hits.len() >= 2);
        assert_eq!(hits[0]["name"], "Aleksander");
        assert_eq!(hits[0]["_distance"], 0);

        let hits = c
            .fuzzy_search("name", "aleksndra", 2, &json!({}), Some(10))
            .unwrap();
        assert!(hits.iter().any(|d| d["name"] == "Aleksandra"));

        let hits = c
            .fuzzy_search("name", "bartek", 1, &json!({}), Some(10))
            .unwrap();
        assert!(hits.is_empty());

        c.update(
            &json!({"name": "Bartosz"}),
            &json!({"$set": {"name": "Bartlomiej"}}),
            false,
        )
        .unwrap();
        let hits = c
            .fuzzy_search("name", "bartlomiej", 2, &json!({}), Some(10))
            .unwrap();
        assert_eq!(hits.len(), 1);

        c.delete(&json!({"name": "Aleksander"}), false).unwrap();
        let hits = c
            .fuzzy_search("name", "aleksander", 1, &json!({}), Some(10))
            .unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn count_fast_paths_are_correct() {
        let (_dir, store) = store();
        let c = store.collection("nums").unwrap();
        c.insert_one(json!({"v": 1})).unwrap();
        c.insert_one(json!({"v": 2})).unwrap();
        c.insert_one(json!({})).unwrap(); // brak pola
        c.insert_one(json!({"v": null})).unwrap();
        c.create_index("v", false).unwrap();

        assert_eq!(c.count(&json!({})).unwrap(), 4);
        assert_eq!(c.count(&Value::Null).unwrap(), 4);
        assert_eq!(c.count(&json!({"v": 1})).unwrap(), 1);
        assert_eq!(c.count(&json!({"v": {"$gte": 2}})).unwrap(), 1);
        // `null` pasuje też do braku pola -> nie wolno użyć indeksu.
        assert_eq!(c.count(&json!({"v": null})).unwrap(), 2);
        assert_eq!(c.count(&json!({"v": {"$eq": null}})).unwrap(), 2);

        let id = c.find_one(&json!({"v": 1})).unwrap().unwrap()["_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(c.count(&json!({"_id": id})).unwrap(), 1);
        assert_eq!(c.count(&json!({"_id": "missing"})).unwrap(), 0);
    }

    #[test]
    fn find_limit_early_exit_and_sorted_queries() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..50 {
            c.insert_one(json!({"n": i, "grp": i % 5})).unwrap();
        }
        c.create_index("grp", false).unwrap();

        let q = FindQuery {
            filter: json!({"grp": 1}),
            limit: Some(3),
            skip: 2,
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 3);

        let q = FindQuery {
            filter: json!({}),
            sort: vec![("n".into(), 1)],
            limit: Some(3),
            ..Default::default()
        };
        let ns: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64())
            .collect();
        assert_eq!(ns, vec![Some(0), Some(1), Some(2)]);

        let q = FindQuery {
            filter: json!({}),
            sort: vec![("n".into(), -1)],
            skip: 10,
            limit: Some(3),
            ..Default::default()
        };
        let ns: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64())
            .collect();
        assert_eq!(ns, vec![Some(39), Some(38), Some(37)]);

        // `find_one` po indeksowanym polu zwraca dokładnie jeden dokument.
        let one = c.find_one(&json!({"grp": 1})).unwrap().unwrap();
        assert_eq!(one["grp"], 1);
    }

    #[test]
    fn skip_and_sorted_topk_match_full_sort() {
        let (_dir, store) = store();
        let c = store.collection("skip").unwrap();
        for i in 0..100 {
            c.insert_one(json!({"n": i, "grp": i % 3})).unwrap();
        }

        // `skip` bez filtra: naturalna kolejność `_id`, bez zbierania pominiętych.
        let q = FindQuery {
            filter: json!({}),
            skip: 90,
            limit: Some(5),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 5);

        // `skip` z filtrem nie-match-all: pomijamy dopiero trafienia.
        let q = FindQuery {
            filter: json!({"grp": 0}),
            skip: 20,
            limit: Some(5),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 5);

        // TopK z sortowaniem, skip i wieloma równymi kluczami.
        let all = c
            .find(&FindQuery {
                filter: json!({}),
                ..Default::default()
            })
            .unwrap();
        let mut expected_docs = all.clone();
        expected_docs.sort_by_key(|d| d["grp"].as_i64().unwrap());
        let expected: Vec<_> = expected_docs
            .iter()
            .skip(50)
            .take(7)
            .map(|d| d["_id"].clone())
            .collect();

        let q = FindQuery {
            filter: json!({}),
            sort: vec![("grp".into(), 1)],
            skip: 50,
            limit: Some(7),
            ..Default::default()
        };
        let got: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["_id"].clone())
            .collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn null_filter_with_index_falls_back_to_scan() {
        let (_dir, store) = store();
        let c = store.collection("nulls").unwrap();
        c.insert_one(json!({"a": null, "tag": "present"})).unwrap();
        c.insert_one(json!({"b": 1, "tag": "missing"})).unwrap();
        c.create_index("a", false).unwrap();

        // Brak pola też pasuje do `null`; indeks nie zawiera braków.
        assert_eq!(c.count(&json!({"a": null})).unwrap(), 2);
        let q = FindQuery {
            filter: json!({"a": null}),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 2);
        assert_eq!(c.count(&json!({"a": {"$eq": null}})).unwrap(), 2);
    }

    #[test]
    fn range_index_respects_types() {
        let (_dir, store) = store();
        let c = store.collection("typed").unwrap();
        for v in [
            json!(1),
            json!(3),
            json!(10),
            json!("a"),
            json!("z"),
            json!(true),
        ] {
            c.insert_one(json!({"n": v})).unwrap();
        }
        c.create_index("n", false).unwrap();

        // Matcher nie porównuje różnych typów: tylko liczby > 2.
        assert_eq!(c.count(&json!({"n": {"$gt": 2}})).unwrap(), 2);
        assert_eq!(c.count(&json!({"n": {"$lt": 2}})).unwrap(), 1);
        let mut ns: Vec<_> = c
            .find(&FindQuery {
                filter: json!({"n": {"$gt": 2}}),
                limit: Some(10),
                ..Default::default()
            })
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64().unwrap())
            .collect();
        ns.sort();
        assert_eq!(ns, vec![3, 10]);

        // Tylko stringi < "m".
        assert_eq!(c.count(&json!({"n": {"$lt": "m"}})).unwrap(), 1);
    }

    #[test]
    fn combined_range_index_and_count() {
        let (_dir, store) = store();
        let c = store.collection("ranges").unwrap();
        for i in 0..100 {
            c.insert_one(json!({"n": i, "grp": i % 10})).unwrap();
        }
        c.create_index("n", false).unwrap();

        let q = FindQuery {
            filter: json!({"n": {"$gte": 30, "$lt": 35}}),
            limit: Some(100),
            ..Default::default()
        };
        let mut ns: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64().unwrap())
            .collect();
        ns.sort();
        assert_eq!(ns, vec![30, 31, 32, 33, 34]);
        assert_eq!(c.count(&json!({"n": {"$gte": 30, "$lt": 35}})).unwrap(), 5);
        assert_eq!(c.count(&json!({"n": {"$gt": 30, "$lte": 35}})).unwrap(), 5);

        // Zakres + sortowanie + skip/limit.
        let q = FindQuery {
            filter: json!({"n": {"$gte": 30, "$lt": 35}}),
            sort: vec![("n".into(), -1)],
            skip: 1,
            limit: Some(2),
            ..Default::default()
        };
        let ns: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64().unwrap())
            .collect();
        assert_eq!(ns, vec![33, 32]);
    }

    #[test]
    fn update_delete_use_index_and_residual_filter() {
        let (_dir, store) = store();
        let c = store.collection("groups").unwrap();
        c.insert_one(json!({"g": "a", "n": 1})).unwrap();
        c.insert_one(json!({"g": "a", "n": 2})).unwrap();
        c.insert_one(json!({"g": "b", "n": 3})).unwrap();
        c.create_index("g", false).unwrap();

        let updated = c
            .update(&json!({"g": "a"}), &json!({"$set": {"n": 0}}), true)
            .unwrap();
        assert_eq!(updated, 2);
        assert_eq!(c.count(&json!({"g": "a", "n": 0})).unwrap(), 2);

        let deleted = c.delete(&json!({"g": "b"}), false).unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(c.count(&json!({})).unwrap(), 2);

        // Ścieżka dokładnego `_id` w update/delete.
        let one = c.find_one(&json!({"g": "a"})).unwrap().unwrap();
        let id = one["_id"].as_str().unwrap().to_string();
        assert_eq!(
            c.update(&json!({"_id": id}), &json!({"$inc": {"n": 5}}), false)
                .unwrap(),
            1
        );
        assert_eq!(c.delete(&json!({"_id": id}), false).unwrap(), 1);
        assert_eq!(c.count(&json!({})).unwrap(), 1);
    }
}

#[cfg(test)]
mod parse_json_tests {
    use super::*;

    #[test]
    fn sonic_matches_serde_for_edge_values() {
        let samples = [
            r#"{"a":1,"b":-2.5,"c":true,"d":null,"e":"x\\ny","u":"zażółć"}"#,
            r#"{"n":18446744073709551615,"i":-9223372036854775808}"#,
            r#"[1,2,3,{"x":"\\u00e9"}]"#,
            r#"0.1"#,
            r#""tekst""#,
        ];
        for raw in samples {
            let fast = parse_json(raw.as_bytes()).unwrap();
            let slow: Value = serde_json::from_slice(raw.as_bytes()).unwrap();
            assert_eq!(fast, slow, "raw={raw}");
        }
    }
}
