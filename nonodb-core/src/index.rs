//! Indeksy wtórne: kodowanie klucza i definicje.

use std::ops::Bound;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Definicja indeksu zapisywana w `meta` pod `index:<coll>:<nazwa>`.
///
/// Obsługuje indeksy pojedyncze (`fields == ["age"]`) i złożone
/// (`fields == ["grp", "age"]`). `field` jest tylko zgodnością wsteczną dla
/// baz zapisanych przed wprowadzeniem indeksów złożonych.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub field: String,
    #[serde(default)]
    pub unique: bool,
    /// Pola dodatkowo kopiowane do wartości indeksu (pokrywanie projekcji).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,
}

impl IndexDef {
    pub fn single(field: &str, unique: bool) -> Self {
        Self {
            fields: vec![field.to_string()],
            field: String::new(),
            unique,
            include: Vec::new(),
        }
    }

    pub fn compound(fields: &[String], unique: bool, include: &[String]) -> Self {
        Self {
            fields: fields.to_vec(),
            field: String::new(),
            unique,
            include: include.to_vec(),
        }
    }

    /// Lista pól w kolejności indeksu (z obsługą starego formatu `field`).
    pub fn effective_fields(&self) -> Vec<String> {
        if !self.fields.is_empty() {
            self.fields.clone()
        } else if !self.field.is_empty() {
            vec![self.field.clone()]
        } else {
            Vec::new()
        }
    }

    /// Nazwa logiczna: `age` albo `grp+age`.
    pub fn name(&self) -> String {
        self.effective_fields().join("+")
    }
}

/// Nazwa sub-bazy indeksu.
pub fn db_name(coll: &str, field: &str) -> String {
    format!("idx:{coll}:{field}")
}

/// Klucz definicji indeksu w `meta`.
pub fn def_key(coll: &str, field: &str) -> String {
    format!("index:{coll}:{field}")
}

/// Wartość pola dokumentu (obsługuje ścieżki `a.b`).
pub fn field_value<'a>(doc: &'a Value, field: &str) -> Option<&'a Value> {
    crate::query::path::get(doc, field)
}

/// Koduje wartość skalarną do bajtów, których porządek leksykograficzny
/// odpowiada porządkowi wartości. Zwraca `None` dla typów nieindeksowalnych.
///
/// Układ: `[tag] [dane]`.
pub fn encode_value(v: &Value) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(10);
    match v {
        Value::Null => out.push(0x01),
        Value::Number(n) => {
            out.push(0x02);
            out.extend_from_slice(&orderable_f64(n.as_f64()?));
        }
        Value::String(s) => {
            out.push(0x03);
            // Escape NUL: 0x00 -> 0x00 0x01; terminator: 0x00 0x00.
            for &b in s.as_bytes() {
                if b == 0 {
                    out.push(0x00);
                    out.push(0x01);
                } else {
                    out.push(b);
                }
            }
            out.push(0x00);
            out.push(0x00);
        }
        Value::Bool(b) => {
            out.push(0x04);
            out.push(u8::from(*b));
        }
        _ => return None,
    }
    Some(out)
}

/// Koduje wartości wszystkich pól indeksu (indeks złożony).
/// Dokument jest pomijany, jeśli brakuje któregokolwiek pola lub jest ono
/// tablicą/obiektem.
pub fn encode_fields(doc: &Value, fields: &[String]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(fields.len() * 10);
    for field in fields {
        out.extend_from_slice(&encode_value(field_value(doc, field)?)?);
    }
    Some(out)
}

/// Wartość wpisu indeksu. Gdy indeks ma pola `include`, wartość zawiera
/// `_id` oraz kopię pól indeksu i `include` (pokrywanie projekcji). W starym
/// formacie wartością jest samo `_id`.
pub fn encode_index_value(def: &IndexDef, doc: &Value, id: &str) -> Vec<u8> {
    if def.include.is_empty() {
        return id.as_bytes().to_vec();
    }
    let mut inc = serde_json::Map::new();
    for field in def.effective_fields().iter().chain(def.include.iter()) {
        if let Some(v) = field_value(doc, field) {
            inc.insert(field.clone(), v.clone());
        }
    }
    let payload = serde_json::json!({ "id": id, "inc": inc });
    match serde_json::to_vec(&payload) {
        Ok(mut bytes) => {
            bytes.insert(0, 0);
            bytes
        }
        Err(_) => id.as_bytes().to_vec(),
    }
}

/// Odczyt wartości wpisu indeksu: `(id, opcjonalne pola pokrywające)`.
pub fn decode_index_value(raw: &[u8]) -> Option<(String, Option<serde_json::Map<String, Value>>)> {
    if raw.first() == Some(&0) {
        if let Ok(Value::Object(mut obj)) = serde_json::from_slice::<Value>(&raw[1..]) {
            let id = match obj.remove("id") {
                Some(Value::String(s)) => s,
                _ => return None,
            };
            let inc = match obj.remove("inc") {
                Some(Value::Object(m)) => Some(m),
                _ => None,
            };
            return Some((id, inc));
        }
    }
    std::str::from_utf8(raw)
        .ok()
        .map(|id| (id.to_string(), None))
}

fn orderable_f64(f: f64) -> [u8; 8] {
    // Normalizacja -0.0 -> 0.0: `f64` traktuje je jako równe, więc kodowanie
    // też musi (inaczej porządek kluczy różniłby się od porównań).
    let f = if f == 0.0 { 0.0 } else { f };
    let bits = f.to_bits();
    // Porządek: liczby ujemne mają odwrócone bity, dodatnie mają ustawiony
    // najwyższy bit, więc big-endian porównuje się jak `f64`.
    let enc = if bits & (1 << 63) != 0 {
        !bits
    } else {
        bits ^ (1 << 63)
    };
    enc.to_be_bytes()
}

fn after_value(vb: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(vb.len() + 1);
    out.extend_from_slice(vb);
    out.push(0xFF);
    out
}

/// Zakres kluczy indeksu dla operatora porównania.
///
/// Klucz wpisu to `encode_value(wartość) ++ _id`, więc `vb + 0xFF` leży po
/// wszystkich wpisach o wartości `vb`, a przed każdą większą wartością.
pub type RangeBytes = (Bound<Vec<u8>>, Bound<Vec<u8>>);

pub fn bounds(op: &str, value: &Value) -> Option<RangeBytes> {
    let vb = encode_value(value)?;
    Some(match op {
        "$eq" => (
            Bound::Included(vb.clone()),
            Bound::Excluded(after_value(&vb)),
        ),
        "$gte" => (Bound::Included(vb), Bound::Unbounded),
        "$gt" => (Bound::Included(after_value(&vb)), Bound::Unbounded),
        "$lte" => (Bound::Unbounded, Bound::Excluded(after_value(&vb))),
        "$lt" => (Bound::Unbounded, Bound::Excluded(vb)),
        _ => return None,
    })
}

/// Zakres dla warunku złożonego, np. `{"$gte": 18, "$lt": 30}`.
///
/// Obsługuje jeden operator (`$eq`, `$gt`, `$gte`, `$lt`, `$lte`) albo kilka
/// operatorów zakresu na tym samym polu. Zakres jest ograniczony do tego samego
/// typu co wartość w warunku — matcher nie porównuje różnych typów, więc bez
/// tego `$gt: 2` obejmowałoby też stringi/bool z indeksu.
pub fn bounds_from_cond(cond: &Value) -> Option<RangeBytes> {
    let Some(obj) = cond.as_object() else {
        return bounds("$eq", cond);
    };
    if obj.is_empty() {
        return None;
    }
    if obj.len() == 1 {
        let (op, value) = obj.iter().next()?;
        if op == "$eq" {
            return bounds(op, value);
        }
        return range_bounds(std::iter::once((op.as_str(), value)));
    }
    range_bounds(obj.iter().map(|(op, value)| (op.as_str(), value)))
}

fn range_bounds<'a>(ops: impl Iterator<Item = (&'a str, &'a Value)>) -> Option<RangeBytes> {
    let mut tag: Option<u8> = None;
    let mut lower = Bound::Unbounded;
    let mut upper = Bound::Unbounded;
    for (op, value) in ops {
        let vb = encode_value(value)?;
        let t = *vb.first()?;
        if let Some(prev) = tag {
            if prev != t {
                return None; // mieszane typy -> pełny skan
            }
        } else {
            tag = Some(t);
        }
        match op {
            "$gt" => lower = max_lower(lower, Bound::Included(after_value(&vb))),
            "$gte" => lower = max_lower(lower, Bound::Included(vb)),
            "$lt" => upper = min_upper(upper, Bound::Excluded(vb)),
            "$lte" => upper = min_upper(upper, Bound::Excluded(after_value(&vb))),
            _ => return None,
        }
    }
    let tag = tag?;
    lower = max_lower(lower, Bound::Included(vec![tag]));
    upper = min_upper(upper, type_upper(tag));
    Some((lower, upper))
}

/// Górna granica typu (wyłącznie): klucze o tagu `tag` są mniejsze.
fn type_upper(tag: u8) -> Bound<Vec<u8>> {
    if tag < u8::MAX {
        Bound::Excluded(vec![tag + 1])
    } else {
        Bound::Unbounded
    }
}

fn max_lower(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (&a, &b) {
        (Bound::Unbounded, _) => b,
        (_, Bound::Unbounded) => a,
        _ => match bound_key(&a).cmp(bound_key(&b)) {
            std::cmp::Ordering::Greater => a,
            std::cmp::Ordering::Less => b,
            // Ten sam klucz: `Included` jest szerszy (mniej restrykcyjny).
            std::cmp::Ordering::Equal => {
                if matches!(a, Bound::Included(_)) {
                    a
                } else {
                    b
                }
            }
        },
    }
}

fn min_upper(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (&a, &b) {
        (Bound::Unbounded, _) => b,
        (_, Bound::Unbounded) => a,
        _ => match bound_key(&a).cmp(bound_key(&b)) {
            std::cmp::Ordering::Less => a,
            std::cmp::Ordering::Greater => b,
            // Ten sam klucz: `Included` jest szerszy.
            std::cmp::Ordering::Equal => {
                if matches!(a, Bound::Included(_)) {
                    a
                } else {
                    b
                }
            }
        },
    }
}

fn bound_key(b: &Bound<Vec<u8>>) -> &[u8] {
    match b {
        Bound::Included(v) | Bound::Excluded(v) => v,
        Bound::Unbounded => &[],
    }
}

/// Dokleja prefiks (wartości wcześniejszych pól indeksu) do granic zakresu.
pub fn prefix_bounds(prefix: &[u8], (lo, hi): RangeBytes) -> RangeBytes {
    let map = |b: Bound<Vec<u8>>| match b {
        Bound::Included(mut v) => {
            let mut k = prefix.to_vec();
            k.append(&mut v);
            Bound::Included(k)
        }
        Bound::Excluded(mut v) => {
            let mut k = prefix.to_vec();
            k.append(&mut v);
            Bound::Excluded(k)
        }
        Bound::Unbounded => Bound::Included(prefix.to_vec()),
    };
    (map(lo), map(hi))
}

/// Zakres dla pełnego prefiksu równościowego (wszystkie składowe `=`).
pub fn exact_prefix(prefix: &[u8]) -> RangeBytes {
    (
        Bound::Included(prefix.to_vec()),
        Bound::Excluded(after_value(prefix)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn enc(v: Value) -> Vec<u8> {
        encode_value(&v).unwrap()
    }

    #[test]
    fn numbers_keep_order() {
        let vals = [-1.5, -1.0, 0.0, 1.0, 2.0, 1e9];
        let mut encs: Vec<Vec<u8>> = vals.iter().map(|v| enc(json!(v))).collect();
        let sorted = {
            let mut c = encs.clone();
            c.sort();
            c
        };
        assert_eq!(encs, sorted);
        encs.reverse();
        assert_ne!(encs, sorted);
    }

    #[test]
    fn strings_keep_order() {
        let mut a = enc(json!("abc"));
        let mut b = enc(json!("abd"));
        assert!(a < b);
        b = enc(json!("ab"));
        assert!(b < a);
        a = enc(json!("ab\u{0}"));
        assert!(a > enc(json!("ab")));
    }

    #[test]
    fn range_for_eq_contains_only_equal() {
        let vb = enc(json!(5));
        let (lo, hi) = bounds("$eq", &json!(5)).unwrap();
        let key_eq = {
            let mut k = vb.clone();
            k.extend_from_slice(b"id");
            k
        };
        let key_gt = {
            let mut k = enc(json!(6));
            k.extend_from_slice(b"id");
            k
        };
        assert!(matches!(lo, Bound::Included(ref b) if *b <= key_eq));
        assert!(matches!(hi, Bound::Excluded(ref b) if key_eq < *b && key_gt >= *b));
    }

    #[test]
    fn compound_encoding_keeps_field_order() {
        let a = encode_fields(
            &json!({"g": 1, "age": 30}),
            &["g".to_string(), "age".to_string()],
        )
        .unwrap();
        let b = encode_fields(
            &json!({"g": 1, "age": 31}),
            &["g".to_string(), "age".to_string()],
        )
        .unwrap();
        assert!(a < b);
        assert!(a.starts_with(&enc(json!(1))));
    }
}
