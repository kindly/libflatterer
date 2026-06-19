//! A leaner, mutable replacement for `serde_json::Value` for the flatten hot path.
//!
//! Profiling showed the parse cost of the flatten core is ~70% *tree allocation*, not
//! scanning: `serde_json::Value` builds an `IndexMap` per object (hash table + entry vec)
//! and a heap `String` per key and string value. `FastValue` keeps serde_json's
//! battle-tested lexer (it deserializes via `serde_json::from_str/from_slice`, inheriting
//! string unescaping, number classification and line/column error messages exactly) but
//! builds a cheaper tree:
//!
//! - objects are a plain `Vec<(SmartString, FastValue)>` — one allocation, no hashing;
//!   JSON objects in this workload are small, so linear lookup is competitive;
//! - keys and string values are `SmartString` — anything ≤ 23 bytes is inline, so most
//!   of the per-key/value heap allocations disappear and clones are memcpys.
//!
//! The API mirrors the subset of `serde_json::Value`/`Map` the flatten core uses, with
//! the same semantics, pinned by tests against serde_json itself:
//!
//! - [`FastMap::remove`] is a **swap-remove** (matching `serde_json::Map::remove` under
//!   `preserve_order`, which the current output column order depends on);
//! - [`FastMap::insert`] on an existing key replaces the value but keeps its position
//!   (so duplicate keys in input behave identically: last value, first position);
//! - numbers partition into `UInt`(≥0) / `Int`(<0) / `Float` exactly as serde_json's
//!   `PosInt`/`NegInt`/`Float`, including integers overflowing u64/i64 becoming floats;
//! - `Display` serializes to compact JSON via serde_json's serializer (same escaping,
//!   itoa/ryu number formatting).

use std::fmt;

use rustc_hash::FxHashMap;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Deserializer, Serialize};
use smartstring::alias::String as SmartString;

#[derive(Clone, Debug, PartialEq, Default)]
pub enum FastValue {
    #[default]
    Null,
    Bool(bool),
    /// A negative integer (non-negative ones normalize to `UInt`, like serde_json).
    Int(i64),
    UInt(u64),
    Float(f64),
    String(SmartString),
    Array(Vec<FastValue>),
    Object(FastMap),
}

impl FastValue {
    /// Take the value, leaving `Null` behind (mirrors `serde_json::Value::take`).
    pub fn take(&mut self) -> FastValue {
        std::mem::take(self)
    }

    pub fn is_null(&self) -> bool {
        matches!(self, FastValue::Null)
    }

    pub fn is_object(&self) -> bool {
        matches!(self, FastValue::Object(_))
    }

    pub fn is_array(&self) -> bool {
        matches!(self, FastValue::Array(_))
    }

    pub fn is_string(&self) -> bool {
        matches!(self, FastValue::String(_))
    }

    /// Mirrors `serde_json::Number::is_f64`: true only for the float representation,
    /// not for integral values.
    pub fn is_f64(&self) -> bool {
        matches!(self, FastValue::Float(_))
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            FastValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Vec<FastValue>> {
        match self {
            FastValue::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_array_mut(&mut self) -> Option<&mut Vec<FastValue>> {
        match self {
            FastValue::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&FastMap> {
        match self {
            FastValue::Object(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_object_mut(&mut self) -> Option<&mut FastMap> {
        match self {
            FastValue::Object(m) => Some(m),
            _ => None,
        }
    }

    /// Mirrors `serde_json::Number::as_f64` (always succeeds for any number variant).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FastValue::Int(i) => Some(*i as f64),
            FastValue::UInt(u) => Some(*u as f64),
            FastValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Mirrors `serde_json::Number::as_i64`: floats are never integers, and a `UInt`
    /// above `i64::MAX` is out of range.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            FastValue::Int(i) => Some(*i),
            FastValue::UInt(u) => i64::try_from(*u).ok(),
            _ => None,
        }
    }
}

/// An insertion-ordered map backed by a `Vec`, mirroring the `serde_json::Map`
/// (`preserve_order`) semantics the flatten core relies on.
///
/// `entries` is always the source of truth for order and content. For objects with
/// many keys, the per-operation linear scan (and the O(n²) build it implies) stops
/// being competitive, so once `entries` crosses [`FastMap::INDEX_THRESHOLD`] a side
/// `key -> position` map is built and kept in sync; lookups then route through it in
/// O(1). Small objects — the common case — never allocate the index and stay a plain
/// contiguous `Vec` (no hashing, cache-friendly). The index is a pure accelerator:
/// `PartialEq` ignores it, and keys must never be mutated in place through `iter_mut`
/// (values may be) or the index would go stale.
#[derive(Clone, Debug, Default)]
pub struct FastMap {
    entries: Vec<(SmartString, FastValue)>,
    /// `Some` once `entries.len() >= INDEX_THRESHOLD`: maps each key to its index in
    /// `entries`. Never torn down once built (shrinking back to a `Vec` scan isn't
    /// worth the bookkeeping for the rare grow-then-shrink object).
    index: Option<FxHashMap<SmartString, usize>>,
}

impl FastMap {
    /// Once an object reaches this many keys, build a `key -> position` side index so
    /// lookups/inserts go O(1) instead of O(n) (and the build stops being O(n²)).
    /// Below it, the contiguous `Vec` scan wins — no hashing, cache-friendly — which
    /// is the overwhelmingly common shape (a handful to a few dozen keys per object).
    const INDEX_THRESHOLD: usize = 64;

    pub fn new() -> Self {
        FastMap {
            entries: vec![],
            index: None,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        FastMap {
            entries: Vec::with_capacity(capacity),
            index: None,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The single choke point every read and write resolves a key through: O(1) via
    /// the side index once it exists, otherwise a linear scan. Keeping both reads and
    /// writes on this path is what guarantees the index can never disagree with `entries`.
    fn position(&self, key: &str) -> Option<usize> {
        match &self.index {
            Some(index) => index.get(key).copied(),
            None => self.entries.iter().position(|(k, _)| k.as_str() == key),
        }
    }

    /// Build the side index the first time `entries` crosses the threshold; after that
    /// `insert`/`remove` keep it in sync incrementally, so this is a one-shot O(n) pass.
    fn maybe_build_index(&mut self) {
        if self.index.is_none() && self.entries.len() >= Self::INDEX_THRESHOLD {
            let mut index =
                FxHashMap::with_capacity_and_hasher(self.entries.len(), Default::default());
            for (i, (k, _)) in self.entries.iter().enumerate() {
                index.insert(k.clone(), i);
            }
            self.index = Some(index);
        }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.position(key).is_some()
    }

    pub fn get(&self, key: &str) -> Option<&FastValue> {
        self.position(key).map(|i| &self.entries[i].1)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut FastValue> {
        let i = self.position(key)?;
        Some(&mut self.entries[i].1)
    }

    /// Insert, replacing the value *in place* if the key exists (the key keeps its
    /// original position) — identical to `IndexMap::insert`.
    pub fn insert<K: Into<SmartString>>(&mut self, key: K, value: FastValue) -> Option<FastValue> {
        let key = key.into();
        if let Some(i) = self.position(&key) {
            return Some(std::mem::replace(&mut self.entries[i].1, value));
        }
        let new_index = self.entries.len();
        if let Some(index) = &mut self.index {
            index.insert(key.clone(), new_index);
        }
        self.entries.push((key, value));
        self.maybe_build_index();
        None
    }

    /// Remove by key using **swap-remove**: the last entry moves into the removed slot.
    /// This matches `serde_json::Map::remove` under `preserve_order`, which the flatten
    /// core's output column order depends on. Do not "fix" this to a shifting remove.
    pub fn remove(&mut self, key: &str) -> Option<FastValue> {
        let index = self.position(key)?;
        let last = self.entries.len() - 1;
        let value = self.entries.swap_remove(index).1;
        if let Some(map) = &mut self.index {
            map.remove(key);
            if index != last {
                // swap_remove moved what was the last entry into `index`; repoint it.
                let moved_key = self.entries[index].0.clone();
                map.insert(moved_key, index);
            }
        }
        Some(value)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, (SmartString, FastValue)> {
        self.entries.iter()
    }

    /// Mutable iteration over entries. Mutating a *value* is fine; mutating a *key* in
    /// place is not — it would desync the side index (see the type-level note). The
    /// flatten core only ever reassigns values here.
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, (SmartString, FastValue)> {
        self.entries.iter_mut()
    }

    pub fn keys(&self) -> impl Iterator<Item = &SmartString> {
        self.entries.iter().map(|(k, _)| k)
    }
}

/// Equality ignores the side index (a pure lookup accelerator): two maps with the
/// same entries in the same order are equal whether or not either has crossed the
/// indexing threshold.
impl PartialEq for FastMap {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}

impl IntoIterator for FastMap {
    type Item = (SmartString, FastValue);
    type IntoIter = std::vec::IntoIter<(SmartString, FastValue)>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<'a> IntoIterator for &'a FastMap {
    type Item = &'a (SmartString, FastValue);
    type IntoIter = std::slice::Iter<'a, (SmartString, FastValue)>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

impl<'a> IntoIterator for &'a mut FastMap {
    type Item = &'a mut (SmartString, FastValue);
    type IntoIter = std::slice::IterMut<'a, (SmartString, FastValue)>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter_mut()
    }
}

impl<K: Into<SmartString>> FromIterator<(K, FastValue)> for FastMap {
    fn from_iter<I: IntoIterator<Item = (K, FastValue)>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut map = FastMap::with_capacity(iter.size_hint().0);
        for (k, v) in iter {
            map.insert(k, v);
        }
        map
    }
}

// ---- parsing: inherit serde_json's lexer, build the lean tree ----

impl<'de> Deserialize<'de> for FastValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FastVisitor;

        impl<'de> Visitor<'de> for FastVisitor {
            type Value = FastValue;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("any valid JSON value")
            }

            fn visit_bool<E>(self, v: bool) -> Result<FastValue, E> {
                Ok(FastValue::Bool(v))
            }

            // serde_json's parser routes non-negative integers to visit_u64 and negative
            // ones here; normalize anyway so programmatic construction keeps the same
            // UInt(≥0)/Int(<0) partition as serde_json's PosInt/NegInt.
            fn visit_i64<E>(self, v: i64) -> Result<FastValue, E> {
                if v >= 0 {
                    Ok(FastValue::UInt(v as u64))
                } else {
                    Ok(FastValue::Int(v))
                }
            }

            fn visit_u64<E>(self, v: u64) -> Result<FastValue, E> {
                Ok(FastValue::UInt(v))
            }

            fn visit_f64<E>(self, v: f64) -> Result<FastValue, E> {
                Ok(FastValue::Float(v))
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<FastValue, E> {
                Ok(FastValue::String(SmartString::from(v)))
            }

            fn visit_string<E>(self, v: String) -> Result<FastValue, E> {
                Ok(FastValue::String(SmartString::from(v)))
            }

            fn visit_unit<E>(self) -> Result<FastValue, E> {
                Ok(FastValue::Null)
            }

            fn visit_none<E>(self) -> Result<FastValue, E> {
                Ok(FastValue::Null)
            }

            fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<FastValue, D::Error> {
                FastValue::deserialize(d)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<FastValue, A::Error> {
                let mut vec = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(element) = seq.next_element()? {
                    vec.push(element);
                }
                Ok(FastValue::Array(vec))
            }

            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<FastValue, A::Error> {
                let mut map = FastMap::with_capacity(access.size_hint().unwrap_or(0));
                while let Some(key) = access.next_key::<SmartString>()? {
                    let value = access.next_value()?;
                    map.insert(key, value);
                }
                Ok(FastValue::Object(map))
            }
        }

        deserializer.deserialize_any(FastVisitor)
    }
}

// ---- serialization: route through serde so output (escaping, itoa/ryu formatting)
//      is byte-identical to serde_json::Value's ----

impl Serialize for FastValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            FastValue::Null => serializer.serialize_unit(),
            FastValue::Bool(b) => serializer.serialize_bool(*b),
            FastValue::Int(i) => serializer.serialize_i64(*i),
            FastValue::UInt(u) => serializer.serialize_u64(*u),
            FastValue::Float(f) => serializer.serialize_f64(*f),
            FastValue::String(s) => serializer.serialize_str(s),
            FastValue::Array(a) => a.serialize(serializer),
            FastValue::Object(m) => {
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (k, v) in m.iter() {
                    map.serialize_entry(k.as_str(), v)?;
                }
                map.end()
            }
        }
    }
}

impl Serialize for FastMap {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for (k, v) in self.iter() {
            map.serialize_entry(k.as_str(), v)?;
        }
        map.end()
    }
}

/// Compact JSON, identical to `serde_json::Value`'s `Display`.
impl fmt::Display for FastValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = serde_json::to_string(self).map_err(|_| fmt::Error)?;
        f.write_str(&s)
    }
}

// ---- conversions for the serde_json::Value boundaries (public API shim, jsonpath) ----

impl From<serde_json::Value> for FastValue {
    fn from(value: serde_json::Value) -> FastValue {
        match value {
            serde_json::Value::Null => FastValue::Null,
            serde_json::Value::Bool(b) => FastValue::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(u) = n.as_u64() {
                    FastValue::UInt(u)
                } else if let Some(i) = n.as_i64() {
                    FastValue::Int(i)
                } else {
                    FastValue::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => FastValue::String(SmartString::from(s)),
            serde_json::Value::Array(a) => {
                FastValue::Array(a.into_iter().map(FastValue::from).collect())
            }
            serde_json::Value::Object(o) => FastValue::Object(
                o.into_iter()
                    .map(|(k, v)| (SmartString::from(k), FastValue::from(v)))
                    .collect(),
            ),
        }
    }
}

impl From<FastValue> for serde_json::Value {
    fn from(value: FastValue) -> serde_json::Value {
        match value {
            FastValue::Null => serde_json::Value::Null,
            FastValue::Bool(b) => serde_json::Value::Bool(b),
            FastValue::Int(i) => serde_json::Value::Number(i.into()),
            FastValue::UInt(u) => serde_json::Value::Number(u.into()),
            FastValue::Float(f) => serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            FastValue::String(s) => serde_json::Value::String(s.into()),
            FastValue::Array(a) => {
                serde_json::Value::Array(a.into_iter().map(serde_json::Value::from).collect())
            }
            FastValue::Object(m) => serde_json::Value::Object(
                m.into_iter()
                    .map(|(k, v)| (String::from(k), serde_json::Value::from(v)))
                    .collect(),
            ),
        }
    }
}

impl From<&str> for FastValue {
    fn from(s: &str) -> FastValue {
        FastValue::String(SmartString::from(s))
    }
}

impl From<String> for FastValue {
    fn from(s: String) -> FastValue {
        FastValue::String(SmartString::from(s))
    }
}

impl From<SmartString> for FastValue {
    fn from(s: SmartString) -> FastValue {
        FastValue::String(s)
    }
}

impl From<bool> for FastValue {
    fn from(b: bool) -> FastValue {
        FastValue::Bool(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn fast(json: &str) -> FastValue {
        serde_json::from_str(json).unwrap()
    }

    fn slow(json: &str) -> Value {
        serde_json::from_str(json).unwrap()
    }

    /// Both trees must serialize identically for anything the lexer accepts.
    fn assert_parity(json: &str) {
        assert_eq!(
            fast(json).to_string(),
            slow(json).to_string(),
            "serialization parity failed for input: {json}"
        );
    }

    #[test]
    fn serialization_parity_battery() {
        for json in [
            "null",
            "true",
            "false",
            "0",
            "-1",
            "1.5",
            "-1.5",
            "1.0",
            "1e21",
            "1e-7",
            "123456789.123456789",
            r#""""#,
            r#""hello""#,
            r#""with \"escapes\" and \\ backslash""#,
            r#""newline\nand\ttab""#,
            r#""unicode A é snowman ☃""#,
            r#""surrogate pair 😀""#,
            r#""escaped controls \u0000\u001f and \u0041""#,
            "[]",
            "{}",
            r#"[1,"two",3.0,null,true,{"a":[]}]"#,
            r#"{"a":1,"b":{"c":[1,2,{"d":null}],"e":"f"},"g":-0.5}"#,
            r#"{"deep":{"deep":{"deep":{"deep":{"deep":[1]}}}}}"#,
        ] {
            assert_parity(json);
        }
    }

    #[test]
    fn number_classification_matches_serde() {
        // partition parity at the i64/u64 boundaries and overflow-to-float
        assert_eq!(fast("0"), FastValue::UInt(0));
        assert_eq!(fast("9223372036854775807"), FastValue::UInt(9223372036854775807));
        assert_eq!(
            fast("18446744073709551615"),
            FastValue::UInt(u64::MAX)
        );
        assert_eq!(
            fast("-9223372036854775808"),
            FastValue::Int(i64::MIN)
        );
        assert!(matches!(fast("18446744073709551616"), FastValue::Float(_)));
        assert!(matches!(fast("-9223372036854775809"), FastValue::Float(_)));
        assert!(matches!(fast("1.0"), FastValue::Float(_)));
        for json in [
            "0",
            "-0",
            "9223372036854775807",
            "18446744073709551615",
            "18446744073709551616",
            "-9223372036854775808",
            "-9223372036854775809",
            "1.0",
            "1e3",
        ] {
            assert_parity(json);
            // is_f64 partition must agree with serde_json::Number's
            let f = fast(json);
            if let Value::Number(n) = slow(json) {
                assert_eq!(f.is_f64(), n.is_f64(), "is_f64 disagrees for {json}");
                assert_eq!(f.as_f64(), n.as_f64(), "as_f64 disagrees for {json}");
                assert_eq!(f.as_i64(), n.as_i64(), "as_i64 disagrees for {json}");
            } else {
                panic!("expected number for {json}");
            }
        }
    }

    /// Pin the exact CSV-cell formatting rules from `value_convert`: integers via
    /// `to_string`, floats via `format!("{}", f64)` (so `1.0` renders as `1`).
    #[test]
    fn cell_format_parity_with_value_convert_rules() {
        for json in ["1", "-1", "1.5", "1.0", "1e21", "18446744073709551615"] {
            let fast_cell = match fast(json) {
                FastValue::Int(i) => i.to_string(),
                FastValue::UInt(u) => u.to_string(),
                FastValue::Float(f) => format!("{}", f),
                _ => panic!("expected number"),
            };
            let serde_cell = if let Value::Number(n) = slow(json) {
                if n.is_f64() {
                    format!("{}", n.as_f64().unwrap())
                } else {
                    n.to_string()
                }
            } else {
                panic!("expected number")
            };
            assert_eq!(fast_cell, serde_cell, "cell format differs for {json}");
        }
    }

    /// `remove` must be a swap-remove, exactly like `serde_json::Map::remove` under
    /// `preserve_order` — output column order depends on it.
    #[test]
    fn remove_is_swap_remove_like_serde_map() {
        let json = r#"{"a":1,"b":2,"c":3,"d":4,"e":5}"#;
        for key in ["a", "b", "c", "d", "e"] {
            let mut f = fast(json);
            let mut s = slow(json);
            let fr = f.as_object_mut().unwrap().remove(key);
            let sr = s.as_object_mut().unwrap().remove(key);
            assert_eq!(
                fr.map(|v| v.to_string()),
                sr.map(|v| v.to_string()),
                "removed value differs for key {key}"
            );
            assert_eq!(
                f.to_string(),
                s.to_string(),
                "key order after removing {key} differs from serde_json::Map"
            );
        }
        // removing a missing key is a no-op on both
        let mut f = fast(json);
        assert_eq!(f.as_object_mut().unwrap().remove("zz"), None);
        assert_eq!(f.to_string(), slow(json).to_string());
    }

    #[test]
    fn insert_existing_key_keeps_position_like_serde_map() {
        let json = r#"{"a":1,"b":2,"c":3}"#;
        let mut f = fast(json);
        let mut s = slow(json);
        let f_old = f.as_object_mut().unwrap().insert("b", fast("99"));
        let s_old = s.as_object_mut().unwrap().insert("b".into(), slow("99"));
        assert_eq!(f_old.map(|v| v.to_string()), s_old.map(|v| v.to_string()));
        assert_eq!(f.to_string(), s.to_string());
        // new key appends on both
        f.as_object_mut().unwrap().insert("d", fast("4"));
        s.as_object_mut().unwrap().insert("d".into(), slow("4"));
        assert_eq!(f.to_string(), s.to_string());
    }

    #[test]
    fn duplicate_input_keys_match_serde() {
        // last value wins, key keeps its first position — inherited from insert semantics
        assert_parity(r#"{"a":1,"b":2,"a":3}"#);
        assert_eq!(fast(r#"{"a":1,"b":2,"a":3}"#).to_string(), r#"{"a":3,"b":2}"#);
    }

    #[test]
    fn take_leaves_null() {
        let mut v = fast(r#"{"a":[1,2]}"#);
        let taken = v.as_object_mut().unwrap().get_mut("a").unwrap().take();
        assert_eq!(taken.to_string(), "[1,2]");
        assert_eq!(v.to_string(), r#"{"a":null}"#);
    }

    #[test]
    fn map_api_basics() {
        let mut v = fast(r#"{"a":1,"b":"x"}"#);
        let obj = v.as_object_mut().unwrap();
        assert_eq!(obj.len(), 2);
        assert!(!obj.is_empty());
        assert!(obj.contains_key("a"));
        assert!(!obj.contains_key("z"));
        assert_eq!(obj.get("b").and_then(FastValue::as_str), Some("x"));
        *obj.get_mut("a").unwrap() = FastValue::Bool(true);
        assert_eq!(obj.get("a"), Some(&FastValue::Bool(true)));
        assert_eq!(
            obj.keys().map(|k| k.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        for (_k, value) in obj.iter_mut() {
            *value = FastValue::Null;
        }
        assert_eq!(v.to_string(), r#"{"a":null,"b":null}"#);
    }

    #[test]
    fn parse_errors_inherit_serde_line_col() {
        let err = serde_json::from_str::<FastValue>("{\"a\": tru}\n").unwrap_err();
        let serr = serde_json::from_str::<Value>("{\"a\": tru}\n").unwrap_err();
        assert_eq!(err.to_string(), serr.to_string());
        assert!(err.to_string().contains("line"));
    }

    #[test]
    fn value_conversions_roundtrip() {
        let json = r#"{"a":[1,-2,3.5,"x",null,true],"b":{"c":18446744073709551615}}"#;
        let f = fast(json);
        let via_serde: Value = f.clone().into();
        assert_eq!(via_serde.to_string(), f.to_string());
        let back: FastValue = via_serde.into();
        assert_eq!(back, f);
        assert_eq!(back.to_string(), slow(json).to_string());
    }

    #[test]
    fn accessor_parity() {
        let v = fast(r#"{"s":"str","arr":[1],"obj":{},"n":null}"#);
        let obj = v.as_object().unwrap();
        assert!(obj.get("s").unwrap().is_string());
        assert_eq!(obj.get("s").unwrap().as_str(), Some("str"));
        assert!(obj.get("arr").unwrap().is_array());
        assert_eq!(obj.get("arr").unwrap().as_array().unwrap().len(), 1);
        assert!(obj.get("obj").unwrap().is_object());
        assert!(obj.get("n").unwrap().is_null());
        assert_eq!(obj.get("s").unwrap().as_f64(), None);
    }

    /// A map large enough to switch to the side index must behave *identically* to
    /// `serde_json::Map` (preserve_order) across parse order, get, insert (replace +
    /// append) and the swap-remove — and the index must stay consistent through it all.
    #[test]
    fn large_map_indexed_parity() {
        let n = 200; // well past INDEX_THRESHOLD, so the side index is live
        assert!(n > FastMap::INDEX_THRESHOLD);
        let mut json = String::from("{");
        for i in 0..n {
            if i > 0 {
                json.push(',');
            }
            json.push_str(&format!("\"k{i}\":{i}"));
        }
        json.push('}');

        let mut f = fast(&json);
        let mut s = slow(&json);
        assert_eq!(f.to_string(), s.to_string(), "large-map parse/order parity");

        // reads resolve through the index
        {
            let fo = f.as_object().unwrap();
            assert!(fo.contains_key("k0"));
            assert!(fo.contains_key("k150"));
            assert!(fo.contains_key("k199"));
            assert!(!fo.contains_key("nope"));
            assert_eq!(fo.get("k0").unwrap().to_string(), "0");
            assert_eq!(fo.get("k150").unwrap().to_string(), "150");
            assert_eq!(fo.get("k199").unwrap().to_string(), "199");
            assert_eq!(fo.get("missing"), None);
        }

        // insert: replace existing (keeps position) + append a new key
        let f_old = f.as_object_mut().unwrap().insert("k100", fast("999"));
        let s_old = s.as_object_mut().unwrap().insert("k100".into(), slow("999"));
        assert_eq!(f_old.map(|v| v.to_string()), s_old.map(|v| v.to_string()));
        f.as_object_mut().unwrap().insert("knew", fast("-1"));
        s.as_object_mut().unwrap().insert("knew".into(), slow("-1"));
        assert_eq!(f.to_string(), s.to_string(), "insert parity on indexed map");

        // swap-remove a spread of keys; order after each must match serde_json::Map
        let removed = ["k0", "k150", "k199", "k50", "knew"];
        for key in removed {
            let fr = f.as_object_mut().unwrap().remove(key);
            let sr = s.as_object_mut().unwrap().remove(key);
            assert_eq!(
                fr.map(|v| v.to_string()),
                sr.map(|v| v.to_string()),
                "removed value parity for {key}"
            );
            assert_eq!(f.to_string(), s.to_string(), "order after removing {key}");
        }

        // after the swap-removes, every surviving key must still resolve to its value
        // through the index (catches a botched swap_remove index fix-up)
        let fo = f.as_object().unwrap();
        for i in 0..n {
            let k = format!("k{i}");
            if removed.contains(&k.as_str()) {
                assert_eq!(fo.get(&k), None, "{k} should be gone");
            } else if k == "k100" {
                assert_eq!(fo.get(&k).unwrap().to_string(), "999"); // replaced earlier
            } else {
                assert_eq!(
                    fo.get(&k).unwrap().to_string(),
                    i.to_string(),
                    "stale index entry for {k}"
                );
            }
        }
        assert!(fo.get("knew").is_none());
    }

    /// Build across the threshold via repeated `insert` (the parse path), then confirm
    /// every key is findable and a replace neither duplicates nor desyncs.
    #[test]
    fn index_threshold_boundary_is_consistent() {
        let count = FastMap::INDEX_THRESHOLD + 5;
        let mut m = FastMap::new();
        for i in 0..count {
            assert_eq!(m.insert(format!("k{i}"), FastValue::UInt(i as u64)), None);
        }
        assert_eq!(m.len(), count);
        for i in 0..count {
            assert_eq!(m.get(&format!("k{i}")), Some(&FastValue::UInt(i as u64)));
        }
        // replacing an existing key returns the old value, keeps len, updates the value
        assert_eq!(
            m.insert("k3", FastValue::UInt(333)),
            Some(FastValue::UInt(3))
        );
        assert_eq!(m.len(), count);
        assert_eq!(m.get("k3"), Some(&FastValue::UInt(333)));
        assert!(!m.contains_key("absent"));
    }
}
