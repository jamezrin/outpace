//! Immutable, presence-preserving startup profiles for broadcast origination.
use ace_wire::bencode::Bencode;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{self, Write};

const MAX_JSON_BYTES: usize = 262_144;

#[derive(Clone, Default, PartialEq, Eq, Serialize)]
pub struct BroadcastOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bitrate_bytes_per_sec: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub piece_length: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub categories: Option<Vec<String>>,
}
impl fmt::Debug for BroadcastOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BroadcastOptions { .. }")
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct BroadcastOptionsError;
// Result-returning main uses Debug through std::process::Termination, so both
// presentations must expose the same sanitized public code.
impl fmt::Debug for BroadcastOptionsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for BroadcastOptionsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid_broadcast_options")
    }
}
impl std::error::Error for BroadcastOptionsError {}

pub(crate) struct ResolvedBroadcastOptions {
    pub title: String,
    pub bitrate_bytes_per_sec: u64,
    pub piece_length: u64,
    pub quality: String,
    pub categories: Vec<String>,
}
fn valid_text(value: &str, limit: usize) -> bool {
    !value.is_empty()
        && value.len() <= limit
        && value.trim() == value
        && !value.chars().any(char::is_control)
}
impl BroadcastOptions {
    fn validate(&self) -> Result<(), BroadcastOptionsError> {
        if self.title.as_ref().is_some_and(|s| !valid_text(s, 256))
            || self
                .bitrate_bytes_per_sec
                .is_some_and(|n| n == 0 || n > u64::MAX / 8)
            || self
                .piece_length
                .is_some_and(|n| !(65_536..=1_048_576).contains(&n) || !n.is_power_of_two())
            || self
                .quality
                .as_deref()
                .is_some_and(|s| !matches!(s, "auto" | "SD" | "HD"))
        {
            return Err(BroadcastOptionsError);
        }
        if let Some(categories) = &self.categories {
            let mut seen = BTreeSet::new();
            if categories.len() > 32
                || categories
                    .iter()
                    .any(|s| !valid_text(s, 128) || !seen.insert(s))
            {
                return Err(BroadcastOptionsError);
            }
        }
        Ok(())
    }
    pub(crate) fn resolve_fresh(
        &self,
        name: &str,
    ) -> Result<ResolvedBroadcastOptions, BroadcastOptionsError> {
        self.validate()?;
        if !crate::broadcast::valid_broadcast_name(name) {
            return Err(BroadcastOptionsError);
        }
        let bitrate = self.bitrate_bytes_per_sec.unwrap_or(8375);
        bitrate.checked_mul(8).ok_or(BroadcastOptionsError)?;
        i64::try_from(bitrate).map_err(|_| BroadcastOptionsError)?;
        Ok(ResolvedBroadcastOptions {
            title: self.title.clone().unwrap_or_else(|| name.to_owned()),
            bitrate_bytes_per_sec: bitrate,
            piece_length: self.piece_length.unwrap_or(65_536),
            quality: self.quality.clone().unwrap_or_else(|| "auto".to_owned()),
            categories: self
                .categories
                .clone()
                .unwrap_or_else(|| vec!["other".to_owned()]),
        })
    }
    pub(crate) fn matches_descriptor(&self, raw: &Bencode) -> bool {
        self.title
            .as_ref()
            .is_none_or(|s| raw.get(b"name").and_then(Bencode::as_bytes) == Some(s.as_bytes()))
            && self.bitrate_bytes_per_sec.is_none_or(|n| {
                i64::try_from(n)
                    .ok()
                    .is_some_and(|n| raw.get(b"bitrate").and_then(Bencode::as_int) == Some(n))
            })
            && self.piece_length.is_none_or(|n| {
                i64::try_from(n)
                    .ok()
                    .is_some_and(|n| raw.get(b"piece_length").and_then(Bencode::as_int) == Some(n))
            })
            && self.quality.as_ref().is_none_or(|s| {
                raw.get(b"quality").and_then(Bencode::as_bytes) == Some(s.as_bytes())
            })
            && self
                .categories
                .as_ref()
                .is_none_or(|categories| match raw.get(b"categories") {
                    Some(Bencode::List(items)) => {
                        items.len() == categories.len()
                            && items
                                .iter()
                                .zip(categories)
                                .all(|(item, s)| item.as_bytes() == Some(s.as_bytes()))
                    }
                    _ => false,
                })
    }
}
// Reject long text and category sequences before accumulating a profile. This
// also bounds Config deserialization independently of the environment byte cap.
struct ProfileText<const MAX: usize>(String);
impl<'de, const MAX: usize> Deserialize<'de> for ProfileText<MAX> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TextVisitor<const MAX: usize>;
        impl<const MAX: usize> Visitor<'_> for TextVisitor<MAX> {
            type Value = ProfileText<MAX>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("bounded profile text")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                if !valid_text(value, MAX) {
                    return Err(E::custom(BroadcastOptionsError));
                }
                Ok(ProfileText(value.to_owned()))
            }
            fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
                if !valid_text(&value, MAX) {
                    return Err(E::custom(BroadcastOptionsError));
                }
                Ok(ProfileText(value))
            }
        }
        deserializer.deserialize_string(TextVisitor::<MAX>)
    }
}
struct Categories(Vec<String>);
impl<'de> Deserialize<'de> for Categories {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct CategoriesVisitor;
        impl<'de> Visitor<'de> for CategoriesVisitor {
            type Value = Categories;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("bounded category list")
            }
            fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Self::Value, S::Error> {
                let mut items = Vec::new();
                while let Some(ProfileText(item)) = seq.next_element::<ProfileText<128>>()? {
                    if items.len() >= 32 || items.contains(&item) {
                        return Err(de::Error::custom(BroadcastOptionsError));
                    }
                    items.push(item);
                }
                Ok(Categories(items))
            }
        }
        deserializer.deserialize_seq(CategoriesVisitor)
    }
}

impl<'de> Deserialize<'de> for BroadcastOptions {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct OptionsVisitor;
        impl<'de> Visitor<'de> for OptionsVisitor {
            type Value = BroadcastOptions;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("broadcast options object")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut options = BroadcastOptions::default();
                let mut seen = BTreeSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !seen.insert(key.clone()) {
                        return Err(de::Error::custom(BroadcastOptionsError));
                    }
                    match key.as_str() {
                        "title" => options.title = Some(map.next_value::<ProfileText<256>>()?.0),
                        "bitrate_bytes_per_sec" => {
                            options.bitrate_bytes_per_sec = Some(map.next_value::<u64>()?)
                        }
                        "piece_length" => options.piece_length = Some(map.next_value::<u64>()?),
                        "quality" => options.quality = Some(map.next_value::<ProfileText<4>>()?.0),
                        "categories" => {
                            options.categories = Some(map.next_value::<Categories>()?.0)
                        }
                        _ => return Err(de::Error::custom(BroadcastOptionsError)),
                    }
                }
                options.validate().map_err(de::Error::custom)?;
                Ok(options)
            }
        }
        deserializer
            .deserialize_map(OptionsVisitor)
            .map_err(|_| de::Error::custom(BroadcastOptionsError))
    }
}

pub(crate) fn parse_broadcast_options(
    raw: Option<&str>,
) -> Result<BTreeMap<String, BroadcastOptions>, BroadcastOptionsError> {
    let Some(raw) = raw else {
        return Ok(BTreeMap::new());
    };
    if raw.len() > MAX_JSON_BYTES {
        return Err(BroadcastOptionsError);
    }
    let mut deserializer = serde_json::Deserializer::from_str(raw);
    let profiles =
        deserialize_broadcast_options(&mut deserializer).map_err(|_| BroadcastOptionsError)?;
    deserializer.end().map_err(|_| BroadcastOptionsError)?;
    Ok(profiles)
}

pub(crate) fn deserialize_broadcast_options<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, BroadcastOptions>, D::Error> {
    struct ProfilesVisitor;
    impl<'de> Visitor<'de> for ProfilesVisitor {
        type Value = BTreeMap<String, BroadcastOptions>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("broadcast profiles object")
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut profiles = BTreeMap::new();
            while let Some(name) = map.next_key::<String>()? {
                if profiles.len() >= 64
                    || !crate::broadcast::valid_broadcast_name(&name)
                    || profiles.contains_key(&name)
                {
                    return Err(de::Error::custom(BroadcastOptionsError));
                }
                profiles.insert(name, map.next_value()?);
            }
            Ok(profiles)
        }
    }
    let profiles = deserializer
        .deserialize_map(ProfilesVisitor)
        .map_err(|_| de::Error::custom(BroadcastOptionsError))?;
    validate_broadcast_options(&profiles).map_err(de::Error::custom)?;
    Ok(profiles)
}

// Count compact serialization without allocating an unbounded intermediate for direct Config callers.
struct SizeLimit(usize);
impl Write for SizeLimit {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|n| *n <= MAX_JSON_BYTES)
            .ok_or_else(|| io::Error::other("invalid_broadcast_options"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(crate) fn validate_broadcast_options(
    options: &BTreeMap<String, BroadcastOptions>,
) -> Result<(), BroadcastOptionsError> {
    if options.len() > 64 {
        return Err(BroadcastOptionsError);
    }
    for (name, options) in options {
        if !crate::broadcast::valid_broadcast_name(name) {
            return Err(BroadcastOptionsError);
        }
        options.validate()?;
    }
    serde_json::to_writer(SizeLimit(0), options).map_err(|_| BroadcastOptionsError)
}
