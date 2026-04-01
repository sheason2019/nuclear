use super::LWWRegister;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug, Clone)]
pub struct LWWMap<K, V> {
    entries: HashMap<K, LWWRegister<Option<V>>>,
    node_id: String,
}

impl<K, V> Serialize for LWWMap<K, V>
where
    K: Hash + Eq + Clone + Serialize,
    V: Clone + PartialEq + Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("LWWMap", 2)?;
        state.serialize_field("entries", &self.entries)?;
        state.serialize_field("node_id", &self.node_id)?;
        state.end()
    }
}

impl<'de, K, V> Deserialize<'de> for LWWMap<K, V>
where
    K: Hash + Eq + Clone + Deserialize<'de>,
    V: Clone + PartialEq + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::MapAccess;
        use serde::de::Visitor;
        use std::fmt;

        #[derive(serde::Deserialize)]
        #[serde(field_identifier, rename_all = "camelCase")]
        enum Field {
            Entries,
            NodeId,
        }

        struct LWWMapVisitor<K, V> {
            _marker: std::marker::PhantomData<(K, V)>,
        }

        impl<'de, K, V> Visitor<'de> for LWWMapVisitor<K, V>
        where
            K: Hash + Eq + Clone + Deserialize<'de>,
            V: Clone + PartialEq + Deserialize<'de>,
        {
            type Value = LWWMap<K, V>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct LWWMap")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut entries = None;
                let mut node_id = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Entries => {
                            entries = Some(map.next_value()?);
                        }
                        Field::NodeId => {
                            node_id = Some(map.next_value()?);
                        }
                    }
                }

                let entries = entries.ok_or_else(|| serde::de::Error::missing_field("entries"))?;
                let node_id = node_id.ok_or_else(|| serde::de::Error::missing_field("node_id"))?;

                Ok(LWWMap { entries, node_id })
            }
        }

        const FIELDS: &'static [&'static str] = &["entries", "nodeId"];
        deserializer.deserialize_struct(
            "LWWMap",
            FIELDS,
            LWWMapVisitor {
                _marker: std::marker::PhantomData,
            },
        )
    }
}

impl<K, V> LWWMap<K, V>
where
    K: Clone + Hash + Eq,
    V: Clone + PartialEq,
{
    pub fn new(node_id: &str) -> Self {
        Self {
            entries: HashMap::new(),
            node_id: node_id.to_string(),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        let entry = self
            .entries
            .entry(key)
            .or_insert_with(|| LWWRegister::new(&self.node_id));
        entry.set(Some(value));
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries
            .get(key)
            .and_then(|reg| reg.get().and_then(|v| v.as_ref()))
    }

    pub fn remove(&mut self, key: K) {
        let entry = self
            .entries
            .entry(key)
            .or_insert_with(|| LWWRegister::new(&self.node_id));
        entry.set(None);
    }

    pub fn merge(&mut self, other: &LWWMap<K, V>) {
        for (key, other_reg) in &other.entries {
            let entry = self
                .entries
                .entry(key.clone())
                .or_insert_with(|| LWWRegister::new(&self.node_id));
            entry.merge(other_reg);
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.entries.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.entries
            .values()
            .filter_map(|reg| reg.get().and_then(|v| v.as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lww_map_insert() {
        let mut map = LWWMap::new("node1");
        map.insert("key1".to_string(), "value1".to_string());
        assert_eq!(map.get(&"key1".to_string()), Some(&"value1".to_string()));
    }

    #[test]
    fn test_lww_map_merge() {
        let mut map1 = LWWMap::new("node1");
        map1.insert("key1".to_string(), "value1".to_string());

        let mut map2 = LWWMap::new("node2");
        map2.insert("key1".to_string(), "value2".to_string());
        map2.insert("key2".to_string(), "value3".to_string());

        map1.merge(&map2);
        assert_eq!(map1.get(&"key2".to_string()), Some(&"value3".to_string()));
    }

    #[test]
    fn test_lww_map_remove() {
        let mut map = LWWMap::new("node1");
        map.insert("key1".to_string(), "value1".to_string());
        map.remove("key1".to_string());
        assert_eq!(map.get(&"key1".to_string()), None);
    }
}
