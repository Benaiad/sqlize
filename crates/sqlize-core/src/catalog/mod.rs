pub mod ddl;
pub mod types;

use std::collections::BTreeMap;

use types::{TableName, VirtualTable};

use crate::error::{Error, Result};

/// The schema catalog: an ordered collection of virtual tables.
///
/// Constructed from an OpenAPI spec via `Catalog::from_tables`.
/// Lookup is by `TableName` — the catalog owns the tables.
#[derive(Debug)]
pub struct Catalog {
    tables: BTreeMap<TableName, VirtualTable>,
}

impl Catalog {
    /// Build a catalog from an iterator of virtual tables.
    /// Duplicate table names are an error.
    pub fn from_tables(tables: impl IntoIterator<Item = VirtualTable>) -> Result<Self> {
        let mut map = BTreeMap::new();
        for table in tables {
            if map.contains_key(&table.name) {
                return Err(Error::DuplicateTable(table.name.clone()));
            }
            map.insert(table.name.clone(), table);
        }
        if map.is_empty() {
            tracing::warn!(
                "catalog is empty — no tables were generated from the spec (check your tag filter)"
            );
        }
        Ok(Self { tables: map })
    }

    pub fn get(&self, name: &TableName) -> Option<&VirtualTable> {
        self.tables.get(name)
    }

    pub fn tables(&self) -> impl Iterator<Item = &VirtualTable> {
        self.tables.values()
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Look up a table, returning a structured error if not found.
    pub fn require(&self, name: &TableName) -> Result<&VirtualTable> {
        self.get(name)
            .ok_or_else(|| Error::TableNotFound(name.clone()))
    }
}
