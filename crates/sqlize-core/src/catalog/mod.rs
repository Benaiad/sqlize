pub mod ddl;
pub mod types;

use std::collections::BTreeMap;

use types::{ColumnName, TableName, VirtualTable};

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
                return Err(Error::Spec(format!(
                    "duplicate table name: {}",
                    table.name
                )));
            }
            map.insert(table.name.clone(), table);
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

    /// Look up a column within a table, returning a structured error if not found.
    pub fn require_column(
        &self,
        table: &TableName,
        column: &ColumnName,
    ) -> Result<&types::Column> {
        let t = self.require(table)?;
        t.columns
            .iter()
            .find(|c| c.name == *column)
            .ok_or_else(|| Error::ColumnNotFound {
                table: table.clone(),
                column: column.clone(),
            })
    }
}
