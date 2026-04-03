use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_schema::{DataType, Field, Schema};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct SchemaDef {
    pub fields: Vec<FieldDef>,
}

impl SchemaDef {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read schema file {}", path.display()))?;
        let schema: Self = serde_yaml::from_str(&raw)
            .with_context(|| format!("failed to parse schema file {}", path.display()))?;
        Ok(schema)
    }

    pub fn validate(&self) -> Result<()> {
        if self.fields.is_empty() {
            anyhow::bail!("schema must contain at least one field");
        }

        let mut seen = HashSet::new();
        for field in &self.fields {
            if !seen.insert(field.name.clone()) {
                anyhow::bail!("duplicate schema field '{}'", field.name);
            }
        }

        Ok(())
    }

    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|field| field.name == name)
    }

    pub fn arrow_schema(&self) -> Arc<Schema> {
        let fields = self
            .fields
            .iter()
            .map(|field| {
                Field::new(
                    &field.name,
                    field.logical_type.as_arrow_type(),
                    field.nullable,
                )
            })
            .collect::<Vec<_>>();
        Arc::new(Schema::new(fields))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FieldDef {
    pub name: String,
    #[serde(rename = "type")]
    pub logical_type: LogicalType,
    #[serde(default = "default_nullable")]
    pub nullable: bool,
}

const fn default_nullable() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogicalType {
    String,
    Int64,
    Float64,
    Boolean,
    Timestamp,
    Ip,
}

impl LogicalType {
    pub fn as_arrow_type(&self) -> DataType {
        match self {
            LogicalType::String | LogicalType::Ip => DataType::Utf8,
            LogicalType::Int64 => DataType::Int64,
            LogicalType::Float64 => DataType::Float64,
            LogicalType::Boolean => DataType::Boolean,
            LogicalType::Timestamp => {
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
            }
        }
    }
}
