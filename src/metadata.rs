use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{error::DeltaError, schema::DeltaTableSchema};

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DeltaTableMetadata {
    id: Uuid,
    name: String,
    format: DeltaTableFormat,
    // TODO: add back schema field, and implement
    // custom serialize and deserialize logic
    // schema: DeltaTableSchema,
    schema_string: String,
    partition_columns: Vec<String>,
    configuration: HashMap<String, String>,
}

impl DeltaTableMetadata {
    pub fn new(
        id: Uuid,
        name: String,
        format: DeltaTableFormat,
        schema_string: String,
        partition_columns: Vec<String>,
        configuration: HashMap<String, String>,
    ) -> Self {
        DeltaTableMetadata {
            id,
            name,
            format,
            schema_string,
            partition_columns,
            configuration,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.format.is_valid()
            // && self.schema.is_valid() // TODO: add back
            && self.partition_columns.is_empty()
            && self.configuration.is_empty()
    }

    pub fn schema(&self) -> Result<DeltaTableSchema, DeltaError> {
        let schema: DeltaTableSchema = serde_json::from_str(&self.schema_string)?;
        Ok(schema)
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DeltaTableFormat {
    provider: String,
    options: HashMap<String, String>,
}

impl DeltaTableFormat {
    pub fn new(provider: String, options: HashMap<String, String>) -> Self {
        DeltaTableFormat { provider, options }
    }

    fn is_valid(&self) -> bool {
        // As of Delta Lake 0.3.0, user-facing APIs only allow the creation
        // of tables where `format = 'parquet' and options = {}`
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata
        self.provider == "parquet" && self.options.is_empty()
    }
}
