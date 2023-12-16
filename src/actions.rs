use crate::metadata::DeltaTableMetadata;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Action {
    Add {
        path: String,
        partition_values: HashMap<String, String>,
        size: u64,
        modification_time: u128,
        data_change: bool,
    },
    Remove {
        path: String,
        data_change: bool,
    },
    #[serde(rename = "metaData")]
    Metadata(DeltaTableMetadata),
}
