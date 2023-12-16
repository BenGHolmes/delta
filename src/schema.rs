use crate::error::DeltaError;
use polars::datatypes::{DataType, TimeUnit};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DeltaTableSchema {
    fields: Vec<DeltaTableColumnDefinition>,
    #[serde(rename = "type")]
    typ: DeltaTableStructType,
}

impl DeltaTableSchema {
    pub fn from_sql(sql_schema: Vec<(&str, &str)>) -> Result<Self, DeltaError> {
        let mut fields = vec![];
        for sql_col in sql_schema {
            let typ = DeltaTableType::from_sql_type(sql_col.1)?;

            fields.push(DeltaTableColumnDefinition {
                name: sql_col.0.to_owned(),
                typ,
                nullable: false,
                metadata: HashMap::new(),
            })
        }

        Ok(Self {
            fields,
            typ: DeltaTableStructType::Struct,
        })
    }

    pub fn is_valid(&self) -> bool {
        let mut seen: HashSet<&str> = HashSet::new();
        self.fields.iter().all(|field| {
            // Make sure this field name is unique
            if seen.contains(field.name.as_str()) {
                return false;
            }

            seen.insert(field.name.as_str());
            return field.is_valid();
        })
    }

    pub fn fields(&self) -> &Vec<DeltaTableColumnDefinition> {
        &self.fields
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DeltaTableColumnDefinition {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: DeltaTableType,
    pub nullable: bool,
    metadata: HashMap<String, String>,
}

impl DeltaTableColumnDefinition {
    fn is_valid(&self) -> bool {
        // Don't know how to handle null values yet.
        // Will just ignore the metadata field so we
        // don't need to enforce that it's empty.
        !self.nullable
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum DeltaTableType {
    String,
    Long,
    Integer,
    Short,
    Byte,
    Float,
    Double,
    Boolean,
    Date,
    Timestamp,
}

// This is a bit of a hack to get the top-level `"type": "struct"` tag
// for the metadata schema field. Don't want to support structs in general
// yet, but this allows us to add a hardcoded field to the DeltaTableSchema
// struct. Without this we'd just need to make it a string and validate.
#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum DeltaTableStructType {
    Struct,
}

impl DeltaTableType {
    pub fn from_sql_type(sql_type: &str) -> Result<DeltaTableType, DeltaError> {
        match sql_type.to_uppercase().as_str() {
            "TEXT" => Ok(DeltaTableType::String),
            "BIGINT" => Ok(DeltaTableType::Long),
            "INT" => Ok(DeltaTableType::Integer),
            "SMALLINT" => Ok(DeltaTableType::Short),
            "TINYINT" => Ok(DeltaTableType::Byte),
            "FLOAT" => Ok(DeltaTableType::Float),
            "DOUBLE" => Ok(DeltaTableType::Double),
            "BOOL" => Ok(DeltaTableType::Boolean),
            "DATE" => Ok(DeltaTableType::Date),
            "TIMESTAMP" => Ok(DeltaTableType::Timestamp),
            _ => Err(DeltaError::InvalidType),
        }
    }

    pub fn to_polars_type(&self) -> DataType {
        match self {
            Self::String => DataType::Utf8,
            Self::Long => DataType::Int64,
            Self::Integer => DataType::Int32,
            Self::Short => DataType::Int16,
            Self::Byte => DataType::Int8,
            Self::Float => DataType::Float32,
            Self::Double => DataType::Float64,
            Self::Boolean => DataType::Boolean,
            Self::Date => DataType::Date,
            Self::Timestamp => DataType::Datetime(TimeUnit::Microseconds, None),
        }
    }
}
