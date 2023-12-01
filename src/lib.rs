use uuid::Uuid;

use {
    serde::{Deserialize, Serialize},
    std::collections::{HashMap, HashSet},
    std::fs,
};

// Features
//  [ ] Create a new deltatable with fixed schema -- CREATE TABLE <TABLE_NAME> (<COLUMN_NAME> <TYPE>, ...)
//  [ ] Insert into a table -- INSERT INTO <TABLE_NAME> VALUES (<VALUE1>, <VALUE2>, ...), ...
//  [ ] Delete from table -- DELETE FROM <TABLE_NAME> WHERE expr
//  [ ] Query a table -- SELECT expr FROM <TABLE_NAME> WHERE expr
//  [ ] Update a table -- UPDATE <TABLE_NAME> SET col1=val1, col2=val2, ... WHERE expr

#[derive(Debug)]
pub enum DeltaError {
    IOError(std::io::Error),
    JsonError(serde_json::Error),
    InvalidType,
    InvalidTable,
    TableAlreadyExists,
}

impl From<std::io::Error> for DeltaError {
    fn from(value: std::io::Error) -> DeltaError {
        DeltaError::IOError(value)
    }
}

impl From<serde_json::Error> for DeltaError {
    fn from(value: serde_json::Error) -> DeltaError {
        DeltaError::JsonError(value)
    }
}

pub struct DeltaTable {
    metadata: DeltaTableMetadata,
    base_dir: String,
    logs_dir: String,
}

impl DeltaTable {
    pub fn create_table(name: &str, schema: Vec<(&str, &str)>) -> Result<DeltaTable, DeltaError> {
        let id = Uuid::new_v4();

        let format = DeltaTableFormat {
            provider: "parquet".to_owned(),
            options: HashMap::new(),
        };

        let schema = DeltaTableSchema::from_sql(schema)?;

        let table = DeltaTable {
            metadata: DeltaTableMetadata {
                id,
                name: name.to_owned(),
                format,
                schema,
                partition_columns: vec![],
                configuration: HashMap::new(),
            },
            base_dir: format!("tables/{}", name),
            logs_dir: format!("tables/{}/logs", name),
        };

        if !table.metadata.is_valid() {
            return Err(DeltaError::InvalidTable);
        }

        // Try to create a directory for the table
        if let Err(e) = fs::create_dir(&table.base_dir) {
            match e.kind() {
                std::io::ErrorKind::AlreadyExists => return Err(DeltaError::TableAlreadyExists),
                _ => return Err(DeltaError::IOError(e)),
            }
        }

        // Make the logs directory
        fs::create_dir(&table.logs_dir)?;

        // Write the first log file
        fs::write(
            format!("{}/000.log", &table.logs_dir),
            serde_json::to_string(&table.metadata)?,
        )?;

        Ok(table)
    }

    pub fn get_datafiles(&self) -> Result<HashSet<String>, DeltaError> {
        let mut logs: Vec<_> = fs::read_dir(&self.logs_dir)?
            .filter_map(|entry| entry.ok())
            .collect();

        logs.sort_by(|a, b| b.file_name().cmp(&a.file_name()));

        let mut removed_files: HashSet<String> = HashSet::new();
        let mut data_files: HashSet<String> = HashSet::new();
        for log in logs {
            for line in fs::read_to_string(log.path())?.lines() {
                let action = serde_json::from_str::<Action>(line)?;

                match action {
                    Action::Add { path, .. } => {
                        if !removed_files.contains(&path) {
                            data_files.insert(path);
                        }
                    }
                    Action::Remove { path, .. } => {
                        removed_files.insert(path);
                    }
                    Action::Metadata { .. } => {
                        todo!("Set the table's schema")
                    }
                }
            }
        }

        Ok(data_files)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
enum Action {
    Add {
        path: String,
        partition_values: HashMap<String, String>,
        size: u64,
        modification_time: u64,
        data_change: bool,
    },
    Remove {
        path: String,
        data_change: bool,
    },
    #[serde(rename = "metaData")]
    Metadata(DeltaTableMetadata),
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeltaTableMetadata {
    id: Uuid,
    name: String,
    format: DeltaTableFormat,
    schema: DeltaTableSchema,
    partition_columns: Vec<String>,
    configuration: HashMap<String, String>,
}

impl DeltaTableMetadata {
    fn is_valid(&self) -> bool {
        self.format.is_valid()
            && self.schema.is_valid()
            && self.partition_columns.is_empty()
            && self.configuration.is_empty()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeltaTableFormat {
    provider: String,
    options: HashMap<String, String>,
}

impl DeltaTableFormat {
    fn is_valid(&self) -> bool {
        // As of Delta Lake 0.3.0, user-facing APIs only allow the creation
        // of tables where `format = 'parquet' and options = {}`
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata
        self.provider == "parquet" && self.options.is_empty()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeltaTableSchema {
    fields: Vec<DeltaTableColumnDefinition>,
    #[serde(rename = "type")]
    typ: DeltaTableStructType,
}

impl DeltaTableSchema {
    fn from_sql(sql_schema: Vec<(&str, &str)>) -> Result<Self, DeltaError> {
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

    fn is_valid(&self) -> bool {
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
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeltaTableColumnDefinition {
    name: String,
    #[serde(rename = "type")]
    typ: DeltaTableType,
    nullable: bool,
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

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum DeltaTableType {
    String,
    Long,
    Integer,
    Short,
    Byte,
    Float,
    Double,
    Decimal,
    Boolean,
    Date,
    Timestamp,
}

// This is a bit of a hack to get the top-level `"type": "struct"` tag
// for the metadata schema field. Don't want to support structs in general
// yet, but this allows us to add a hardcoded field to the DeltaTableSchema
// struct. Without this we'd just need to make it a string and validate.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum DeltaTableStructType {
    Struct,
}

impl DeltaTableType {
    fn from_sql_type(sql_type: &str) -> Result<DeltaTableType, DeltaError> {
        match sql_type.to_uppercase().as_str() {
            "TEXT" => Ok(DeltaTableType::String),
            "BIGINT" => Ok(DeltaTableType::Long),
            "INT" => Ok(DeltaTableType::Integer),
            "SMALLINT" => Ok(DeltaTableType::Short),
            "TINYINT" => Ok(DeltaTableType::Byte),
            "FLOAT" => Ok(DeltaTableType::Float),
            "DOUBLE" => Ok(DeltaTableType::Double),
            "DECIMAL" => Ok(DeltaTableType::Decimal),
            "BOOL" => Ok(DeltaTableType::Boolean),
            "DATE" => Ok(DeltaTableType::Date),
            "TIMESTAMP" => Ok(DeltaTableType::Timestamp),
            _ => Err(DeltaError::InvalidType),
        }
    }

    fn to_sql_type(&self) -> String {
        match self {
            Self::String => "TEXT".to_owned(),
            Self::Long => "BIGINT".to_owned(),
            Self::Integer => "INT".to_owned(),
            Self::Short => "SMALLINT".to_owned(),
            Self::Byte => "TINYINT".to_owned(),
            Self::Float => "FLOAT".to_owned(),
            Self::Double => "DOUBLE".to_owned(),
            Self::Decimal => "DECIMAL".to_owned(),
            Self::Boolean => "BOOL".to_owned(),
            Self::Date => "DATE".to_owned(),
            Self::Timestamp => "TIMESTAMP".to_owned(),
        }
    }
}
