use polars::sql::SQLContext;

use {
    polars::{
        datatypes::{DataType, TimeUnit},
        prelude::*,
        series::Series,
    },
    serde::{Deserialize, Serialize},
    std::collections::{HashMap, HashSet},
    std::{fs, time::SystemTime},
    uuid::Uuid,
};

// Features
//  [ ] Create a new deltatable with fixed schema -- CREATE TABLE <TABLE_NAME> (<COLUMN_NAME> <TYPE>, ...)
//  [ ] Insert into a table -- INSERT INTO <TABLE_NAME> VALUES (<VALUE1>, <VALUE2>, ...), ...
//  [ ] Delete from table -- DELETE FROM <TABLE_NAME> WHERE expr
//  [ ] Query a table -- SELECT expr FROM <TABLE_NAME> WHERE expr
//  [ ] Update a table -- UPDATE <TABLE_NAME> SET col1=val1, col2=val2, ... WHERE expr
//  [ ] SQL query parser and command line tool

#[derive(Debug)]
pub enum DeltaError {
    IOError(std::io::Error),
    JsonError(serde_json::Error),
    PolarsError(PolarsError),
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

impl From<PolarsError> for DeltaError {
    fn from(value: PolarsError) -> Self {
        DeltaError::PolarsError(value)
    }
}

pub struct DeltaTable {
    metadata: DeltaTableMetadata,
    base_dir: String,
    logs_dir: String,
}

impl DeltaTable {
    pub fn read_table(name: &str) -> Result<DeltaTable, DeltaError> {
        todo!("read and set metadata");
    }

    pub fn create_table(name: &str, schema: Vec<(&str, &str)>) -> Result<DeltaTable, DeltaError> {
        let id = Uuid::new_v4();

        let format = DeltaTableFormat {
            provider: "parquet".to_owned(),
            options: HashMap::new(),
        };

        let schema = DeltaTableSchema::from_sql(schema)?;
        if !schema.is_valid() {
            return Err(DeltaError::InvalidTable);
        }

        let table = DeltaTable {
            metadata: DeltaTableMetadata {
                id,
                name: name.to_owned(),
                format,
                schema_string: serde_json::to_string(&schema)?,
                partition_columns: vec![],
                configuration: HashMap::new(),
            },
            base_dir: format!("tables/{}", name),
            logs_dir: format!("tables/{}/_delta_log", name),
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
            format!("{}/{}", &table.logs_dir, table.next_log_file()?),
            serde_json::to_string(&Action::Metadata(table.metadata.clone()))?,
        )?;

        Ok(table)
    }

    pub fn insert(&self, data: Vec<Vec<&str>>) -> Result<(), DeltaError> {
        let schema: DeltaTableSchema = serde_json::from_str(&self.metadata.schema_string)?;
        let n_cols = schema.fields.len();

        // Bad, should fix this
        let cols = (0..n_cols)
            .map(|i| {
                let s = Series::new(
                    &schema.fields[i].name,
                    data.iter().map(|row| row[i]).collect::<Vec<&str>>(),
                );
                s.cast(&schema.fields[i].typ.to_polars_type()).unwrap()
            })
            .collect::<Vec<Series>>();

        let mut df = DataFrame::new(cols)?;

        let data_file = self.write_data_file(&mut df)?;

        fs::write(
            format!("{}/{}", self.logs_dir, self.next_log_file()?),
            serde_json::to_string(&Action::Add {
                path: data_file.name,
                partition_values: HashMap::new(),
                size: data_file.size,
                modification_time: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
                data_change: true,
            })?,
        )?;

        Ok(())
    }

    // For now delete assumes single writer, meaning no race conditions
    // where a new log file is added during the deletion. Should look into
    // how to handle that long term.
    pub fn delete(&self, expr: &str) -> Result<(), DeltaError> {
        let query = format!("SELECT * FROM df WHERE NOT ({});", expr);

        let mut created_files: Vec<DataFile> = vec![];
        let mut deleted_files: Vec<String> = vec![];

        let files = self.get_datafiles()?;
        for file in files {
            let df = LazyFrame::scan_parquet(
                format!("{}/{}", &self.base_dir, &file),
                Default::default(),
            )?
            .collect()?;

            let original_rows = df.height();

            let mut ctx = SQLContext::new();
            ctx.register("df", df.lazy());
            let mut updated = ctx.execute(&query)?.collect()?;

            if updated.height() == original_rows {
                continue; // No rows deleted
            }

            created_files.push(self.write_data_file(&mut updated)?);
            deleted_files.push(file)
        }

        let modification_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let mut actions: Vec<String> = vec![];
        for created in created_files {
            let action = Action::Add {
                path: created.name,
                partition_values: HashMap::new(),
                size: created.size,
                modification_time,
                data_change: true,
            };

            actions.push(serde_json::to_string(&action)?);
        }

        for deleted in deleted_files {
            let action = Action::Remove {
                path: deleted,
                data_change: true,
            };

            actions.push(serde_json::to_string(&action)?);
        }

        let contents = actions.join("\n");
        fs::write(
            format!("{}/{}", self.logs_dir, self.next_log_file()?),
            contents,
        )?;

        Ok(())
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
                    Action::Metadata { .. } => {}
                }
            }
        }

        Ok(data_files)
    }

    fn next_data_file(&self) -> Result<String, DeltaError> {
        // Hacky, but do `n-1` instead of `n` for data files because
        // one of the entries in the base dir is the logs directory.
        let n = fs::read_dir(&self.base_dir)?.collect::<Vec<_>>().len();
        return Ok(format!("{:0>20}.parquet", n - 1));
    }

    fn next_log_file(&self) -> Result<String, DeltaError> {
        let n = fs::read_dir(&self.logs_dir)?.collect::<Vec<_>>().len();
        return Ok(format!("{:0>20}.json", n));
    }

    fn write_data_file(&self, df: &mut DataFrame) -> Result<DataFile, DeltaError> {
        let data_file = self.next_data_file()?;
        let file = fs::File::create(format!("{}/{}", self.base_dir, data_file))?;
        let data_file_size = ParquetWriter::new(file).finish(df)?;

        return Ok(DataFile {
            name: data_file,
            size: data_file_size,
        });
    }
}

struct DataFile {
    name: String,
    size: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
enum Action {
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

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct DeltaTableMetadata {
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
    fn is_valid(&self) -> bool {
        self.format.is_valid()
            // && self.schema.is_valid() // TODO: add back
            && self.partition_columns.is_empty()
            && self.configuration.is_empty()
    }
}

#[derive(Serialize, Deserialize, Clone)]
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

#[derive(Serialize, Deserialize, Clone)]
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

#[derive(Serialize, Deserialize, Clone)]
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

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
enum DeltaTableType {
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
            "BOOL" => Ok(DeltaTableType::Boolean),
            "DATE" => Ok(DeltaTableType::Date),
            "TIMESTAMP" => Ok(DeltaTableType::Timestamp),
            _ => Err(DeltaError::InvalidType),
        }
    }

    fn to_polars_type(&self) -> DataType {
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
