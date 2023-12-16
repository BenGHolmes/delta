// Features
//  [X] Create a new deltatable with fixed schema -- CREATE TABLE <TABLE_NAME> (<COLUMN_NAME> <TYPE>, ...)
//  [X] Insert into a table -- INSERT INTO <TABLE_NAME> VALUES (<VALUE1>, <VALUE2>, ...), ...
//  [X] Delete from table -- DELETE FROM <TABLE_NAME> WHERE expr
//  [ ] Query a table -- SELECT expr FROM <TABLE_NAME> WHERE expr
//  [ ] Update a table -- UPDATE <TABLE_NAME> SET col1=val1, col2=val2, ... WHERE expr
//  [ ] SQL query parser and command line tool

use crate::{
    actions::Action,
    data_file::DataFile,
    error::DeltaError,
    metadata::{DeltaTableFormat, DeltaTableMetadata},
    schema::DeltaTableSchema,
};
use polars::{prelude::*, series::Series, sql::SQLContext};
use std::collections::HashMap;
use std::{collections::HashSet, fs, time::SystemTime};
use uuid::Uuid;

pub struct DeltaTable {
    metadata: DeltaTableMetadata,
    base_dir: String,
    logs_dir: String,
}

impl DeltaTable {
    pub fn read_table(name: &str) -> Result<DeltaTable, DeltaError> {
        let base_dir = format!("tables/{}", name);
        let logs_dir = format!("tables/{}/_delta_log", name);

        let contents = fs::read_to_string(format!("{}/{}", logs_dir, DeltaTable::log_file(0)))?;
        if let Ok(Action::Metadata(metadata)) = serde_json::from_str(&contents) {
            return Ok(DeltaTable {
                metadata,
                base_dir,
                logs_dir,
            });
        }

        return Err(DeltaError::InvalidTable);
    }

    pub fn create_table(name: &str, schema: Vec<(&str, &str)>) -> Result<DeltaTable, DeltaError> {
        let schema = DeltaTableSchema::from_sql(schema)?;
        if !schema.is_valid() {
            return Err(DeltaError::InvalidTable);
        }

        let metadata = DeltaTableMetadata::new(
            Uuid::new_v4(),
            name.to_owned(),
            DeltaTableFormat::new("parquet".to_owned(), HashMap::new()),
            serde_json::to_string(&schema)?,
            vec![],
            HashMap::new(),
        );
        if !metadata.is_valid() {
            return Err(DeltaError::InvalidTable);
        }

        let table = DeltaTable {
            metadata,
            base_dir: format!("tables/{}", name),
            logs_dir: format!("tables/{}/_delta_log", name),
        };

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
        let schema: DeltaTableSchema = self.metadata.schema()?;
        let fields = schema.fields();
        let n_cols = fields.len();

        // Bad, should fix this
        let cols = (0..n_cols)
            .map(|i| {
                let s = Series::new(
                    &fields[i].name,
                    data.iter().map(|row| row[i]).collect::<Vec<&str>>(),
                );
                s.cast(&fields[i].typ.to_polars_type()).unwrap()
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

    fn log_file(idx: usize) -> String {
        format!("{:0>20}.json", idx)
    }
}
