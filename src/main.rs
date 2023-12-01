use delta::{DeltaError, DeltaTable};
use polars::prelude::*;

fn main() -> Result<(), DeltaError> {
    let table = DeltaTable::create_table("my-table", vec![("foo", "int"), ("bar", "TEXT")])?;
    table.insert(vec![
        vec!["1", "test row"],
        vec!["2", "another test row"],
        vec!["3", "the final row"],
    ])?;

    let df = LazyFrame::scan_parquet(
        "tables/my-table/00000000000000000000.parquet",
        Default::default(),
    )?
    .collect()?;

    println!("{}", df);

    Ok(())
}
