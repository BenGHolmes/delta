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
    println!("FIRST INSERT\n{}", df);

    table.insert(vec![vec!["4", "second insert"]])?;
    let df = LazyFrame::scan_parquet(
        "tables/my-table/00000000000000000001.parquet",
        Default::default(),
    )?
    .collect()?;
    println!("SECOND INSERT\n{}", df);

    table.delete("bar = 'test row'")?; // Expect to delete 1 and 2
    let df = LazyFrame::scan_parquet(
        "tables/my-table/00000000000000000002.parquet",
        Default::default(),
    )?
    .collect()?;
    println!("DELETE\n{}", df);
    Ok(())
}
