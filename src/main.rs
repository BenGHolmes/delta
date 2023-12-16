use delta::{error::DeltaError, table::DeltaTable};

fn main() -> Result<(), DeltaError> {
    let table = DeltaTable::create_table("my-table", vec![("foo", "int"), ("bar", "TEXT")])?;

    table.insert(vec![
        vec!["1", "test row"],
        vec!["2", "another test row"],
        vec!["3", "the final row"],
    ])?;

    let table = DeltaTable::read_table("my-table")?;

    table.insert(vec![vec!["4", "second insert"]])?;

    table.delete("bar = 'test row' OR foo = 2")?; // Expect to delete 1 and 2

    Ok(())
}
