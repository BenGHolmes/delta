use delta::{DeltaError, DeltaTable};

fn main() -> Result<(), DeltaError> {
    let table = DeltaTable::create_table("my-table", vec![("foo", "int"), ("bar", "TEXT")])?;
    Ok(())
}
