import polars as pl
import pandas as pd
from deltalake import DeltaTable, write_deltalake

dt = DeltaTable("tables/my-table")
df = pl.DataFrame(dt.to_pyarrow_table())

print(df)
