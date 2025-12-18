# Let's use polars to process data ready for duckdb.
import polars as pl

df = pl.read_excel(source='../data/NAICS/NAICS_codes_2022.xlsx')
df = df.rename({'2022 NAICS US   Code':'code', '2022 NAICS US Title':'description'})

# polars detected NAICS codes as integers
# we have to convert to int & right-pad with 0s to create the 6 digit code
# and drop any duplicates created
df = df.select(
        pl.col('code').cast(pl.String).str.pad_end(6,'0'),
        pl.col('description').str.strip_chars(),
    ).unique(maintain_order=True)

# went from 2125 -> 1603 rows
df.write_parquet(file='../data/NAICS/naics_codes_2022.parquet',
                 compression='snappy')

