# Let's use polars to process data ready for duckdb.
import logging
import polars as pl


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Q... there's a blank row in the source data, does polars deal with this automatically?

for year in [2012, 2017, 2022]:
    fpath = f'../data/NAICS/2-6_digit_{year}_Codes.xlsx'
    if year == 2012:
        fpath = f'../data/NAICS/2-6_digit_{year}_Codes.xls'    

    logger.info(f'Working on year {year}')
    df = pl.read_excel(source=fpath)
    df = df.rename({f'{year} NAICS US   Code':'code', f'{year} NAICS US Title':'description'})

    df = df.select(
        pl.col('code').cast(pl.String).str.pad_end(6,'0'),
        pl.col('description').str.strip_chars(),
        pl.lit(year).alias('year'),
    ).unique(maintain_order=True)

    # went from 2125 -> 1603 rows
    fname_output = f'../data/NAICS/naics_codes_{year}.parquet'
    df.write_parquet(file=fname_output,
                 compression='snappy')
    logger.info(f"Wrote {fname_output}")
