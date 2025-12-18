from pathlib import Path
import polars as pl


base_path = "../data/WA_tax/"
data_path = Path(base_path)

sources = (p.name for p in data_path.rglob("*.xlsx"))

for source in sources:

    # first 10 or 11 lines should be skipped, but depends on file
    # Also some have a blank line between header row & data.
    # set the column fields & read in data only from file.



    # extract year from source name
    year = int(source.split('.')[0][-4:])


    # data starts at row 13: years: 2012, 2013 - 2017
    # data starts at row 11: 2018 - 2024

    if year >= 2018:
        skip_row_value = 10 # data starts at row 11
    else:
        skip_row_value = 12 # data starts at row 13

    df = pl.read_excel(source=base_path+source,
                       has_header=False,
                       read_csv_options={ "skip_rows": skip_row_value})

    # rename columns
    df = df.rename(
        { 'column_1': 'NAICS',
         'column_2': 'tax_type',
         'column_3': 'line_code',
         'column_4': 'line_code_description',
         'column_5': 'reporting_units',
         'column_6': 'gross',
         'column_7': 'taxable',
         'column_8': 'taxdue' })

    # add a column with the year to the data
    df = df.with_columns(pl.lit(year).alias('year'))

    # let's keep NAICS code a string
    df = df.cast({'NAICS': pl.String})


    # to do - write parquet file out

