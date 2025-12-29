from pathlib import Path

import duckdb
import polars as pl



def process_sales_tax_data(source_folder: str = "../data/WA_tax/"):
    """
    Reads excel files for WA state sales taxes
    Saves to parquet
    """

    data_path = Path(source_folder)
    sources = (p.name for p in data_path.rglob("*.xlsx"))

    for source in sources:

        # extract year from source name
        year = int(source.split('.')[0][-4:])

        if year in (2018, 2019, 2022, 2023, 2024):
            skip_row_value = 10 # data starts at row 11
        elif year in (2020, 2021):
            skip_row_value = 11 # data starts at row 12
        else:
            skip_row_value = 12 # data starts at row 13

        df = pl.read_excel(source=source_folder+source,
                        has_header=False,
                        read_options={ "skip_rows": skip_row_value})

        df = df.rename(
            { 'column_1': 'NAICS',
            'column_2': 'tax_type',
            'column_3': 'line_code',
            'column_4': 'line_code_description',
            'column_5': 'reporting_units',
            'column_6': 'gross',
            'column_7': 'taxable',
            'column_8': 'taxdue' })

        df = df.with_columns(pl.lit(year).alias('year'))

        # There are entries with "D" to indicate there are insufficient data 
        # to aggregate the reporting. Let's replace these & set the types.
        cols_for_replace = [
            'reporting_units', 'gross', 'taxable', 'taxdue'
            ]
        
        df = df.with_columns(
            pl.col(col).replace({'D':None}).alias(col) 
            for col in cols_for_replace
        )

        # let's set NAICS code as a string, given how we'll use it
        # Money amounts in the file are rounded to the nearest dollar (no cents)
        df = df.cast({'NAICS': pl.String,
                      'reporting_units': pl.Int64,
                      'gross': pl.Int64,
                      'taxable': pl.Int64,
                      'taxdue': pl.Int64
                      })

        df.write_parquet(file=f'../data/WA_tax/wa_sales_tax_{year}.parquet',
                        compression='snappy')


if __name__=="__main__":
    process_sales_tax_data()