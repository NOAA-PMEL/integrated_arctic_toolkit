from database import engine, Base
from models import create_tables, mof, dna_derived, occurrence
from dotenv import load_dotenv
from align_schema.schema_aligner import DwcSchemaAligner
from sqlalchemy import text
import os
import pyarrow.parquet as pq
import polars as pl
import io

BATCH_SIZE = 100000

schema_aligner = DwcSchemaAligner()
column_rename_dict = schema_aligner.rename_col_map
# Replace handwritten dictionary with this when changing to full database
# parquet_file_dict = schema_aligner.parquet_files
parquet_file_dict = {
    'obis': {
        'occ': '/home/users/zalmanek/integrated_arctic_toolkit/get_test_data/test_data/obis_test_data/obis_occurrence_test.parquet',
        'dna_derived': '/home/users/zalmanek/integrated_arctic_toolkit/get_test_data/test_data/obis_test_data/obis_dna_derived_test.parquet',
        'mof': '/home/users/zalmanek/integrated_arctic_toolkit/get_test_data/test_data/obis_test_data/obis_mof_test.parquet'
    },
    'gbif': {
        'occ': '/home/users/zalmanek/integrated_arctic_toolkit/get_test_data/test_data/gbif_test_data/gbif_occurrence_test.parquet',
        'dna_derived': '/home/users/zalmanek/integrated_arctic_toolkit/get_test_data/test_data/gbif_test_data/gbif_dna_derived_test.parquet',
        'mof': '/home/users/zalmanek/integrated_arctic_toolkit/get_test_data/test_data/gbif_test_data/gbif_mof_test.parquet'
    }
}

DATETIME_COLUMNS = [
    'lastInterpreted',
    'dateIdentified',
    'modified',
    'lastCrawled',
    'lastParsed'
]

INT_COLUMNS = ['aphiaid', 
               'organismQuantity', 
               'individualCount', 
               'year', 
               'month', 
               'day',
               'phylumKey',
               'classKey',
               'superfamilyKey',
               'orderKey',
               'familyKey',
               'genusKey',
               'speciesKey',
               ]

# COLUMNS_TO_DROP = ['geometry'] # a binary column and I think added in because of my q

def create_the_tables(): 
    # Base.metadata.drop_all(bind=engine)
    create_tables()
    print("Tables created!")

def transorm_df(df: pl):

    # 1. Rename columns
    # only rename columns that exist in the dataframe
    rename_map = {k: v for k, v in column_rename_dict.items() if k in df.columns}
    if rename_map:
        df = df.rename(rename_map)

    # 2. Convert boolean columns to integers (false->0 True->1)
    bool_columns = [col for col in df.columns if df[col].dtype == pl.Boolean]
    if bool_columns:
        print(f"    Converting {len(bool_columns)} boolean column(s) to int: {bool_columns}")
        df = df.with_columns([
            pl.col(col).cast(pl.Int32).alias(col) for col in bool_columns
        ])

    # 3. Handle nested data (List) and Binary data (for Obis's 'areas', 'missing', 'invalid', and 'flags' columns + 'geometry' column which is binary)
    list_cols = [col for col, dtype in df.schema.items() if isinstance(dtype, (pl.List, pl.Array))]
    if list_cols:
        print(f"    Converting list columns to JSON: {list_cols}")
        df = df.with_columns([
            pl.col(col)
            .list.eval(pl.element().cast(pl.String)) # Cast inner elements to String
            .list.join(", ")                         # Now join is safe
            .alias(col) 
            for col in list_cols
        ])

    # 4. Handle Binary data (Geometry)
    binary_cols = [col for col, dtype in df.schema.items() if isinstance(dtype, pl.Binary)]
    if binary_cols:
        print(f"    Converting {len(binary_cols)} binary columns to hex: {binary_cols}")
        df = df.with_columns([
            # .bin.encode("hex") converts raw bytes to a hex string
            pl.col(col).bin.encode("hex") for col in binary_cols
        ])

    # 5. Parse out eventDate into two separate cols: startEventDate and endEventDate so can make datetime for search
    if 'eventDate' in df.columns:
        df = split_dwc_event_date(df=df)

    # 6. Parse datetime columns with varying ISO formats (minus eventDate (dealt with separately above))
    for col in DATETIME_COLUMNS:
        if col in df.columns:
            if df.schema[col] == pl.Datetime:
                continue
            else:
                print(f"    Parsing datetime column: {col}")
                # Polars can parse multiple datetime formats automaticaaly
                # Handle nulls and parse errors
                df = df.with_columns([
                    pl.col(col).str.to_datetime(
                        format=None, #Auto detect format
                        strict=False # Don't fail on parse errors
                    ).alias(col)
                ])

    # 7 . REmove backticks from column names (e.g. ones that start with numbers)
    df = df.rename(({c: c.replace("`", "") for c in df.columns}))

    # 8. Cast int columns appropariatley
    int_cols = [col for col in df.columns if col in INT_COLUMNS]
    if int_cols:
        df = df.with_columns([
            pl.col(c).cast(pl.Int64, strict=False) for c in int_cols
        ])

    return df

def split_dwc_event_date(df: pl.DataFrame) -> pl.DataFrame:
    """
    Splits a DarwinCore eventDate interval into start and end datetime columns.
    Handles both 'YYYY-MM-DD' and 'YYYY-MM-DDTHH:MM:SS' formats.
    """

    event_date_col = "eventDate"
    
    return (
        df.with_columns(
            # 1. Split the interval by the "/" character into a struct
            # If no "/" exists, the second field will be null
            pl.col(event_date_col).str.split_exact("/", 1)
            .struct.rename_fields(["start_raw", "end_raw"])
            .alias("_split")
        )
        .unnest("_split")
        .with_columns(
            # 2. Convert to datetime. 
            # 'strict=False' allows Polars to try parsing ISO8601 automatically.
            # If the end date is null (single date event), we fill it with the start date.
            pl.col("start_raw").str.to_datetime(strict=False).alias("startEventDate"),
            pl.col("end_raw").str.to_datetime(strict=False).alias("endEventDate")
        )
        .with_columns(
            # 3. Logic: If endEventDate is null, it was a single point in time, not a range.
            # In DwC, a single date means the start and end are the same day.
            pl.col("endEventDate").fill_null(pl.col("startEventDate"))
        )
        .drop(["start_raw", "end_raw"])
    )

def load_parquet_streaming(file_path, table_name):
    """
    Load a parquet file into PostgresSQL using Polars streaming
    Memory-efficient for very large files
    """
    print(f"\n  Loading: {file_path}")
    print(f"    Target table: {table_name}")

    # get total row count
    row_count = pl.scan_parquet(file_path).select(pl.len()).collect().item()
    print(f"    Total rows in file: {row_count}")

    total_loaded = 0
    batch_num = 0

    # lazy-API for memory-efficient streaming
    lazy_df = pl.scan_parquet(file_path)

    # Process in batches
    for batch_df in lazy_df.collect().iter_slices(BATCH_SIZE):
        batch_num += 1

        # Apply transformations
        if batch_num == 1:
            print(f"    Applying transformations...")

        batch_df = transorm_df(batch_df)

        # Get column names (after transformations)
        columns = batch_df.columns
        columns_str = ', '.join([f'"{c}"' for c in columns])

        # Convert to CSV in memory
        csv_buffer = io.StringIO()
        batch_df.write_csv(csv_buffer, include_header=False, null_value='\\N', separator='\t')
        csv_buffer.seek(0)

        # Insert using PostgreSQL COPY
        with engine.begin() as conn:
            raw_conn = conn.connection
            cursor = raw_conn.cursor()

            copy_sql = f"""
                COPY {table_name} ({columns_str}) 
                FROM STDIN 
                WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N');
            """

            try:
                cursor.copy_expert(sql=copy_sql, file=csv_buffer)
                raw_conn.commit()
            except Exception as e:
                raw_conn.rollback()
                print(f"\n  X Error in batch {batch_num}: {e}")
                print(f" Columns: {columns}")
                print(f" Sample data:\n{batch_df.head(2)}")

        total_loaded += len(batch_df)

        # Progress update
        progress = (total_loaded / row_count) * 100
        print(f"    Progress: {total_loaded:,} / {row_count:,} rows ({progress:.1%})", end='\r')

    print(f"\n  ✅ Loaded {total_loaded:,} rows from {os.path.basename(file_path)}")
    return total_loaded

def verify_data():
    """Verify data was loaded correctly"""
    print("Verifying data")

    with engine.connect() as conn:
        # Get all tables
        result = conn.execute(text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
        """))

        tables = [row[0] for row in result]

        if not tables:
            print(" ⚠️ No tables found in database!")
            return
        
        print(f"\n Found {len(tables)} table(s):\n")

        for table in tables:
            count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = count_result.scalar()

            # Get column info
            columns_result = conn.execute(text(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table}'
            ORDER BY ordinal_position
            LIMIT 10
            """))
            columns_info = columns_result.fetchall()

            # Get sample row
            sample_result = conn.execute(text(f"SELECT * FROM {table} LIMIT 1"))
            sample = sample_result.fetchone()

            print(f"    {table} ")
            print(f"    Rows: {count:,}")
            print(f"    Columns (first 10):")
            for col_name, col_type in columns_info:
                print(f"        - {col_name}: {col_type}")
            if sample:
                sample_str = str(sample[:5]) + "..." if len(sample) > 5 else str(sample)
                print(f"        Sample: {sample_str}")
            print()

def main():

    # 1. Create tables
    create_the_tables()

    # 2. Fill tables
    for paths_to_pq_files in parquet_file_dict.values():
        for table_type, filepath in paths_to_pq_files.items():
            if table_type == 'occ':
                load_parquet_streaming(file_path=filepath, table_name="occurrence")
            elif table_type == "dna_derived":
                load_parquet_streaming(file_path=filepath, table_name="dna_derived")
            elif table_type == "mof":
                load_parquet_streaming(file_path=filepath, table_name="mof")

    # 3. Verify data
    verify_data()
    

if __name__ == "__main__":
    main()