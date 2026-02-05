import polars as pl
from pathlib import Path
from datetime import datetime
import hashlib

# TODO: Look into columns to drop (that aren't needed) - like lastParsed?
# TODO: Finish writing the pivot_mof_measurment_df function when mof cols are correct (GBIF issue filed)
# TODO: Load parquet file into ERDDAP and inspect.
# TODO: Add citation column with "GBIF.org (13 November 2025) GBIF Occurrence Download https://doi.org/10.15468/dl.eta6v7"

class GbifOccurrenceParser:

    DNA_FILE_OCCURRENCE_ID = "gbifid" # occurrence id in dna.txt file
    OCC_FILE_OCCURRENCE_ID = "gbifID" # occurrence id in occurrence.txt file
    MOF_FILE_OCCURRENCE_ID = "gbifid" # occurrence id for the mof.txt file
    
    # Standardized identifiers changed to
    SOURCE_ID = "source_id"
    OCCURRENCE_SOURCE_ID = "occurrence_source_id"

    # DNA fields to hash on besided gbifid
    PCR_PRIMER_FORWARD = "pcrprimerforward"
    PCR_PRIMER_REVERSE = "pcrprimerreverse"
    DNA_OCCURRENCEID = "occurrenceid" # Darwin Core created by data producer so may not be universally unique

    # MOF fields to has besides gbifid
    MOF_OCCURENCEID = "occurrenceid"
    MEASUREMENT_TYPE = "measurementtype"
    MEASUREMENT_VALUE = "measurementvalue"

    occ_non_str_schema_dtypes = { # Specified data types of columns in the occurrence records that aren't strings
        "modified": pl.Datetime,
        "individualCount": pl.Int64,
        "year": pl.UInt16,
        "month": pl.UInt8,
        "day": pl.UInt8,
        "decimalLatitude": pl.Float64,
        "decimalLongitude": pl.Float64,
        "coordinateUncertaintyInMeters": pl.Float64,
        "coordinatePrecision": pl.Float64,
        "dateIdentified": pl.Datetime,
        "lastInterpreted": pl.Datetime,
        "elevation": pl.Float64,
        "elevationAccuracy": pl.Float64,
        "depth": pl.Float64,
        "distanceFromCentroidInMeters": pl.Float64,
        # "hasCoordinate": pl.Boolean, # Did not work casting from str to boolean
        # "hasGeospatialIssues": pl.Boolean,
        "taxonKey": pl.Int64,
        "acceptedTaxonKey": pl.Int64,
        "kingdomKey": pl.Int64,
        "phylumKey": pl.Int64, 
        "classKey": pl.Int64,
        "orderKey": pl.Int64,
        "familyKey": pl.Int64, 
        "genusKey": pl.Int64,
        "subgenusKey": pl.Int64,
        "speciesKey": pl.Int64,
        "lastParsed": pl.Datetime,
        "lastCrawled": pl.Datetime
    }

    def __init__(self, gbif_download_dir: str):

        self.gbif_download_dir = Path(gbif_download_dir)
        self.occurrence_txt_file, self.dna_txt_file, self.mof_txt_file  = self.find_occurrence_txt_files()
        self.output_file_name = self.gbif_download_dir / f"{self.gbif_download_dir.name}.parquet"
        self.occurrence_df = self.transform_gbif_df()

    def transform_gbif_df(self) -> pl.DataFrame:
        """
        The orchestrator function to transform the GBIF data occurrence frame to the final 
        desired data frame. Also for dna_derived df to a parquet file and mof df to a parquet file. 
        """

        # 1. Load original df
        original_df = self.read_txt_file_to_df(txt_file=self.occurrence_txt_file)

        # Update column data types
        # Change data types for not string columns. Was running into polars column data types inferences.
        original_df = original_df.with_columns([
            pl.col(col_name).cast(dtype) for col_name, dtype in self.occ_non_str_schema_dtypes.items()
        ])

        # 2. Add a data_source column with "GBIF" as value
        gbif_added_df=self.add_column_and_val_to_df(df=original_df, column_name="data_source", value="gbif")

        # 3. Add dna_exists by joining with dna_df and checking if occurrence_id exists in it
        dna_df = self.read_txt_file_to_df(txt_file=self.dna_txt_file)
        final_dna_df = self.add_column_and_val_to_df(df=dna_df, column_name="data_source", value="gbif")
        occ_dna_updated_df = self.add_extension_exists_bool_to_occ_df(occ_df=gbif_added_df, ext_df=dna_df, bool_col_name="dna_derived", ext_file_occurid=self.DNA_FILE_OCCURRENCE_ID)

        # 4. has_mof by joining with mof_df and checking if occurence_id exists
        mof_df = self.read_txt_file_to_df(txt_file=self.mof_txt_file)
        final_mof_df = self.add_column_and_val_to_df(df=mof_df, column_name="data_source", value="gbif")
        occ_mof_updated_df = self.add_extension_exists_bool_to_occ_df(occ_df=occ_dna_updated_df, ext_df=final_mof_df, bool_col_name="has_mof", ext_file_occurid=self.MOF_FILE_OCCURRENCE_ID)

        # 5. Edit OCC file identifiers and save OCC to parquet file
        occ_source_id_col_updated = self.rename_gbif_id_to_occurrence_source_id(df=occ_mof_updated_df, col_to_rename=self.OCC_FILE_OCCURRENCE_ID, desired_col_name=self.SOURCE_ID)
        occ_file_name = self._construct_file_parquet_file_path(file_prefix="arctic_occurences")
        self.write_to_parquet(df=occ_source_id_col_updated, output_file_name=occ_file_name)

        # 6. Edit DNA derived identifiers and save DNA derived to parquet file
        dna_source_id_hashed = self.create_hashed_source_id(df=final_dna_df, cols_to_hash=[self.DNA_FILE_OCCURRENCE_ID, self.DNA_OCCURRENCEID, self.PCR_PRIMER_FORWARD, self.PCR_PRIMER_REVERSE])
        dna_occ_source_id_col_updated = self.rename_gbif_id_to_occurrence_source_id(df=dna_source_id_hashed, col_to_rename=self.DNA_FILE_OCCURRENCE_ID, desired_col_name=self.OCCURRENCE_SOURCE_ID)
        dna_file_name = self._construct_file_parquet_file_path(file_prefix="arctic_dna_derived")
        self.write_to_parquet(df=dna_occ_source_id_col_updated, output_file_name=dna_file_name)

        # 7.Edit MOF identifiers and save Measurement of Fact to parquet file
        mof_source_id_hashed = self.create_hashed_source_id(df=final_mof_df, cols_to_hash=[self.MOF_FILE_OCCURRENCE_ID, self.MOF_OCCURENCEID, self.MEASUREMENT_TYPE, self.MEASUREMENT_VALUE])
        mof_occ_source_id_col_updated = self.rename_gbif_id_to_occurrence_source_id(df=mof_source_id_hashed, col_to_rename=self.MOF_FILE_OCCURRENCE_ID, desired_col_name=self.OCCURRENCE_SOURCE_ID)
        mof_file_name = self._construct_file_parquet_file_path(file_prefix="arctic_mof")
        self.write_to_parquet(df=mof_occ_source_id_col_updated, output_file_name=mof_file_name)
      
        return  occ_source_id_col_updated

    def find_occurrence_txt_files(self) -> tuple:
        """
        Find the occurrenct.txt, the gbif_dnaderiveddata.txt, and 
        obis_extendedmeasurmentorfact.txt files in the specified 
        data directory
        """
        occurrence_txt_file = next(self.gbif_download_dir.rglob("occurrence.txt"), None)
        dna_txt_file =  next(self.gbif_download_dir.rglob("gbif_dnaderiveddata.txt"), None)
        mof_txt_file = next(self.gbif_download_dir.rglob("obis_extendedmeasurementorfact.txt"), None)

        return occurrence_txt_file, dna_txt_file, mof_txt_file
    
    def read_txt_file_to_df(self, txt_file: str) -> pl.DataFrame:
        """
        Uses polars to read the GBIF txt files file from GBIF
        into a data frame. Return lazy data frame. Will need to do 
        .collect() later to return whole df.
        """
        df = pl.scan_csv(txt_file, 
                         separator = "\t",
                         low_memory=False, # optional performance booster use more memory for speed
                         rechunk=True, # optional performance booster optimize memory
                         infer_schema_length=0, # will make all types strings
                         quote_char=None) # treat all quotes as literal characters
        
        return df
    
    def add_column_and_val_to_df(self, df: pl.DataFrame, column_name: str, value: str) -> pl.DataFrame:
        """
        Adds a new column and a value for a specified name and specified value
        to the lazy data frame.
        """
        print("Adding the data_source as GBIF...")
        df = df.with_columns([
            pl.lit(value).alias(column_name)
        ])

        return df
    
    def add_extension_exists_bool_to_occ_df(self, occ_df: pl.DataFrame, ext_df: pl.DataFrame, bool_col_name: bool, ext_file_occurid: str) -> pl.DataFrame:
        """
        Adds a specified column called something like dna_derived or has_mof with a True or False value if there is
        DNA data for the occurrence id or MOF for the occurrence id.
        bool_col_name is the name of the desired column to be a boolean like dna_derived or has_mof
        ext_file_occurid is the name of the column in the extension df for the occurrence id (should be self.DNA_FILE_OCCURRENCE_ID or self.MOF_FILE_OCCURRENCE_ID)
        """
        print("Adding if dna_exists")
        ext_updated_occ_df = occ_df.join(
            ext_df.select([ext_file_occurid]).unique().with_columns([
                pl.lit(True).alias(bool_col_name)
            ]),
            left_on=self.OCC_FILE_OCCURRENCE_ID,
            right_on=ext_file_occurid, 
            how="left"
        ).with_columns([
            pl.col(bool_col_name).fill_null(False)
        ])

        return ext_updated_occ_df

    def rename_gbif_id_to_occurrence_source_id(self, df: pl.DataFrame, col_to_rename: str, desired_col_name: str) -> pl.DataFrame:
        """
        Rename the gbifid to occurrence_source_id if dna_derived or mof, will just be
        source_id for occurrence 
        """
        return df.rename({col_to_rename: desired_col_name})

    def create_hashed_source_id(self, df: pl.DataFrame, cols_to_hash: list) -> pl.DataFrame:
        """
        Creates a hash based on the specified cols to hash their values.
        For dna_derived hash will be the gbif_id and the dna sequence
        For mof hash will be the gbifid, measurementType and measurementValue
        """
        def md5_python(s: pl.Series) -> pl.Series:
        # This function takes a Series of concatenated strings and hashes each one
            return s.map_elements(
                lambda val: hashlib.md5(val.encode()).hexdigest() if val is not None else None,
                return_dtype=pl.Utf8
            )

        return df.with_columns(
            pl.concat_str(
                [pl.col(c).cast(pl.Utf8).fill_null("") for c in cols_to_hash],
                separator="|"
            )
            .pipe(md5_python) # Pipe the concatenated string into our hasher
            .alias(self.SOURCE_ID)
        )

    def _construct_file_parquet_file_path(self, file_prefix: str) -> str:
        """"
        Constructs the file path to svae the data to, given the
        data directory and the file_name
        """
        today = datetime.now().strftime("%Y-%m-%d")
        file_path = self.gbif_download_dir / f"{file_prefix}_{today}.parquet"
        return str(file_path)
    
    def write_to_parquet(self, df: pl.DataFrame, output_file_name: str):
        """
        Writes the final occurrence df into a parquet file.
        sink_parquet streams data to disk in chunks (much more
        memory efficent)
        """
        
        df.sink_parquet(
            path=output_file_name,
            compression = "zstd",
            compression_level=3,
            statistics=True,
            row_group_size=1_000_000
        )