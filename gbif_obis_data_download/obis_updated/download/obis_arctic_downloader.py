import duckdb
from pathlib import Path
from datetime import datetime

class ObisArcticDownloader:

    ARCTIC_POLYGON = "POLYGON((-180 60, 180 60, 180 90, -180 90, -180 60))"
    DNA_DERIVED_EXTENSION = "http://rs.gbif.org/terms/1.0/DNADerivedData"
    MOF_EXTENSION = "http://rs.iobis.org/obis/terms/ExtendedMeasurementOrFact"
    AWS_S3_PATH = "s3://obis-open-data/occurrence/*.parquet"

    def __init__(self, data_dir: str):
        
        self.data_dir = Path(data_dir) # The directory to save the data to.

    def _query_obis_aws(self, query: str):
        """
        Executes a query to Obis's AWS
        """
        # connect to DuckDB
        con = duckdb.connect()
        print("Setting up DuckDB extensions...")
        
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL spatial; LOAD spatial")

        # Configure AWS access (no credentials needed for public data)
        con.execute("SET s3_region='us-east-1';")
        con.execute("SET s3_url_style='path';")

        print("Querying OBIS Arctic data from AWS...")

        con.execute(query)

    def _construct_file_parquet_file_path(self, file_prefix: str) -> str:
        """"
        Constructs the file path to svae the data to, given the
        data directory and the file_name
        """
        today = datetime.now().strftime("%Y-%m-%d")
        file_path = self.data_dir / f"{file_prefix}_{today}.parquet"
        return str(file_path)

    def get_obis_arctic_occurrences(self):
        """
        Gets the occurrence data from obis and saves 
        as a parquet file in the specified data_dir
        """
        file_path = self._construct_file_parquet_file_path(file_prefix="arctic_occurrences")

        occurrence_query = f"""
        COPY (
            SELECT _id AS source_id,
                dataset_id,
                interpreted.*,
                missing,
                invalid,
                flags,
                dropped,
                absence,
                geometry,
                'obis' AS data_source,
                CASE
                    WHEN TRY_CAST(extensions."{self.DNA_DERIVED_EXTENSION}" AS VARCHAR) IS NOT NULL
                    AND len(extensions['{self.DNA_DERIVED_EXTENSION}']) > 0
                        THEN TRUE
                        ELSE FALSE
                END AS dna_derived,
                CASE
                    WHEN extensions['{self.MOF_EXTENSION}'] IS NOT NULL 
                    AND len(extensions['{self.MOF_EXTENSION}']) > 0
                        THEN TRUE
                        ELSE FALSE
                END AS has_mof
            FROM read_parquet('{self.AWS_S3_PATH}',
                union_by_name=True,
                hive_partitioning=false)
            WHERE ST_Within(
                geometry,
                ST_GeomFromText('{self.ARCTIC_POLYGON}')
                )
            ) TO '{file_path}' (FORMAT PARQUET);
        """

        # Execute query
        self._query_obis_aws(query=occurrence_query)

    def get_obis_dna_derived(self):
        """
        Gets the DNA derived data from obis and saves 
        as a parquet file in the specified data dir
        """
        file_path = self._construct_file_parquet_file_path(file_prefix="arctic_dna_derived")
        
        dna_query = f"""
            COPY ( 
                SELECT 
                    _id AS source_id,
                    * EXCLUDE (_occurrence_id),
                    _occurrence_id AS occurrence_source_id,
                    'obis' AS data_source
                FROM (
                    SELECT 
                        UNNEST(extensions['{self.DNA_DERIVED_EXTENSION}'], recursive := true)
                    FROM read_parquet('{self.AWS_S3_PATH}')
                    WHERE ST_Within(
                        geometry,
                        ST_GeomFromText('{self.ARCTIC_POLYGON}')
                    )
                    AND extensions['{self.DNA_DERIVED_EXTENSION}'] IS NOT NULL
                    AND len(extensions['{self.DNA_DERIVED_EXTENSION}']) > 0
                )
            ) TO '{file_path}' (FORMAT PARQUET);
        """
        
        # Execute query
        self._query_obis_aws(query=dna_query)

    def get_obis_mof(self):
        """
        Gets the Measurement of Fact data from obis and
        saves as a parquet file in the specified data dir
        """
        file_path = self._construct_file_parquet_file_path(file_prefix="arctic_mof")

        mof_query = f"""
            COPY ( 
                SELECT 
                    _id AS source_id,
                    * EXCLUDE (_occurrence_id),
                    _occurrence_id AS occurrence_source_id,
                    'obis' AS data_source
                FROM (
                    SELECT 
                        UNNEST(extensions['{self.MOF_EXTENSION}'], recursive := true)
                    FROM read_parquet('{self.AWS_S3_PATH}')
                    WHERE ST_Within(
                        geometry,
                        ST_GeomFromText('{self.ARCTIC_POLYGON}')
                    )
                    AND extensions['{self.MOF_EXTENSION}'] IS NOT NULL
                    AND len(extensions['{self.MOF_EXTENSION}']) > 0
                )
            ) TO '{file_path}' (FORMAT PARQUET);
        """
        
        self._query_obis_aws(query=mof_query)
