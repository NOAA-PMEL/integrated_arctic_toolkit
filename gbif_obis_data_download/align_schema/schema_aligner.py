import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
from datetime import datetime
from arctic_postgres.schema_comparison.compare_schemas import SchemaComparer

class DwcSchemaAligner:

    DATA_PARQUET_DIR = '/home/mule-external/sci-dig/arctic_toolkit'
    GBIF_DATA_SUBDIR = 'gbif'
    OBIS_DATA_SUBDIR = 'obis'
    MAIN_DWC_TERMS = pd.read_csv('/home/users/zalmanek/arctic_postgres/gbif_obis_data_download/align_schema/all_dwc_vertical.csv', header=None)[0].tolist()

    OCCURRENCES = "occ"
    DNA_DERIVED = "dna_derived"
    MOF = "mof"

    
    def __init__(self):
        self.parquet_files = self.get_parquet_data_files()
        self.occ_schema_compare_df, self.dna_derived_schema_compare_df, self.mof_schema_compare_df = self.compare_schemas()
        self.rename_col_map = self.create_rename_master_rename_dict()

    def create_rename_master_rename_dict(self):
        """
        Get a master dictionary of all the columns that will need to be renamed
        """
        master_rename_map = {}
        tasks = [
            (self.parquet_files.get(self.GBIF_DATA_SUBDIR).get(self.OCCURRENCES), self.occ_schema_compare_df, self.GBIF_DATA_SUBDIR),
            (self.parquet_files.get(self.OBIS_DATA_SUBDIR).get(self.OCCURRENCES), self.occ_schema_compare_df, self.OBIS_DATA_SUBDIR),
            (self.parquet_files.get(self.GBIF_DATA_SUBDIR).get(self.DNA_DERIVED), self.dna_derived_schema_compare_df, self.GBIF_DATA_SUBDIR),
            (self.parquet_files.get(self.OBIS_DATA_SUBDIR).get(self.DNA_DERIVED), self.dna_derived_schema_compare_df, self.OBIS_DATA_SUBDIR),
            (self.parquet_files.get(self.GBIF_DATA_SUBDIR).get(self.MOF), self.mof_schema_compare_df, self.GBIF_DATA_SUBDIR),
            (self.parquet_files.get(self.OBIS_DATA_SUBDIR).get(self.MOF), self.mof_schema_compare_df, self.OBIS_DATA_SUBDIR)
        ]

        for path, df, db in tasks:
            renames = self.get_col_rename_dict(parquet_path=path, schema_compare_df=df, database=db)
            master_rename_map.update(renames)
        
        print(f"Total unique column renames identified: {len(master_rename_map)}")

        return master_rename_map

    def get_latest_data_directories(self) -> dict:
        """
        Get the latest date directories for each OBIS and GBIF data directories. 
        Returns a dictionary like {obis: latest_date_dir, gbif: latest_date_dir}
        """

        base_path = Path(self.DATA_PARQUET_DIR)
        latest_directories = {}

        for subdir in [self.GBIF_DATA_SUBDIR, self.OBIS_DATA_SUBDIR]:
            subdir_path = base_path / subdir

            if not subdir_path.exists():
                raise ValueError(f"{subdir} does not exist in the {base_path} - please check in order to get parquet files")
            
            date_dirs = []
            for d in subdir_path.iterdir():
                if d.is_dir():
                    date_str = d.name.replace('_', '-')

                    try:
                        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                        date_dirs.append((date_obj, d))
                    except ValueError:
                        print(f"Could not parse date from {d.name}!")

            if not date_dirs:
                raise ValueError(f"No valid date directories found in {subdir}")
            
            # Sort and get the latest directory
            date_dirs.sort(key=lambda x: x[0], reverse=True)
            latest_dir = date_dirs[0][1]
            latest_directories[subdir] = latest_dir

        return latest_directories
    
    def get_parquet_data_files(self) -> dict:
        """
        Gets the latest parquet files for both OBIS and GBIF for occurrences, dna_derived and mof.
        Returns a dictionary like {obis: {occ: dir, dna_derived: dir, mof: dir}, gbif: {etc.}}
        """

        latest_data_dirs = self.get_latest_data_directories()
        
        parquet_files = {}
        for repository, dir in latest_data_dirs.items():
            dna_parquet_file = list(dir.glob('*dna_derived*.parquet'))[0]
            mof_parquet_file = list(dir.glob('*mof*.parquet'))[0]
            occ_parquet_file = list(dir.glob('*occur*.parquet'))[0]
            parquet_files[repository] = {self.OCCURRENCES: occ_parquet_file,
                                  self.DNA_DERIVED: dna_parquet_file,
                                  self.MOF: mof_parquet_file}
            
        return parquet_files
    
    def compare_schemas(self):
        """
        Compares the schemas for dna_derived, mof and occurences. 
        Saves three schema comparison csvs to "/home/users/zalmanek/arctic_postgres/schema_comparison/comparison_csvs"
        Returns dictionaries of those csvs each for occurrences, dna_derived, and mof
        """
        save_dir = Path('/home/users/zalmanek/arctic_postgres/schema_comparison/comparison_csvs')
        
        dna_derived_comparer = SchemaComparer(obis_parquet=self.parquet_files.get(self.OBIS_DATA_SUBDIR).get(self.DNA_DERIVED),
                                              gbif_parquet=self.parquet_files.get(self.GBIF_DATA_SUBDIR).get(self.DNA_DERIVED),
                                              output_csv_path=save_dir / "dna_schema_comparison.csv")
        
        mof_comparer = SchemaComparer(obis_parquet=self.parquet_files.get(self.OBIS_DATA_SUBDIR).get(self.MOF),
                                      gbif_parquet=self.parquet_files.get(self.GBIF_DATA_SUBDIR).get(self.MOF),
                                      output_csv_path=save_dir / "mof_schema_comparison.csv")
        
        occ_comparer = SchemaComparer(obis_parquet=self.parquet_files.get(self.OBIS_DATA_SUBDIR).get(self.OCCURRENCES),
                                      gbif_parquet=self.parquet_files.get(self.GBIF_DATA_SUBDIR).get(self.OCCURRENCES),
                                      output_csv_path=save_dir / "occ_schema_comparison.csv")
        
        return occ_comparer.mapping_df, dna_derived_comparer.mapping_df, mof_comparer.mapping_df

    def get_col_rename_dict(self, parquet_path: Path, schema_compare_df: pd.DataFrame, database: str):
        """
        Compares the columns in the parquet file to the schema comparions diciontary and the main
        Darwin Core terms and updates the columns in the parquet file to match the Darwin Core
        Term if it exists in that list, if not, it will use the GBIF term if there is a match between
        GBIF and OBIS, if there is no match, will just use whatever the term is for that parquet file.
        """
        parquet_file = pq.ParquetFile(str(parquet_path))
        schema = parquet_file.schema_arrow

        # Compare to lowercase version of offfical dwc term
        valid_columns_lower = {col.lower(): col for col in self.MAIN_DWC_TERMS}

        # Dict to store old_name: new_name
        column_renames = {}

        source_col = 'gbif_column' if database == self.GBIF_DATA_SUBDIR else 'obis_column'

        for idx, row in schema_compare_df.iterrows():
            
            current_col = row[source_col]
            if pd.isna(current_col):
                continue
            
            match_type = row['match_type']
            normalized_col = row['normalized_name']
            gbif_col = row['gbif_column']
            obis_col = row['obis_column']

            new_name = None
            
            if match_type == 'exact':
                # Check if normalized col name exists in the dwc list.lower()
                if normalized_col.lower() in valid_columns_lower:
                    # Use the original case from the list
                    new_name = valid_columns_lower[normalized_col.lower()]
                else:
                    # Use the GBIF name
                    new_name = gbif_col

            if match_type == 'fuzzy':
                # Check normalized col, gbif_col and obis_col against the list
                for candidate in [normalized_col, gbif_col, obis_col]:
                    if candidate and candidate.lower() in valid_columns_lower:
                        new_name = valid_columns_lower[candidate.lower()]
                        break
                
                # If none found in list, use the gbif col name
                if new_name is None:
                    new_name = self.change_cols_with_special_chars(gbif_col)

            elif match_type == 'gbif_only':
                new_name = self.change_cols_with_special_chars(gbif_col)

            elif match_type == 'obis_only':
                new_name = self.change_cols_with_special_chars(obis_col)

            # Only add to renames if the name actually changes
            if new_name and new_name != current_col:
                column_renames[current_col] = new_name      

        # If no renames needed, we are done
        if not column_renames:
            print("No column renames needed.")
            return column_renames

        return column_renames
    

    def change_cols_with_special_chars(self, col_val:str):
        """"Remove spaces from col values and """
        updated_col_val = col_val.replace(' ', '_').replace(')', '').replace('(', '')

        return updated_col_val
     
