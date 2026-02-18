import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pandas as pd
import os

# # Read just the metadata (very fast, doesn't load the data)
# parquet_file = pq.ParquetFile('/home/mule-external/sci-dig/arctic_toolkit/gbif/2025-11-26/arctic_occurences_2025-11-26.parquet')

# # Get schema
# # print(parquet_file.schema)

# # Or just get column names
# print(parquet_file.schema.names)

class CreateTestData:
    def __init__(self, occurrence_parquet: str,
                  dna_derived_parquet: str,
                    mof_parquet: str,
                    occ_occurrence_id_col_name: str,
                    dna_derived_occurrence_id_col_name: str, 
                    mof_occurrence_id_col_name: str,
                    data_output_dir: str):
        
        self.occurrence_parquet = occurrence_parquet
        self.dna_derived_parquet = dna_derived_parquet
        self.mof_parquet = mof_parquet
        self.occ_occurrence_id_col_name = occ_occurrence_id_col_name
        self.dna_derived_occurrence_id_col_name = dna_derived_occurrence_id_col_name
        self.mof_occurrence_id_col_name = mof_occurrence_id_col_name
        self.data_output_dir: str = data_output_dir
   

        # Save CSV subsets
        self.df_orchestrator()

    def df_orchestrator(self):
        """
        The orchestrator method for getting all the test dfs
        """
        # 1. Get occurrence csv subset for rows that have both dna_derived and measurment of fact as True
        occurrence_ids = self.get_occ_subset_with_dna_and_mof()

        # 2. Get DNA-derived csv subset for those occurrence ids
        self.filter_parquet_files_by_occ_ids(parquet_path=self.dna_derived_parquet,
                                             occ_ids=occurrence_ids,
                                             occ_col_name=self.dna_derived_occurrence_id_col_name,
                                             filename_suffix="dna_derived_test")
        
        # 3. Get Measurement of Fact csv subset for those occurrence ids
        self.filter_parquet_files_by_occ_ids(parquet_path=self.mof_parquet,
                                             occ_ids=occurrence_ids,
                                             occ_col_name=self.mof_occurrence_id_col_name,
                                             filename_suffix="mof_test")

    def get_occ_subset_with_dna_and_mof(self) -> list:
        """
        Get a subset dataframe (5000 rows) of the occurrence records 
        (from the occurrence parquet file) that have both measurement 
        of fact and dna derived records. This method returns a list of the 
        occurrence ids for the test df.
        """
        ###################################################
        parquet_file = pq.ParquetFile(self.occurrence_parquet)
        samples = []
        rows_found = 0

        print(f"Total row groups: {parquet_file.num_row_groups}")

        # Iterate through row groups to find True values for both dna_derived and has_mof
        for i in range(parquet_file.num_row_groups):
            # Read just the dna_derived column first (fast check)
            row_group = parquet_file.read_row_group(i, columns=['dna_derived', 'has_mof'])

            # Check if this row group has any True values for both
            both_true = pc.and_(row_group['dna_derived'], row_group['has_mof'])
            count_both_true = pc.sum(pc.cast(both_true, 'int64')).as_py()

            if count_both_true > 0:
                print(f"Row group {i} has {count_both_true} rows with both True")
                # Now read the full row group
                row_group_full = parquet_file.read_row_group(i)
                mask = pc.and_(row_group_full['dna_derived'], row_group['has_mof'])
                filtered = row_group_full.filter(mask)

                # Convert to pandas and sample randomly a small bit to diversify data
                df_filtered = filtered.to_pandas()
                sample_size = min(50, len(df_filtered)) # Take up to 10 rows
                df_random_sample = df_filtered.sample(n=sample_size, random_state=42)

                samples.append(df_random_sample)
                rows_found += sample_size

                if rows_found >= 5000:
                    break

        if samples:
            df_occurrences = pd.concat(samples).head(200)
            output_filepath = self.save_test_parquet_and_csv(df=df_occurrences, filename_suffix = 'occurrence_test')
            print(f"Occurrences df saved to {output_filepath}!")
        else:
            print("No rows found with dna_derived=True")

        return df_occurrences[self.occ_occurrence_id_col_name].tolist()

    def filter_parquet_files_by_occ_ids(self, parquet_path: str, occ_ids: list, occ_col_name: str, filename_suffix: str):
        """
        Filters a parquet file by a list of occurence ids based on the specified occurence id column
        name in the parquet file
        """
        table = pq.read_table(
            parquet_path,
            filters = [(occ_col_name, 'in', occ_ids)]
        )

        df = table.to_pandas()
        filepath = self.save_test_parquet_and_csv(df=df, filename_suffix=filename_suffix)
        print(f"Saved df to {filepath}!")

    def save_test_parquet_and_csv(self, df: pd.DataFrame, filename_suffix: str) -> str:
        """
        Takes a data frame and saves as a csv with specified filenmae_suffix
        appended to the first part of the directory name to create the full
        filename
        """
        dir_name = os.path.basename(os.path.normpath(self.data_output_dir))
        data_source = dir_name.split('_')[0]

        filename = f"{data_source}_{filename_suffix}.parquet"
        csv_filename = f"{data_source}_{filename_suffix}.csv"

        filepath = os.path.join(self.data_output_dir, filename)
        csv_filepath = os.path.join(self.data_output_dir, csv_filename)

        df.to_parquet(filepath, index=False, engine='pyarrow', compression='snappy')
        df.to_csv(csv_filepath, index=False)
        return filepath

    