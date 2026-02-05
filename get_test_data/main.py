from arctic_postgres.get_test_data.get_test_data_subset import CreateTestData
import pyarrow.parquet as pq

# For GBIF:
gbif_test_data_creator = CreateTestData(
    occurrence_parquet='/home/mule-external/sci-dig/arctic_toolkit/gbif/2026-01-26/arctic_occurences_2026-02-03.parquet',
    dna_derived_parquet='/home/mule-external/sci-dig/arctic_toolkit/gbif/2026-01-26/arctic_dna_derived_2026-02-03.parquet',
    mof_parquet='/home/mule-external/sci-dig/arctic_toolkit/gbif/2026-01-26/arctic_mof_2026-02-03.parquet',
    occ_occurrence_id_col_name='source_id',
    dna_derived_occurrence_id_col_name='occurrence_source_id',
    mof_occurrence_id_col_name='occurrence_source_id',
    data_output_dir='/home/users/zalmanek/arctic_postgres/get_test_data/test_data/gbif_test_data',
)


# parquet_file = pq.ParquetFile('/home/mule-external/sci-dig/arctic_toolkit/gbif/2026-01-26/arctic_occurences_2026-01-27.parquet')

# # # Get schema
# # # print(parquet_file.schema)

# # # Or just get column names
# print(parquet_file.schema.names)

obis_test_data_creator = CreateTestData(
    occurrence_parquet='/home/mule-external/sci-dig/arctic_toolkit/obis/2026_02_03/arctic_occurrences_2026-02-03.parquet',
    dna_derived_parquet='/home/mule-external/sci-dig/arctic_toolkit/obis/2026_02_03/arctic_dna_derived_2026-02-03.parquet',
    mof_parquet='/home/mule-external/sci-dig/arctic_toolkit/obis/2026_02_03/arctic_mof_2026-02-03.parquet',
    occ_occurrence_id_col_name='source_id',
    dna_derived_occurrence_id_col_name='occurrence_source_id',
    mof_occurrence_id_col_name='occurrence_source_id',
    data_output_dir='/home/users/zalmanek/arctic_postgres/get_test_data/test_data/obis_test_data',
)