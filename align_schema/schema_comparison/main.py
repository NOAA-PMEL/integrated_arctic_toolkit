from arctic_postgres.schema_comparison.compare_schemas import SchemaComparer


# Occurrences
occ_comparer = SchemaComparer(obis_parquet='/home/mule-external/sci-dig/arctic_toolkit/obis/2026_01_26/arctic_occurrences_2026-01-26.parquet',
                              gbif_parquet='/home/mule-external/sci-dig/arctic_toolkit/gbif/2026-01-26/arctic_occurences_2026-01-27.parquet',
                              output_csv_path='/home/users/zalmanek/arctic_postgres/schema_comparison/comparison_csvs/occ_schema_comparison.csv')


dna_derived_comparer = SchemaComparer(obis_parquet='/home/mule-external/sci-dig/arctic_toolkit/obis/2026_01_26/arctic_dna_derived_2026-01-26.parquet',
                                      gbif_parquet='/home/mule-external/sci-dig/arctic_toolkit/gbif/2026-01-26/arctic_dna_derived_2026-01-27.parquet',
                                      output_csv_path='/home/users/zalmanek/arctic_postgres/schema_comparison/comparison_csvs/dna_schema_comparison.csv')

mof_comparer = SchemaComparer(obis_parquet='/home/mule-external/sci-dig/arctic_toolkit/obis/2026_01_26/arctic_mof_2026-01-26.parquet',
                              gbif_parquet='/home/mule-external/sci-dig/arctic_toolkit/gbif/2026-01-26/arctic_mof_2026-01-27.parquet',
                              output_csv_path='/home/users/zalmanek/arctic_postgres/schema_comparison/comparison_csvs/mof_schema_comparison.csv')