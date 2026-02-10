from arctic_postgres.gbif_obis_data_download.align_schema.schema_aligner import DwcSchemaAligner

aligner = DwcSchemaAligner()
rename_dict = aligner.rename_col_map
