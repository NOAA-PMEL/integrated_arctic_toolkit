from arctic_postgres.gbif_obis_data_download.gbif.parse.gbif_occurrence_parser import GbifOccurrenceParser

if __name__ == "__main__":

    gbif_occ_parser = GbifOccurrenceParser(gbif_download_dir="/home/mule-external/sci-dig/arctic_toolkit/gbif/2026-01-26")