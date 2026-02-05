import argparse
from arctic_postgres.gbif_obis_data_download.obis_updated.download.obis_arctic_downloader import ObisArcticDownloader


if __name__ == "__main__":


    parser = argparse.ArgumentParser(
        description = "Data directory to save obis data to"
    )
    parser.add_argument(
        '-d',
        '--data_dir', 
        type=str,
        help="path to data directory to save files to."
    )

    args = parser.parse_args()
    print(f"args.data_dir = {args.data_dir}")

    arctic_downloader = ObisArcticDownloader(data_dir=args.data_dir)

    # Get occurences, dna_derived, and mof parquet files
    arctic_downloader.get_obis_arctic_occurrences()
    arctic_downloader.get_obis_dna_derived()
    arctic_downloader.get_obis_mof()