import argparse

from gbif.download.gbif_download_manager import GbifDownloadManager
from gbif.download.gbif_download_unzipper import GbifDownloadUnzipper

if __name__ == "__main__":


    parser = argparse.ArgumentParser(
        description = "GBIF login credentials to download data"
    )
    parser.add_argument(
        '--gbif_user', 
        type=str,
        help="Your GBIF user name."
    )

    parser.add_argument(
        '--gbif_password',
        type=str,
        help="Your password to your GBIF longin"
    )

    parser.add_argument(
        '--email',
        type=str,
        help="The email you want to be notified regarding your the status of GBIF download."
        )

    parser.add_argument(
        '--data_dir',
        type=str,
        help="The path to the directory you want your output data to be saved to."
    )

    args = parser.parse_args()
    
    # Run entire Manager
    # This initiates the download, checks the status and once it succeeds, downloads, and unzips
    manager = GbifDownloadManager(gbif_user=args.gbif_user, 
                                  gbif_notify_email=args.email,
                                  gbif_pass=args.gbif_password,
                                  data_download_dir=args.data_dir)

    # Uncomment if something happens and just need to run the unzipper (add your download_key)
    unzipper = GbifDownloadUnzipper(download_key=manager.download_key, data_download_dir=args.data_dir)
    unzipper.download_and_unzip()