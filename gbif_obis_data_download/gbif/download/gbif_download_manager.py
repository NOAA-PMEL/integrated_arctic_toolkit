import time

from pygbif import occurrences as occ
from gbif.download.gbif_arctic_data_download_requester import GbifArcticDataDownloadRequester
from gbif.download.gbif_download_unzipper import GbifDownloadUnzipper

class GbifDownloadManager:

    def __init__(self, gbif_user: str, gbif_notify_email: str, gbif_pass: str, data_download_dir: str):
        self.download_initiator = GbifArcticDataDownloadRequester(gbif_user=gbif_user, gbif_notify_email=gbif_notify_email, gbif_pass=gbif_pass)
        self.download_key = self.download_initiator.download_key
        self.data_download_dir = data_download_dir

        self.check_and_process_gbif_download()

    def check_and_process_gbif_download(self):

        start_time = time.time()
        max_wait_time = 18000 # 5 hours (5 * 60 *60) in seconds
        check_interval = 900 # 15 minutes
        
        while True:

            # check elapsed time
            elapsed_time = time.time() - start_time

            if elapsed_time > max_wait_time:
                print(f"Timeout: Download not completed within {max_wait_time} seconds.")
                return None
            
            try:
                # Get download metadata
                meta = occ.download_meta(self.download_key)
                status = meta['status']

                print(f"Download status: {status} (checked at {time.strftime('%Y-%m-%d %H:%M:%S')})")

                # If sucesses, download and unzip
                if status == "SUCCEEDED":
                    print("Download completed! Processing...")
                    unzipper = GbifDownloadUnzipper(download_key=self.download_key, data_download_dir=self.data_download_dir)
                    unzipper.download_and_unzip()

                elif status in ["FAILED", "CANCELED", "KILLED"]:
                    print(f"Download {status.lower()}, Cannot proceed.")
                    return None
                
                elif status in ["PREPARING", "RUNNING", "SUSPENDED"]:
                    print(f"Download still in progress, waiting {check_interval} seconds...")
                    time.sleep(check_interval)

                else:
                    print(f"Unknown status: {status}")
                    return None
                
            except Exception as e:
                print(f"Error checking download status: {e}")
                return None

