import requests
import zipfile
from datetime import datetime
from pathlib import Path

class GbifDownloadUnzipper:

    def __init__(self, download_key: str, data_download_dir: str):

        self.download_key = download_key
        self.download_url = f"https://api.gbif.org/v1/occurrence/download/request/{download_key}.zip"
        self.data_download_dir = data_download_dir

    def download_and_unzip(self):
        """
        Downloads the GBIF zip file and unzips it to the data directory
        """
        print(f"Downloading {self.download_key}...")
        response = requests.get(self.download_url, stream=True)

        if response.status_code == 200:
            # Save the zip file
            zip_path = self.create_dated_file_name()
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded to {zip_path}")

            print("Extracting")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(f"{zip_path.parent}")
                print(f"{zip_path.parent}/")
                zip_path.unlink() # Deletes the file represented by the Path object
        else:
            print(f"Error: {response.status_code}")

    def create_dated_file_name(self) -> str:
        """
        Creates a .zip filename with the date - year first - 
        for the arctic data
        """
        date_str = datetime.now().strftime("%Y-%m-%d")

        # Create directory with date as name
        dated_dir = Path(self.data_download_dir) / date_str
        dated_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{date_str}_arctic_data.zip"

        # Combine directory and filename
        full_path = Path(self.data_download_dir) / date_str / filename

        return full_path


