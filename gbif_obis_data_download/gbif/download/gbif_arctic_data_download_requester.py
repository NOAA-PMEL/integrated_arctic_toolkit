import requests

class GbifArcticDataDownloadRequester:

    ARCTIC_POLYGON = 'POLYGON((-180 60, 180 60, 180 90, -180 90, -180 60))' # Loose lat/lon polygon coords for the Arctic
    DOWNLOAD_FORMAT = "DWCA" # Darwin Core Archive - only format to get DNA-derived and MOF
    VERBATIM_DNA_DER_EXT = "http://rs.gbif.org/terms/1.0/DNADerivedData" # name of extension to specify getting DNA-derived data
    VERBATIM_MOF_EXT = "http://rs.iobis.org/obis/terms/ExtendedMeasurementOrFact" # name of extension to specify getting measurement of fact
    GBIF_OCCURRENCE_REQ_URL = "https://api.gbif.org/v1/occurrence/download/request" # GBIF request url for occurrence downloads

    def __init__(self, gbif_user: str, gbif_notify_email: str, gbif_pass: str):

        self.gbif_user = gbif_user
        self.gbif_notify_email = gbif_notify_email
        self.gbif_pass = gbif_pass
        self.download_key = self._initiate_arctic_download()

    def _construct_query(self):
        """"
        Constructs the query to get the arctic data from GBIF including
        the DNA-derived and Measurment of Fact files.
        """
        return {
            "creator": self.gbif_user,
            "notificationAddresses": [
                self.gbif_notify_email
                ],
            "sendNotification": True,
            "format": self.DOWNLOAD_FORMAT,
            "predicate": {
                "type": "within",
                "geometry": self.ARCTIC_POLYGON
                },
                "verbatimExtensions": [
                    self.VERBATIM_DNA_DER_EXT,
                    self.VERBATIM_MOF_EXT
                    ]
                    }
    
    def _initiate_arctic_download(self):
        """
        Initiates the download of Arctic data and prints the download
        key.
        """
        query = self._construct_query()

        response = requests.post(
            self.GBIF_OCCURRENCE_REQ_URL,
            json=query,
            auth=(self.gbif_user, self.gbif_pass),
            headers={'Content-Type': 'application/json'}
        )

        if response.status_code == 201:
            download_key = response.text.strip()
            print(f"Download created!: {download_key}")
            print(f"Track it as: https://www.gbif.org/occurrence/download/{download_key}")
            return download_key
        else:
            print(f"Error {response.status_code}: {response.text}")