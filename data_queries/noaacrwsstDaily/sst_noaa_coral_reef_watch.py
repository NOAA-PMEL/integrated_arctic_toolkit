import pandas as pd
from erddapy import ERDDAP
from datetime import datetime, timedelta
from cache import cache

# TODO: fix dates issue. Allow grabbing dates

ERDDAP_SERVER = "https://coastwatch.noaa.gov/erddap"
DATASET_ID = "noaacrwsstDaily"

def get_latest_date():
    # 2 day lag? Not sure if this is actually the case
    return (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")

@cache.memoize(timeout=3600) # will cache query for 1 hour. This decorator checks redis cache first
def fetch_sst(stride=20):
    "SST dataset has a reading every 5 km across the whole globe, Stride means skip every 20 grid points"
    date = get_latest_date()
    print(f"Fetching SST for {date}")

    try:
        e = ERDDAP(
            server=ERDDAP_SERVER,
            protocol="griddap"
        )
        e.dataset_id = DATASET_ID
        e.griddap_initialize()

        print(f"Available variables: {e.variables}")

        e.constraints["time>="] = f"{date}T00:00:00Z"
        e.constraints["time<="] = f"{date}T23:59:59Z"
        e.constraints["time_step"] = 1
        e.constraints["latitude_step"] = stride
        e.constraints["longitude_step"] = stride
        
        e.variables = [
            "analysed_sst",
            "sea_ice_fraction"
        ]

        df = e.to_pandas().reset_index()
        df = df.dropna(subset=["analysed_sst (degree_C)"])
        df = df.rename(columns={
            "latitude (degrees_north)": "latitude",
            "longitude (degrees_east)": "longitude",
            "analysed_sst (degree_C)": "sst",
            "sea_ice_fraction (1)": "sea_ice_fraction"
        })
        df["datatype"] = "chemistry"
        return df[["latitude", "longitude", "sst", "sea_ice_fraction", "datatype"]]
    except Exception as e:
        print(f"ERDDAP fetch failed: {e}")
        return pd.DataFrame()