from data.noaacrwsstDaily.sst_noaa_coral_reef_watch import fetch_sst

df = fetch_sst()
print(f"shape: {df.shape}")
print(df.head())