import pandas as pd

# Use the CSV export link for public Google Sheets
sheet_id = "1-ssVGeL5ORZwZiJWxPbZ-Uz8fMH8NabAJKHp5turv34"
gid = "0"

csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

# Load into pandas dataframe
df = pd.read_csv(csv_url)
print(df.head())

