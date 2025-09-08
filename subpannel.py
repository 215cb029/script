import requests
from datetime import datetime

# === CONFIGURATION ===
GRAFANA_URL = "https://cosmos.fkcloud.in"
API_KEY = "your_actual_token_here"  # Replace with your Grafana API key
PROMETHEUS_DS_UID = "j_ToAp8Ik"     # Replace with your Prometheus datasource UID

# Prometheus expression from the panel
expr = 'sum(jmx_metrics_io_dropwizard_jetty_MutableServletContextHandler_dispatches_OneMinuteRate{process=~"dart-k8s.*"}) by(process)'

# Time range
from_dt = "2025-07-06 13:38:26"
to_dt = "2025-07-06 16:38:26"

# === END CONFIGURATION ===

# Convert time range to epoch milliseconds
from_ts = int(datetime.strptime(from_dt, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
print(from_ts)
to_ts = int(datetime.strptime(to_dt, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
print(to_ts)

# Prepare request headers
# headers = {
#     "Authorization": f"Bearer {API_KEY}",
#     "Content-Type": "application/json"
# }

# Build the query payload
query = {
    "queries": [
        {

            "datasource": {
                "uid": PROMETHEUS_DS_UID
            },
            "expr": expr,




        }
    ],
    "from": str(1751790423222),
    "to": str(1751801223222)
}

# Send the query to Grafana
res = requests.post(f"{GRAFANA_URL}/api/ds/query", json=query)

if res.status_code != 200:
    print(f"Query failed: {res.status_code} - {res.text}")
    exit()

data = res.json()

# Extract metric values from response
values = []
try:
    frames = data["results"]["A"]["frames"]
    for frame in frames:
        value_array = frame.get("data", {}).get("values", [])
        if len(value_array) >= 2:
            values = value_array[1]  # index 1 is the metric values
            break
except Exception as e:
    print(f"Error reading data: {e}")
    exit()

if not values:
    print("No values found in the response.")
    exit()

# Calculate statistics
min_val = min(values)
max_val = max(values)
last_val = values[-1]
mean_val = sum(values) / len(values) if values else 0

# Human-readable formatting
def humanize(n):
    return f"{n/1000:.2f} K" if n >= 1000 else f"{n:.2f}"

# Print results
print("\n--- Prometheus Metric Summary ---")
print(f"Min:  {humanize(min_val)}")
print(f"Max:  {humanize(max_val)}")
print(f"Last: {humanize(last_val)}")
print(f"Mean: {humanize(mean_val)}")