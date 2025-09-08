#Used to make HTTP requests
import requests
from datetime import datetime
import statistics

GRAFANA_URL = "https://cosmos.fkcloud.in"
PROMETHEUS_DS_UID = "j_ToAp8Ik"
EXPR = 'sum(jmx_metrics_io_dropwizard_jetty_MutableServletContextHandler_dispatches_OneMinuteRate{process=~"dart-k8s.*"}) by(process)'

from_dt = "2025-07-07 10:18:53"
to_dt = "2025-07-07 10:48:53"


# Convert time range to epoch milliseconds
FROM_TS = int(datetime.strptime(from_dt, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
print(FROM_TS)
TO_TS = int(datetime.strptime(to_dt, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
print(TO_TS)


MAX_DATA_POINTS = 1352

# === HELPER: Round values 11661.224792999998= 11.7 K===
def humanize(n):

    return f"{n / 1000:.1f} K" if n >= 1000 else f"{n:.1f}"

# === Build query payload ===
payload = {
    "from": str(FROM_TS),
    "to": str(TO_TS),
    "queries": [
        {

            "expr": EXPR,
            "datasource": {
                "uid": PROMETHEUS_DS_UID
            },


        }
    ]
}



# === Send the request to Grafana ===
response = requests.post(f"{GRAFANA_URL}/api/ds/query", json=payload)

if response.status_code != 200:
    print(f"❌ Query failed: {response.status_code} - {response.text}")
    exit()

data = response.json()

# === Extract time series values which are present in milliseconds ===
try:
    results = data["results"]["A"]["frames"]
    values = []

    for frame in results:
        fields = frame.get("data", {}).get("values", [])

        if len(fields) >= 2:
            metric_values = fields[1]  # index 1 contains metric values

            values = [float(v) for v in metric_values if v is not None]
            break

    if not values:
        print("⚠️ No data points returned.")
        exit()

except Exception as e:
    print(f"❌ Error parsing response: {e}")
    exit()

# === Compute statistics ===
min_val = min(values)
max_val = max(values)
last_val = values[-1]
mean_val = statistics.mean(values)

# === Display results ===
print("\n--- Prometheus Metric Summary ---")
print(f"Min:  {humanize(min_val)}")
print(f"Max:  {humanize(max_val)}")
print(f"Last: {humanize(last_val)}")
print(f"Mean: {humanize(mean_val)}")
