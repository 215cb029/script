import requests
from datetime import datetime
import statistics
import string

# === Configuration ===
GRAFANA_URL = "https://cosmos.fkcloud.in"
API_KEY = "your_api_key_here"  # 🔐 Replace this with your actual Grafana API key
DASHBOARD_UID = "b9i1QHWSz"
TARGET_ROW_TITLE = "Dart - Service + Kafka => Mohit"

from_dt = "2025-07-07 12:38:08"
to_dt = "2025-07-07 15:38:08"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# === Time Conversion ===
FROM_TS = int(datetime.strptime(from_dt, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
TO_TS = int(datetime.strptime(to_dt, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

def humanize(n):
    return f"{n / 1000:.1f} K" if n >= 1000 else f"{n:.1f}"

# === Step 1: Load Dashboard JSON ===
res = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{DASHBOARD_UID}")
if res.status_code != 200:
    print(f"❌ Dashboard fetch failed: {res.status_code}")
    exit()

dashboard = res.json()["dashboard"]
panels = dashboard.get("panels", [])

# === Step 2: Find Target Row Panel ===
target_row = None
row_index = -1
for i, panel in enumerate(panels):
    if panel.get("type") == "row" and panel.get("title") == TARGET_ROW_TITLE:
        target_row = panel
        row_index = i
        break

if not target_row:
    print(f"❌ Row '{TARGET_ROW_TITLE}' not found.")
    exit()

# === Step 3: Collect Sub-Panels Under the Target Row ===
start_y = target_row["gridPos"]["y"]
sub_panels = []
for panel in panels[row_index + 1:]:
    if panel.get("type") == "row":
        break
    if panel["gridPos"]["y"] > start_y:
        sub_panels.append(panel)

print(f"✅ Found {len(sub_panels)} sub-panels:\n")

# === Step 4: Process Each Sub-Panel ===
for i, panel in enumerate(sub_panels, 1):
    title = panel.get("title", f"Untitled-{i}")
    targets = panel.get("targets", [])
    print(f"\n{i}. 📊 {title}")

    grouped_queries = {}   # {ds_uid: [(expr, refId, legendFormat, queryType)]}
    expr_lookup = {}       # {refId: (expr, legendFormat)}

    for j, target in enumerate(targets):
        expr = target.get("expr")
        ds = target.get("datasource")
        if not expr or not ds:
            continue

        ds_uid = ds.get("uid") if isinstance(ds, dict) else ds
        refId = string.ascii_uppercase[j % 26]
        legendFormat = target.get("legendFormat", f"{refId}-legend")
        queryType = target.get("queryType", "timeSeriesQuery")

        grouped_queries.setdefault(ds_uid, []).append((expr, refId, legendFormat, queryType))
        expr_lookup[refId] = (expr, legendFormat)

    if not grouped_queries:
        print("   ⚠️ No valid expressions.")
        continue

    for ds_uid, expr_list in grouped_queries.items():
        # === Construct Query Payload (no maxDataPoints, no intervalMs, no utcOffsetSec) ===
        payload = {
            "from": str(FROM_TS),
            "to": str(TO_TS),
            "queries": []
        }

        for expr, refId, legendFormat, queryType in expr_list:
            payload["queries"].append({
                "refId": refId,
                "expr": expr,
                "datasource": {"uid": ds_uid},
                "legendFormat": legendFormat,
                "queryType": queryType
            })

        # === Step 5: Query Grafana Backend API ===
        response = requests.post(f"{GRAFANA_URL}/api/ds/query",json=payload)
        if response.status_code != 200:
            print(f"   ❌ Failed to query Grafana (UID={ds_uid}): {response.status_code}")
            continue

        try:
            results = response.json().get("results", {})
            for refId, result in results.items():
                expr, legendFormat = expr_lookup.get(refId, ("<unknown>", "<legend>"))
                frames = result.get("frames", [])
                values = []

                for frame in frames:
                    fields = frame.get("data", {}).get("values", [])
                    if len(fields) >= 2:
                        metric_values = fields[1]
                        values = [float(v) for v in metric_values if v is not None]
                        break

                if not values:
                    print(f"   ➤ {legendFormat}\n     ⚠️ No data for expr: {expr}")
                    continue

                min_val = min(values)
                max_val = max(values)
                last_val = values[-1]
                mean_val = statistics.mean(values)

                print(f"   ➤ {legendFormat}")
                print(f"     📈 Min:  {humanize(min_val)}")
                print(f"     📈 Max:  {humanize(max_val)}")
                print(f"     📈 Last: {humanize(last_val)}")
                print(f"     📈 Mean: {humanize(mean_val)}")

        except Exception as e:
            print(f"   ❌ Error parsing results: {e}")
