import requests

GRAFANA_URL = "https://cosmos.fkcloud.in"
API_KEY = "your_api_key_here"  # Replace with actual token
DASHBOARD_UID = "b9i1QHWSz"

# headers = {
#     "Authorization": f"Bearer {API_KEY}",
#     "Content-Type": "application/json"
# }

# Step 1: Fetch dashboard JSON
res = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{DASHBOARD_UID}")
if res.status_code != 200:
    print(f"Failed to fetch dashboard: {res.status_code}")
    exit()

dashboard = res.json()["dashboard"]
panels = dashboard.get("panels", [])

# Step 2: Find the target row
target_title = "Dart - Service + Kafka => Mohit"
target_row = None
for i, panel in enumerate(panels):
    if panel.get("type") == "row" and panel.get("title") == target_title:
        target_row = panel
        row_index = i
        break

if not target_row:
    print(f"Row panel titled '{target_title}' not found.")
    exit()

start_y = target_row["gridPos"]["y"]
nested_panels = []

# Step 3: Collect sub-panels under this row
for panel in panels[row_index + 1:]:
    if panel.get("type") == "row":
        break  # stop at next row
    if panel["gridPos"]["y"] > start_y:
        nested_panels.append(panel)

# Step 4: Extract title and Prometheus expressions
print(f"\nFound {len(nested_panels)} panels inside '{target_title}':\n")

for i, panel in enumerate(nested_panels, 1):
    title = panel.get("title", "Untitled")
    expressions = []

    # Panels using Prometheus data source usually store targets under "targets"
    for target in panel.get("targets", []):
        expr = target.get("expr")
        if expr:
            expressions.append(expr.strip())

    if expressions:
        print(f"{i}. {title}")
        for expr in expressions:
            print(f"   ➤ {expr}")
    else:
        print(f"{i}. {title} (no Prometheus expression found)")
