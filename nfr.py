import requests
import json

# ===== CONFIGURATION =====
GRAFANA_URL = "https://cosmos.fkcloud.in"
API_KEY = "your_api_key_here"  # 🔐 Replace this
DASHBOARD_UID = "b9i1QHWSz"
TARGET_ROW_TITLE = "Pentos -> Gaurang"

HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# ===== FETCH DASHBOARD JSON =====
res = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{DASHBOARD_UID}")
if res.status_code != 200:
    print(f"❌ Failed to fetch dashboard: {res.status_code}")
    exit()

dashboard = res.json()["dashboard"]
panels = dashboard.get("panels", [])

# ===== FIND TARGET ROW =====
target_row = None
row_index = -1
for i, panel in enumerate(panels):
    if panel.get("type") == "row" and panel.get("title") == TARGET_ROW_TITLE:
        target_row = panel
        row_index = i
        break

if not target_row:
    print(f"❌ Row titled '{TARGET_ROW_TITLE}' not found.")
    exit()

start_y = target_row["gridPos"]["y"]
sub_panels = []

# ===== COLLECT SUB-PANELS UNDER THE ROW =====
for panel in panels[row_index + 1:]:

    if panel.get("type") == "row":
        break  # Stop at the next row
    #if panel["gridPos"]["y"] > start_y:
    sub_panels.append(panel)
print(sub_panels)
# ===== GROUPED JSON OUTPUT =====
output = {
    "hyd": {
        "ip": "10.24.44.196",
        "sub-panel": {}
    },
    "calvin": {
        "ip": "10.83.45.174",
        "sub-panel": {}
    }
}

# ===== PROCESS EACH SUB-PANEL =====
for i, panel in enumerate(sub_panels, 1):
    title = panel.get("title", f"Untitled-{i}").strip()
    targets = panel.get("targets", [])

    exprs = []
    legends = []

    for target in targets:
        expr = target.get("expr")
        if expr:
            exprs.append(expr.strip())

        legend = target.get("legendFormat")
        if legend:
            legends.append(legend.strip())

    # Skip panels with no expressions
    if not exprs:
        continue

    # Determine group
    lower_title = title.lower()
    group = "hyd" if "hyd" in lower_title or "hyderabad" in lower_title else "calvin"

    # Create entry
    sub_panel_entry = {}
    for idx, expr in enumerate(exprs, 1):
        sub_panel_entry[f"expr{idx}"] = expr

    for idx, legend in enumerate(legends, 1):
        sub_panel_entry[f"legend{idx}"] = legend

    # Insert into output
    output[group]["sub-panel"][title] = sub_panel_entry


# ===== WRITE TO FILE =====
with open("dashboard5.json", "w") as f:
    json.dump(output, f, indent=2)

print("✅ JSON written to dashboard_subpanels.json")



# ===== PRINT FINAL JSON =====
#print(json.dumps(output, indent=2))
