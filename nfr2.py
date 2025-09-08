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

dashboard = res.json().get("dashboard", {})
panels = dashboard.get("panels", [])

# ===== FIND COLLAPSED TARGET ROW =====
target_row = None
for panel in panels:
    print(panel.get("title"))
    #print(panel)
    if panel.get("type") == "row" and "pentos -> gaurang" in panel.get("title", "").strip().lower():
            target_row = panel
            break

if not target_row:
    print(f"❌ Collapsed row titled '{TARGET_ROW_TITLE}' not found.")
    exit()

sub_panels = target_row.get("panels", [])
if not sub_panels:
    print(f"⚠️ No sub-panels embedded inside '{TARGET_ROW_TITLE}'.")
    exit()

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

    if not exprs:
        continue

    group = "hyd" if "hyd" in title.lower() or "hyderabad" in title.lower() else "calvin"

    sub_panel_entry = {}
    for idx, expr in enumerate(exprs, 1):
        sub_panel_entry[f"expr{idx}"] = expr
    for idx, legend in enumerate(legends, 1):
        sub_panel_entry[f"legend{idx}"] = legend

    output[group]["sub-panel"][title] = sub_panel_entry

# ===== WRITE TO FILE =====
with open("dashboard4.json", "w") as f:
    json.dump(output, f, indent=2)

print("✅ JSON written to dashboard_subpanels.json")
