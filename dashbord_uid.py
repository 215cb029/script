import requests

# ===== CONFIGURATION =====
GRAFANA_URL = "https://cosmos.fkcloud.in"
API_KEY = "your_api_key_here"  # 🔐 Replace with your actual API key
DASHBOARD_UID = "b9i1QHWSz"
TARGET_ROW_TITLE = "Dart - Service + Kafka => Mohit"



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
    if panel["gridPos"]["y"] > start_y:
        sub_panels.append(panel)

print(f"\n✅ Found {len(sub_panels)} sub-panels under row '{TARGET_ROW_TITLE}':\n")

# ===== PROCESS EACH SUB-PANEL =====
for i, panel in enumerate(sub_panels, 1):
    title = panel.get("title", f"Untitled-{i}")
    targets = panel.get("targets", [])
    expr_list = []
    datasource_uids = set()

    for target in targets:
        expr = target.get("expr")
        if expr:
            expr_list.append(expr.strip())

        ds = target.get("datasource")
        if isinstance(ds, dict):
            datasource_uids.add(ds.get("uid") or ds.get("name"))
        elif isinstance(ds, str):
            datasource_uids.add(ds)

    print(f"{i}. 📊 Title: {title}")
    if expr_list:
        for e in expr_list:
            print(f"   ➤ Expr: {e}")
    else:
        print("   ⚠️ No Prometheus expressions found.")

    if datasource_uids:
        for ds_uid in datasource_uids:
            print(f"   🗄️ Datasource UID: {ds_uid}")
    else:
        print("   ⚠️ No datasource UID found.")
