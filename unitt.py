import json

# Load the JSON files
with open('/Users/manoranjans.vc/idea/TestDemo/dashboard_subpanels.json', 'r') as f1, open('/Users/manoranjans.vc/idea/TestDemo/nfr_dashboard.json', 'r') as f2:
    dashboard_data = json.load(f1)
    nfr_data = json.load(f2)
    #print(nfr_data)
# Extract sub-panel names
sub_panels = dashboard_data.get("sub-panel", {})

# Build a mapping of titles to units from nfr_dashboard.json
title_unit_map = {}

for outer_panel in nfr_data.get("dashboard", {}).get("panels", []):
    inner_panels = outer_panel.get("panels", [])
    for panel in inner_panels:
        title = panel.get("title", "")
        unit = panel.get("fieldConfig", {}).get("defaults", {}).get("unit", "no unit")
        title_unit_map[title] = unit
print(title_unit_map)

#print(title_unit_map)
# Create a result list with unit info for each sub-panel
results = []
for name in sub_panels:
    unit = title_unit_map.get(name, "no unit")
    results.append({
        "sub_panel": name,
        "unit": unit
    })

# Print results or write to a new file
for item in results:
    print(f"{item['sub_panel']}: {item['unit']}")
