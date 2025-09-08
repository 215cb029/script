import requests
import json

url = "https://cosmos.fkcloud.in/api/dashboards/uid/b9i1QHWSz"
# headers = {
#     "Authorization": "Bearer YOUR_API_KEY",  # Replace with your key
#     "Accept": "application/json"
# }

response = requests.get(url)

if response.status_code == 200:
    dashboard_json = response.json()
    # Save only the dashboard part if needed
    with open("nfr_dashboard.json", "w") as f:
        json.dump(dashboard_json, f, indent=4)
    print("Dashboard JSON saved.")
else:
    print(f"Error fetching dashboard: {response.status_code}")
