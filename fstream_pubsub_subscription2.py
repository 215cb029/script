import json
import pandas as pd

def process_pubsub_file_to_excel(file_path: str, output_excel: str):
    with open(file_path, "r") as f:
        data = json.load(f)

    # Create a lookup dictionary for permissions
    permissions_lookup = {
        perm["name"]: perm["permissions"][0]["members"]
        for perm in data.get("subscriptions_permissions", [])
    }

    rows = []
    for sub in data.get("pubsub_subscriptions", []):
        subscription_name = sub["name"]

        # Match subscription name and get service accounts
        service_accounts = permissions_lookup.get(subscription_name, [])

        rows.append({
            "Subscription Name": subscription_name,
            "ServiceAccounts": ',   '.join(service_accounts),  # Join list into a string

        })

    df = pd.DataFrame(rows)
    df.to_excel(output_excel, index=False)
    print(f"✅ Excel file created: {output_excel}")

# Example usage
json_file = "/Users/manoranjans.vc/idea/TestDemo/pubsub.tfvars.json"
output_excel = "pubsub_demo2.xlsx"
process_pubsub_file_to_excel(json_file, output_excel)
