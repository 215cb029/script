import json
import re
import pandas as pd
from google.cloud import monitoring_v3


def get_project(subscription_path: str) -> str:
    """
    Extracts the project ID from a Pub/Sub subscription path.
    Example: 'projects/my-project/subscriptions/my-sub' → 'my-project'
    """
    parts = subscription_path.split('/')
    try:
        project_index = parts.index('projects') + 1
        return parts[project_index]
    except (ValueError, IndexError):
        raise ValueError("Invalid subscription path format for extracting project.")

def get_sub(subscription_path: str) -> str:
    """
    Extracts the subscription ID from a Pub/Sub subscription path.
    Example: 'projects/my-project/subscriptions/my-sub' → 'my-sub'
    """
    parts = subscription_path.split('/')
    try:
        sub_index = parts.index('subscriptions') + 1
        return parts[sub_index]
    except (ValueError, IndexError):
        raise ValueError("Invalid subscription path format for extracting subscription ID.")
def run_query2(mql_query, project_id):
    client = monitoring_v3.QueryServiceClient()
    project_name = f"projects/{project_id}"
    list = []
    try:
        request = monitoring_v3.QueryTimeSeriesRequest(name=project_name, query=mql_query)
        response = client.query_time_series(request=request)
        for point in response.time_series_data:
            for entry in point.point_data:
                for val in entry.values:
                    list.append(val.double_value)
    except Exception as e:
        print(f"Error querying Monitoring API for project {project_id}: {e}")
    return list

def get_last_word(input_string):
    parts = input_string.replace('/', ' ').replace('-', ' ').split()
    return parts[-1] if parts else None

def get_words_between(input_string, word1, word2):
    try:
        start_index = input_string.index(word1) + len(word1)
        end_index = input_string.rindex(word2)
        between = input_string[start_index:end_index]
        words = between.replace('/', ' ').replace('-', ' ').split()
        return ' '.join(words) if words else None
    except ValueError:
        return None

def extract_job_name(subscription_path):
    match = re.search(r'subscriptions/([^/]+?)--', subscription_path)
    if match:
        job_name = match.group(1)

        if "Job" in job_name:
            job_index = job_name.find("Job") + len("Job")
            return job_name[:job_index]

        else:
            return job_name
    return re.search(r'subscriptions/([^/]+?)(?:-sub)?$', subscription_path).group(1)

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
        topic_name = sub["topic_name"]
        project=get_project(subscription_name)
        sub=get_sub(subscription_name)
        mql_query=f"fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/ack_message_count' | filter resource.project_id == {project} && (resource.subscription_id == {sub}) | align rate(1m) | every 1m | group_by [metric.delivery_type], [value_ack_message_count_aggregate: aggregate(value.ack_message_count)] | within d'2025/08/10-11:34:39', d'2025/08/18-12:04:39'"
        list=run_query2(mql_query,project)
        idle = 'Yes' if not list else 'No'
        last_subscription_name = get_last_word(subscription_name)
        last_topic_name = get_last_word(topic_name)

        extracted = get_words_between(subscription_name, last_topic_name, last_subscription_name)
        jobname = extracted if extracted is not None else extract_job_name(subscription_name)

        # Match subscription name and get service accounts
        service_accounts = permissions_lookup.get(subscription_name, [])

        rows.append({
            "JobName": jobname,
            "Pubsub / Viesti / Kafka Topic Name": topic_name,
            "Pubsub/Viesti  Subscription Name": subscription_name,
            "ServiceAccounts": ',   '.join(service_accounts),  # Join list into a string
            "Idle":idle

        })

    df = pd.DataFrame(rows)
    df.to_excel(output_excel, index=False)
    print(f"✅ Excel file created: {output_excel}")

# Example usage
json_file = "/Users/manoranjans.vc/idea/TestDemo/pubsub.tfvars.json"
output_excel = "pubsub_final.xlsx"
process_pubsub_file_to_excel(json_file, output_excel)
