import json
import re
import pandas as pd

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
            # Return up to and including "Job"
            job_index = job_name.find("Job") + len("Job")
            return job_name[:job_index]
        else:
            return job_name


    return re.search(r'subscriptions/([^/]+?)(?:-sub)?$', subscription_path).group(1)

def process_pubsub_file_to_excel(file_path: str, output_excel: str):
    with open(file_path, "r") as f:
        data = json.load(f)

    rows = []
    for sub in data.get("pubsub_subscriptions", []):
        subscription_name = sub["name"]
        topic_name = sub["topic_name"]

        last_subscription_name = get_last_word(subscription_name)
        last_topic_name = get_last_word(topic_name)

        extracted = get_words_between(subscription_name, last_topic_name, last_subscription_name)
        jobname = extracted if extracted is not None else extract_job_name(subscription_name)

        rows.append({
            "JobName": jobname,
            "SubscriptionName": subscription_name,
            "TopicName": topic_name,
        })

    df = pd.DataFrame(rows)
    df.to_excel(output_excel, index=False)
    print(f"✅ Excel file created: {output_excel}")

# Example usage
json_file = "/Users/manoranjans.vc/idea/TestDemo/pubsub.tfvars.json"
output_excel = "pubsub_output.xlsx"
process_pubsub_file_to_excel(json_file, output_excel)
