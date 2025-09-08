import json
from pyspark import SparkConf, SparkContext
import json
import re
import pandas as pd
from google.cloud import monitoring_v3
from datetime import datetime, timedelta
import pytz
output_excel = "non_idle2.xlsx"
# Load the JSON file
with open('/Users/manoranjans.vc/idea/TestDemo/ideal_sub_names.json', 'r') as file:
    data = json.load(file)
sub_check_count=0
# Traverse each sheet and its subscriptions
list = []
rows = []
for sheet_name, subscriptions in data.items():
    for sub in subscriptions:
         list.append(sub)
def get_current_date_time():
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    return now.strftime('%Y/%m/%d-%H:%M:%S')

def get_date_seven_days_ago(current_date_str):
    current_date = datetime.strptime(current_date_str, '%Y/%m/%d-%H:%M:%S')
    previous_date = current_date - timedelta(days=7)
    return previous_date.strftime('%Y/%m/%d-%H:%M:%S')
def run_query1(mql_query, project_id):
    """
    run oldest_unacked_message_age metric & return list of data (basically it returns data in second format)
    """
    client = monitoring_v3.QueryServiceClient()
    project_name = f"projects/{project_id}"
    list = []
    try:
        request = monitoring_v3.QueryTimeSeriesRequest(name=project_name, query=mql_query)
        response = client.query_time_series(request=request)
        for point in response.time_series_data:
            for entry in point.point_data:
                for val in entry.values:
                    list.append(val.int64_value)
    except Exception as e:
        print(f"Error querying Monitoring API for project {project_id}: {e}")
    return list

def run_query2(mql_query, project_id):
    """
    run  ack_message_count metric
    """
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
def seconds_to_days(seconds):
    # There are 86400 seconds in a day (24 * 60 * 60)
    days = seconds / 86400
    return round(round(days, 7))  # convert second value to day format


def precondition_check(sub):
    global sub_check_count
    sub_check_count += 1
    print(f"[{sub_check_count}] Checking subscription: {sub}")
    project_name=get_project(sub)
    sub_name=get_sub(sub)
    end_time=get_current_date_time()
    start_time=get_date_seven_days_ago(end_time)
    mql_query1 = f"""
    fetch pubsub_subscription
    | metric 'pubsub.googleapis.com/subscription/oldest_unacked_message_age'
    | filter resource.project_id == '{project_name}' &&
             resource.subscription_id == '{sub_name}'
    | group_by 1m, [value_oldest_unacked_message_age_max: max(value.oldest_unacked_message_age)]
    | every 1m
    | group_by [], [value_oldest_unacked_message_age_max_max: max(value_oldest_unacked_message_age_max)]
    | within d'{start_time}', d'{end_time}'
    """
    mql_query2=f"""
    fetch pubsub_subscription
    | metric 'pubsub.googleapis.com/subscription/ack_message_count'
    | filter
        resource.project_id == '{project_name}'
        &&
        (resource.subscription_id == '{sub_name}')
    | align rate(1m)
    | every 1m
    | group_by [metric.delivery_type],
        [value_ack_message_count_aggregate: aggregate(value.ack_message_count)]
    | within d'{start_time}', d'{end_time}'
    """

    list1=run_query1(mql_query1,project_name)
    list2=run_query2(mql_query2,project_name)

    if list1:
        second=list1[0] # find the first index value(second)
        day=seconds_to_days(second)# convert to days
        print(f"day is {day}")
        if day>=3:
            if not list2 or all(x == 0 for x in list2):
                return ''
            else:
                print(sub)
                return sub
        else:
            print(sub)
            return sub
    else:
        print(sub)
        return sub



def process_pubsub_file(sc):

    rdd = sc.parallelize(list, numSlices=10)

    # Map each subscription to a dictionary
    subscription_rows = rdd.map(lambda sub: {
        "non Idle Subscription":precondition_check(sub),

    }).collect()

    rows.extend(subscription_rows)


    # Save to Excel
    df = pd.DataFrame(rows)
    df.to_excel(output_excel, index=False)

if __name__ == "__main__":
    conf = SparkConf().setAppName("fstream").setMaster("local[*]")
    sc = SparkContext.getOrCreate(conf)
    process_pubsub_file(sc)
    # print(len(rows))