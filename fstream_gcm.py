from google.cloud import monitoring_v3
from numpy.matlib import empty
from datetime import datetime
import pytz

def get_current_date_time():
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    return now.strftime('%Y/%m/%d-%H:%M:%S')

# Example usage
print(get_current_date_time())
from datetime import datetime, timedelta

def get_date_seven_days_ago(current_date_str):
    # Parse the input string into a datetime object
    current_date = datetime.strptime(current_date_str, '%Y/%m/%d-%H:%M:%S')

    # Subtract one day
    previous_date = current_date - timedelta(days=7)

    # Format back to the original string format
    return previous_date.strftime('%Y/%m/%d-%H:%M:%S')

# Example usage
print(get_date_seven_days_ago(get_current_date_time()))  # Output: 2025/08/17-20:12:29

def seconds_to_days(seconds):
    # There are 86400 seconds in a day (24 * 60 * 60)
    days = seconds / 86400
    return round(round(days, 7))
def run_query1(mql_query, project_id):
    print(mql_query)
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
    print(mql_query)
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

if __name__ == "__main__":
    #mql_query1="fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/ack_message_count' | filter resource.project_id == 'fkp-sonic-pubsub' && (resource.subscription_id == 'asia-south1-fk-recycle-clus-d20436b6-gcs-subscription') | align rate(1m) | every 1m | group_by [metric.delivery_type], [value_ack_message_count_aggregate: aggregate(value.ack_message_count)] | within d'2025/08/10-11:34:39', d'2025/08/18-12:04:39'"
    # mql_query="fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/oldest_unacked_message_age' | filter resource.project_id == 'fkp-sonic-pubsub' && resource.subscription_id == 'in-hyderabad-1.dart.fkint.cp.ca_discover.DiscoveryContentImpression--M3DataDiscoverySonicFrawProd--sub' | group_by 1m, [value_oldest_unacked_message_age_max: max(value.oldest_unacked_message_age)] | every 1m | group_by [], [value_oldest_unacked_message_age_max_max: max(value_oldest_unacked_message_age_max)] | within d'2025/08/12-14:47:00', d'2025/08/19-14:47:00"
    end_time=get_current_date_time()
    start_time=get_date_seven_days_ago(end_time)
    mql_query1 = f"""
    fetch pubsub_subscription
    | metric 'pubsub.googleapis.com/subscription/oldest_unacked_message_age'
    | filter resource.project_id == 'fkp-p0-pubsub' &&
             resource.subscription_id == 'cdm_order_orderitemunit_enriched_v2--M3DataAdsL0SalesAttributionProd--sub'
    | group_by 1m, [value_oldest_unacked_message_age_max: max(value.oldest_unacked_message_age)]
    | every 1m
    | group_by [], [value_oldest_unacked_message_age_max_max: max(value_oldest_unacked_message_age_max)]
    | within d'2025/08/15-09:50:00', d'2025/08/22-09:50:00'
    """
    mql_query2=f"""
    fetch pubsub_subscription
    | metric 'pubsub.googleapis.com/subscription/ack_message_count'
    | filter
        resource.project_id == 'fkp-p0-pubsub'
        &&
        (resource.subscription_id == 'cdm_order_orderitemunit_enriched_v2--M3DataAdsL0SalesAttributionProd--sub')
    | align rate(1m)
    | every 1m
    | group_by [metric.delivery_type],
        [value_ack_message_count_aggregate: aggregate(value.ack_message_count)]
    | within d'2025/08/15-09:50:00', d'2025/08/22-09:50:00'
    """
    list1=run_query1(mql_query1,'fkp-p0-pubsub')
    list2=run_query2(mql_query2,'fkp-p0-pubsub')
    # idle=''
    # idle = 'yes' if not list else 'no'
    print(list1)
    print(list2)
    if list1:
        second=list1[0]
        print(f"the second is {second}")
        day=seconds_to_days(second)
        print(f" {day} days ago")
        if day>=3:
            if not list2 or all(x == 0 for x in list2):
                print(f'idle  sub')
            else:
                print(f" not idle")
        else:
            print(f"not idle")



    mql_query1a = f"""
    fetch pubsub_subscription
    | metric 'pubsub.googleapis.com/subscription/oldest_unacked_message_age'
    | filter resource.project_id == 'fkp-p0-pubsub' &&
             resource.subscription_id == 'cdm_order_orderitemunit_enriched_v2--M3DataAdsL0SalesAttributionProd--sub'
    | group_by 1m, [value_oldest_unacked_message_age_max: max(value.oldest_unacked_message_age)]
    | every 1m
    | group_by [], [value_oldest_unacked_message_age_max_max: max(value_oldest_unacked_message_age_max)]
    | within d'{start_time}', d'{end_time}'
    """
    mql_query2a=f"""
    fetch pubsub_subscription
    | metric 'pubsub.googleapis.com/subscription/ack_message_count'
    | filter
        resource.project_id == 'fkp-p0-pubsub'
        &&
        (resource.subscription_id == 'cdm_order_orderitemunit_enriched_v2--M3DataAdsL0SalesAttributionProd--sub')
    | align rate(1m)
    | every 1m
    | group_by [metric.delivery_type],
        [value_ack_message_count_aggregate: aggregate(value.ack_message_count)]
    | within d'{start_time}', d'{end_time}'
    """

    list3=run_query1(mql_query1a,'fkp-p0-pubsub')
    list4=run_query2(mql_query2a,'fkp-p0-pubsub')
    # idle=''
    # idle = 'yes' if not list else 'no'
    print(list3)
    print(list4)
    if list3:
        second2=list3[0]
        print(f"the second is {second2}")
        day2=seconds_to_days(second2)
        print(f" {day2} days ago")
        if day2>=3:
            if not list4 or all(x == 0 for x in list4):
                print(f'idle  sub')
            else:
                print(f" not idle")
        else:
            print(f"not idle")

    # print(max(list))
    # print(min(list))
    # print(len(list))
    # print(idle)