from google.cloud import monitoring_v3

def run_query2(mql_query, project_id):
    client = monitoring_v3.QueryServiceClient()
    project_name = f"projects/{project_id}"
    result_list = []

    try:
        request = monitoring_v3.QueryTimeSeriesRequest(name=project_name, query=mql_query)
        response = client.query_time_series(request=request)

        for time_series in response.time_series_data:
            for point_data in time_series.point_data:
                for value in point_data.values:
                    # Check for different value types and append to the list
                    if value.HasField("double_value"):
                        result_list.append(value.double_value)
                    elif value.HasField("int64_value"):
                        result_list.append(value.int64_value)
                    # Add other value types if needed, like bool_value, string_value, etc.

    except Exception as e:
        print(f"Error querying Monitoring API for project {project_id}: {e}")

    return result_list

if __name__ == "__main__":
    mql_query = """
    fetch pubsub_subscription
    | metric 'pubsub.googleapis.com/subscription/oldest_unacked_message_age'
    | filter resource.project_id == 'fkp-sonic-pubsub' &&
             resource.subscription_id == 'in-hyderabad-1.dart.fkint.cp.ca_discover.DiscoveryContentImpression--M3DataDiscoverySonicFrawProd--sub'
    | group_by 1m, [value_oldest_unacked_message_age_max: max(value.oldest_unacked_message_age)]
    | every 1m
    | group_by [], [value_oldest_unacked_message_age_max_max: max(value_oldest_unacked_message_age_max)]
    | within d'2025/08/12-14:47:00', d'2025/08/19-14:47:00'
    """

    data_list = run_query2(mql_query, 'fkp-sonic-pubsub')

    print("Received data points:", data_list)
    print("Number of data points:", len(data_list))

    if all(x == 0 for x in data_list):
        print("Idle: yes")
    else:
        print("Idle: no")