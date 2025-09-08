from google.cloud import monitoring_v3

from gcm3 import mql_query

client = monitoring_v3.QueryServiceClient()
project_name = "projects/fkp-specter-pubsub"
# mql_query="fetch cloud_dataproc_cluster | metric 'dataproc.googleapis.com/cluster/yarn/nodemanagers' | filter (metric.status == 'unhealthy') | group_by 1m, [value_nodemanagers_mean: mean(value.nodemanagers)] | every 1m | group_by [], [value_nodemanagers_mean_max: max(value_nodemanagers_mean)] | graph_period 2s | within d'2025/07/23-11:34:39', d'2025/07/23-12:04:39'"
mql_query = '''
fetch pubsub_subscription
| metric 'pubsub.googleapis.com/subscription/ack_message_count'
| filter
    resource.project_id == 'fkp-specter-pubsub'
    &&
    (resource.subscription_id
     ==
     'in-hyderabad-1.dart.fkint.ixp.catalog.ProductStateEntity--M3DataProductAttributesProd--sub')
| align rate(1m)
| every 1m
| group_by [metric.delivery_type],
    [value_ack_message_count_aggregate: aggregate(value.ack_message_count)]  | within d'2025/08/11-11:34:39', d'2025/08/18-12:04:39'
'''

request = monitoring_v3.QueryTimeSeriesRequest(
    name=project_name,
    query=mql_query
)

response = client.query_time_series(request=request)
print(response)
list = []
print(response)
for point in response.time_series_data:
    data = point.point_data
    print(data)

    # Each 'entry' may contain multiple 'values'
    for entry in data:
        for val in entry.values:
           list.append(val.double_value)

#print(list)
print("Max:", max(list))
print("Min:", min(list))
print("last:", list[0])


