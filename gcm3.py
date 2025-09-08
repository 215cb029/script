from google.cloud import monitoring_v3
from datetime import datetime

client = monitoring_v3.QueryServiceClient()
project_name = "projects/fk-sparrow-k8s"

mql_query = '''
fetch https_lb_rule
| metric 'loadbalancing.googleapis.com/https/request_bytes_count'
| filter
   (resource.backend_target_name == 'mci-vx262k-28226--u44glr-fdp-sonic-gcp-prod-sonic-service-mcs')
| align rate(1m)
| every 1m
| group_by [],
   [value_request_bytes_count_aggregate: aggregate(value.request_bytes_count)] | graph_period 500ms | within d'2025/07/23-06:55:20', d'2025/07/23-07:10:20'
'''

request = monitoring_v3.QueryTimeSeriesRequest(
    name=project_name,
    query=mql_query
)

response = client.query_time_series(request=request)

# Save response to logfile
logfile = "monitoring_response2.log"
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

with open(logfile, "a") as file:
    file.write(f"[{timestamp}] Response:\n{str(response)}\n\n")

print("Response saved to logfile.")
