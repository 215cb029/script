from kafka.admin import KafkaAdminClient
from kafka import KafkaConsumer

admin_client = KafkaAdminClient(
    bootstrap_servers="10.116.25.231:9092",
    client_id='my-python-client'
)

# Get cluster metadata
metadata = admin_client.describe_cluster()
print(f"Cluster ID: {metadata['cluster_id']}")
print(f"Controller: {metadata['controller']}")
print("Brokers:")
for node in metadata['brokers']:
    print(f" - {node.host}:{node.port} (ID: {node.node_id})")

admin_client.close()
