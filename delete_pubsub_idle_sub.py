
from google.cloud import pubsub_v1

def delete_subscription(project_id: str, subscription_id: str):

    client = pubsub_v1.SubscriberClient()
    subscription_path = client.subscription_path(project_id, subscription_id)
    print(subscription_path)
    try:
        client.delete_subscription(request={"subscription": subscription_path})
        print(f"Subscription '{subscription_path}' deleted successfully.")
    except Exception as e:
        print(f"Error deleting subscription: {e}")

delete_subscription("fks-fstream-compute", "topic-01-sub")
