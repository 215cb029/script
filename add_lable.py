from google.cloud import pubsub_v1
from google.protobuf.field_mask_pb2 import FieldMask
import logging
import re
import argparse
import concurrent.futures
logging.basicConfig(level=logging.INFO)



# Initialize the Pub/Sub client
publisher_client = pubsub_v1.PublisherClient()
subscriber_client=pubsub_v1.SubscriberClient()

# getting all topics by project id
def list_pubsub_topics(project_id):
    try:
        # Define the project path
        project_path = f"projects/{project_id}"

        # List topics
        topics = publisher_client.list_topics(request={"project": project_path})

        return topics

    except Exception as e:
        logging.error(f"An unexpected error occurred while listing topics: {e}", exc_info=True)
        return None


# parsing topic according to in-hyderabad-1/in-chennai-1
def parse_topic(topics,project_id):
    filter_topic = []
    for topic in topics:
        if topic.name.startswith(f"projects/{project_id}/topics/in-hyderabad-1.dart") or \
                topic.name.startswith(f"projects/{project_id}/topics/in-chennai-1.dart"):
            filter_topic.append(topic)
    return filter_topic


# getting 'org' & 'ns' from topic
def extract_org_ns_from_topic(topic_name,project_id):
    topic_name = topic_name.split(f"projects/{project_id}/topics/")[-1]

    # Match the pattern to extract org and ns
    match = re.match(r"^(in-(hyderabad-1|chennai-1)\.dart)\.(\w+)\.(\w+)\.(\w+)", topic_name)

    org = match.group(4)
    ns = match.group(5)
    return org, ns

# setting labels to topic
def add_labels_to_topics(topic,project_id):



    try:
        org, ns = extract_org_ns_from_topic(topic.name,project_id)

        updated_topic=replace_prefix(topic.name,project_id)
        # Get the existing labels from the topic
        labels = topic.labels or {}

        # Add new label
        labels["org"] = org.lower()
        labels["namespace"] = ns.lower()
        labels["billing_app_id"] = f"{org}__{ns}".lower()
        labels["topic_name"] = updated_topic.lower()

        topic.labels=labels


        # Create the update mask to specify that we want to update the labels
        update_mask = FieldMask(paths=["labels"])
        publisher_client.update_topic(topic=topic, update_mask=update_mask  )
        logging.info("successfully updated label")
    except Exception as e:
        print(topic)
        # logging.error(f"An unexpected error occurred: {e}")

#getting all subscription by project id
def list_pubsub_subscription(project_id):
    try:
        # defining project path
        project_path = f"projects/{project_id}"
        #getting all subscription list
        subscription_list=subscriber_client.list_subscriptions(request={"project": project_path})
        return subscription_list

    except Exception as e:
        logging.error(f"An unexpected error occurred while listing topics: {e}", exc_info=True)
        return None

#filter the subscription whose topic id is present in filter_topics list
def parse_subscription(subscriptions,filter_topics):
    filter_subscription = []
    for subscription in subscriptions:
        topic_path=subscription.topic

        for topic in filter_topics:
            if topic_path==topic.name:
                filter_subscription.append(subscription)
    return filter_subscription

# setting labels to subscription
def add_labels_to_subscription(filter_subscription,project_id):

    for subscription in filter_subscription:
        try:
            topic=subscription.topic
            org, ns= extract_org_ns_from_topic(topic,project_id)
            updated_topic=replace_prefix(topic,project_id)
            # Get the existing labels from the subscription
            labels = subscription.labels or {}

            # Add new label
            labels["org"] = org.lower()
            labels["ns"] = ns.lower()
            labels["billing_app_id"] = f"{org}__{ns}".lower()

            labels["topic_name"] = updated_topic
            subscription.labels=labels

            update_mask = FieldMask(paths=["labels"])
            subscriber_client.update_subscription(subscription=subscription,update_mask=update_mask)
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")

def replace_prefix(topic_name,project_id):
    topic_name = topic_name.split(f"projects/{project_id}/topics/")[-1]
    topic_name=  topic_name.replace(".","-")
    if topic_name.startswith("in-hyderabad-1"):
        return topic_name.replace("in-hyderabad-1", "h", 1).lower()
    elif topic_name.startswith("in-chennai-1"):
        return topic_name.replace("in-chennai-1", "calvin", 1).lower()
    return topic_name


def get_topic(topic):
    if "topic_name" not in topic.labels:
        print(topic.name)


def remove_ns_label_from_topic( topic):


    try:
        # Get the current labels of the topic
        # topic = publisher_client.get_topic(topic.name)
        labels = topic.labels

        # Check if the 'ns' key exists in the labels
        if 'ns' in labels:
            # Remove the 'ns' label
            del labels["ns"]

            # Update the topic with the new labels
            topic.labels = labels

            # Create a field_mask to specify the 'labels' field only
            field_mask = FieldMask(paths=["labels"])

            # Update the topic with the new labels and the field_mask
            publisher_client.update_topic(topic=topic, update_mask=field_mask)
            print(f"Label 'ns' removed from topic: {topic.name}")
        else:
            print(f"*****No label with key 'ns' found in topic: {topic.name}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

def find_not_update_topic(topic):

    labels=topic.labels

    if 'topic_name'  not in labels:
        print(topic.name)
        print()















def main(project_id):

    topics = list_pubsub_topics(project_id)
    filter_topics = parse_topic(topics,project_id)
    print(len(filter_topics))
    # add_labels_to_topics(filter_topics,project_id)
   # with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
       # futures = [executor.submit(add_labels_to_topics,topic,project_id) for topic in filter_topics]
    #
    #  for future in concurrent.futures.as_completed(futures):
    #      future.result()
    # subscriptions=list_pubsub_subscription(project_id)
    # filter_subscription= parse_subscription(subscriptions,filter_topics)
    # add_labels_to_subscription(filter_subscription,project_id)


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Google Cloud Pub/Sub labels script")
    # parser.add_argument('--project_id', required=True, help="The Google Cloud project ID")
    #
    # args = parser.parse_args()
    main("fks-fstream-compute")


