{
    "graph": {
        "Overall Sonic Pubsub": {
            "mql1": {
                "query": "fetch pubsub_topic | metric 'pubsub.googleapis.com/topic/byte_cost' | align rate(1m) | every 1m | group_by [], [value_byte_cost_aggregate: aggregate(value.byte_cost)] | graph_period 2s",
                "aliasBy": "publish"
            },
            "mql2": {
                "query": "fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/byte_cost' | align rate(1m) | every 1m | group_by [], [value_byte_cost_aggregate: aggregate(value.byte_cost)] | graph_period 2s",
                "aliasBy": "consume (fStream+m3)"
            },
            "mql3": {
                "query": "fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/byte_cost' | filter (metadata.user_labels.channel == 'sonic') | align rate(1m) | every 1m | group_by [], [value_byte_cost_aggregate: aggregate(value.byte_cost)] | graph_period 2s",
                "aliasBy": "consume sco"
            },
            "unit": "bytes(IEC)",
            "service-label": "Overall Pubsub"
        },
        "Overall Specter Pubsub":{
            "mql1": {
                "query": "fetch pubsub_topic | metric 'pubsub.googleapis.com/topic/byte_cost' | align rate(1m) | every 1m | group_by [resource.project_id], [value_byte_cost_aggregate: aggregate(value.byte_cost)] | graph_period 2s",
                "aliasBy": "publish"
            },
            "mql2": {
                "query": "fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/byte_cost' | align rate(1m) | every 1m | group_by [], [value_byte_cost_aggregate: aggregate(value.byte_cost)] | graph_period 2s",
                "aliasBy": "consume (fStream+m3)"
            },
            "mql3": {
                "query": "fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/byte_cost' | filter (metadata.user_labels.channel == 'specter') | align rate(1m) | every 1m | group_by [], [value_byte_cost_aggregate: aggregate(value.byte_cost)] | graph_period 2s",
                "aliasBy": "consume sco"
            },
            "unit": "bytes(IEC)",
            "service-label": "Overall Pubsub"
        }
    }
}