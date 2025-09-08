import json
from datetime import datetime
import pytz
import argparse
import statistics
import pandas as pd
from google.cloud import monitoring_v3


DASHBOARD_FILE = "/Users/manoranjans.vc/idea/TestDemo/gcm_dashboard.json"

#convert IST to UTC
def convert_ist_to_utc(ist_time_str):
    # Define IST and UTC time zones
    ist = pytz.timezone('Asia/Kolkata')
    utc = pytz.utc
    # Parse the input string to datetime
    naive_dt = datetime.strptime(ist_time_str, "%Y-%m-%d %H:%M:%S")
    # Localize to IST
    localized_dt = ist.localize(naive_dt)
    # Convert to UTC
    utc_dt = localized_dt.astimezone(utc)
    # Return as formatted string
    return utc_dt.strftime("%Y/%m/%d-%H:%M:%S")
#Processes mql query results and summarizes statistical data for each metric series.
def summarize_result(results_list,alias,redline,unit,service_label,project_id):
    summaries = {}
    if not results_list:
        print(results_list)
        summaries[alias] = {"project_id":{"value": project_id},"min": None, "max": None, "mean": None, "last": None,"unit":{"value": unit},"service-label":{"value": service_label}}
    else:
        summaries[alias] = {
            "project_id": {
                "value": project_id
            },
            "min": {
                "value": min(results_list),
                "isBreached": min(results_list) > redline
            },
            "max": {
                "value": max(results_list),
                "isBreached": max(results_list) > redline
            },
            "mean": {
                "value": round(statistics.mean(results_list), 2),
                "isBreached": round(statistics.mean(results_list), 2) > redline
            },
            "last": {
                "value": results_list[-1],
                "isBreached": results_list[0] > redline
            },
            "unit": {
                "value": unit
            },
            "service-label":{
                "value": service_label
            }
        }
        return summaries


def run_query(mql_query,project_id):
    client = monitoring_v3.QueryServiceClient()
    project_name = f"projects/{project_id}"

    request = monitoring_v3.QueryTimeSeriesRequest(
        name=project_name,
        query=mql_query
    )
    response = client.query_time_series(request=request)
    list=[]
    for point in response.time_series_data:
        data = point.point_data

        for entry in data:
            for val in entry.values:
                list.append(val.double_value)
    return list

def build_dashboard_table_from_json(summary,start_date,end_date,redline):
    rows = []

    for graph_title, queries in summary.items():
        for query, values in queries.items():
            for alias, stats in values.items():
                row = {
                    "Graph Title": graph_title,
                    "Expression": query.split('graph_period')[0].strip(),
                    "Alias by": alias,
                    "Start_Date":start_date,
                    "End_Date":end_date,
                    "Redline":redline
                }
                project = stats.get("project_id", {})
                row["Project_Id"] = project.get("value", "")
                for stat_name in ["min", "max", "mean", "last"]:
                    stat = stats.get(stat_name, {})
                    value_str = stat.get("value", "")
                    breached = stat.get("isBreached", "")

                    key = stat_name.capitalize()
                    breached_key = f"{key}_isBreached"
                    try:
                         value = round(float(value_str))
                    except ValueError:
                        value = value_str
                    row[key] = value
                    row[breached_key] = breached



                unit_info = stats.get("unit", {})
                row["Unit"] = unit_info.get("value", "")
                service_label = stats.get("service-label", {})
                row["Service-Label"] = service_label.get("value", "")
                rows.append(row)

    df = pd.DataFrame(rows)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    filename = f"dashboard_metrics_{timestamp}.xlsx"
    df.to_excel(filename, index=False)
    print(f"Excel file created: {filename}")

def process(start_time, end_time,redline,isbreached):
    with open(DASHBOARD_FILE) as f:
        gcm_config = json.load(f)

    start_utc = convert_ist_to_utc(start_time)
    end_utc = convert_ist_to_utc(end_time)
    graph = gcm_config["graph"]

    results= {}
    counter = 1
    for title, values in graph.items():
        unit=values["unit"]
        service_label=values["service-label"]
        graph_summary = {}
        for key, value in values.items():
            if key.startswith("mql"):
                query=value["query"]
                alias=value["aliasBy"]
                project_id=value["project"]
                mql_query = f"{query} | within d'{start_utc}', d'{end_utc}'"
                results_list=run_query(mql_query,project_id)
                summary= summarize_result(results_list,alias,redline,unit,service_label,project_id)
                if isbreached is not None:
                    filtered_summary = {}
                    for metric, stat in summary.items():
                        if isinstance(stat, dict):
                            filtered_metrics = {
                                k: v for k, v in stat.items()
                                if isinstance(v, dict) and v.get("isBreached") == isbreached
                            }
                            if "project_id" in stat:
                                filtered_metrics["project_id"] = stat["project_id"]
                            if "unit" in stat:
                                filtered_metrics["unit"] = stat["unit"]
                            if "service-label" in stat:
                                filtered_metrics["service-label"] = stat["service-label"]
                            if filtered_metrics:
                                filtered_summary[metric] = filtered_metrics
                    summary = filtered_summary

                graph_summary[f"{query}{counter}"] = summary
                counter += 1
        results[title] = graph_summary
    return results

if __name__ == "__main__":
      parser = argparse.ArgumentParser(description="Fetch Prometheus metric summaries per subpanel")
      parser.add_argument("--start", required=True, help="Start time in format 'YYYY-MM-DD HH:MM:SS'")
      parser.add_argument("--end", required=True, help="End time in format 'YYYY-MM-DD HH:MM:SS'")
      parser.add_argument("--redline", required=True, type=int, help="Threshold to flag a metric as breached")
      parser.add_argument(
        "--isbreached",
        type=lambda x: x.lower() == "true",
        required=False,
        help="Optional: Filter summary to show only breached (true) or non-breached (false) metrics"
    )

      args = parser.parse_args()
      result_summary=process(args.start,args.end,args.redline,args.isbreached)

      print(json.dumps(result_summary, indent=2))
      build_dashboard_table_from_json(result_summary,args.start,args.end,args.redline)