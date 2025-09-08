import json
from datetime import datetime
import pytz
import argparse
import statistics
import pandas as pd
from google.cloud import monitoring_v3

# NOTE: Local system path please replace with your own environment's path when using
DASHBOARD_FILE = "/Users/manoranjans.vc/idea/TestDemo/gcm_dashboard.json"

def convert_ist_to_utc(ist_time_str):
    try:
        ist = pytz.timezone('Asia/Kolkata')
        utc = pytz.utc
        naive_dt = datetime.strptime(ist_time_str, "%Y-%m-%d %H:%M:%S")
        localized_dt = ist.localize(naive_dt)
        utc_dt = localized_dt.astimezone(utc)
        return utc_dt.strftime("%Y/%m/%d-%H:%M:%S")
    except Exception as e:
        print(f"Error converting time: {e}")
        return None

#Processes mql query results and summarizes statistical data for each gcm metric series.
def summarize_result(results_list, alias, redline, unit, service_label, project_id,filter_service_label):
    summaries = {}
    try:
        if not results_list:
            summary = {
                "project_id": {"value": project_id},
                "min": None, "max": None, "mean": None, "last": None,
                "unit": {"value": unit},
                "service-label": {"value": service_label}
            }
        else:
            mean_val = round(statistics.mean(results_list), 2)
            summary = {
                "project_id": {"value": project_id},
                "min": {"value": min(results_list), "isBreached": min(results_list) > redline},
                "max": {"value": max(results_list), "isBreached": max(results_list) > redline},
                "mean": {"value": mean_val, "isBreached": mean_val > redline},
                "last": {"value": results_list[-1], "isBreached": results_list[0] > redline},
                "unit": {"value": unit},
                "service-label": {"value": service_label}
            }
        if filter_service_label is None or service_label== filter_service_label:
            summaries[alias]=summary
    except Exception as e:
        print(f"Error summarizing results: {e}")
    return summaries
# Executes a range mql query for a specific project.
def run_query(mql_query, project_id):
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
#generate an excel file from summary data
def build_dashboard_table_from_json(summary, start_date, end_date, redline):
    rows = []
    try:
        for graph_title, queries in summary.items():
            for query, values in queries.items():
                for alias, stats in values.items():
                    row = {
                        "Graph Title": graph_title,
                        "Expression": query.split('graph_period')[0].strip(),
                        "Alias by": alias,
                        "Start_Date": start_date,
                        "End_Date": end_date,
                        "Redline": redline
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
                        except (ValueError, TypeError):
                            value = value_str
                        row[key] = value
                        row[breached_key] = breached
                    unit_info = stats.get("unit", {})
                    row["Unit"] = unit_info.get("value", "")
                    service_label = stats.get("service-label", {})
                    row["Service-Label"] = service_label.get("value", "")
                    rows.append(row)

        df = pd.DataFrame(rows)
        if df.empty:
            raise ValueError("No data found in the response. You might have entered the wrong service label check gcm_dashboard.json file.")
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        filename = f"gcm_dashboard_metrics_{timestamp}.xlsx"
        try:
            df.to_excel(filename, index=False)
            print(f"Excel file created: {filename}")
        except Exception as e:
            print(f"Failed to write Excel file: {e}")
    except Exception as e:
        print(f"Error building dashboard table: {e}")

#Executes mql queries for a specified  time range, then summarizes the results.
def process(start_time, end_time, redline, isbreached=None,filter_service_label=None):
    try:
        with open(DASHBOARD_FILE) as f:
            gcm_config = json.load(f)
    except FileNotFoundError:
        print(f"Dashboard config file not found at: {DASHBOARD_FILE}")
        return {}
    except json.JSONDecodeError:
        print("Dashboard config JSON is malformed")
        return {}

    start_utc = convert_ist_to_utc(start_time)
    end_utc = convert_ist_to_utc(end_time)
    graph = gcm_config.get("graph", {})

    results = {}
    counter = 1
    for title, values in graph.items():
        unit = values.get("unit", "")
        service_label = values.get("service-label", "")
        graph_summary = {}
        for key, value in values.items():
            if key.startswith("mql"):
                query = value.get("query", "")
                alias = value.get("aliasBy", "")
                project_id = value.get("project", "")
                mql_query = f"{query} | within d'{start_utc}', d'{end_utc}'"
                results_list = run_query(mql_query, project_id)
                summary = summarize_result(results_list, alias, redline, unit, service_label, project_id,filter_service_label)
                if isbreached is not None:
                    filtered_summary = {}
                    for metric, stat in summary.items():
                        if isinstance(stat, dict):
                            filtered_metrics = {
                                k: v for k, v in stat.items()
                                if isinstance(v, dict) and v.get("isBreached") == isbreached
                            }
                            for meta in ["project_id", "unit", "service-label"]:
                                if meta in stat:
                                    filtered_metrics[meta] = stat[meta]
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
    parser.add_argument("--isbreached", type=lambda x: x.lower() == "true", required=False,
                        help="Optional: Filter summary to show only breached (true) or non-breached (false) metrics")
    parser.add_argument("--servicelabel", required=False,
                        help="Optional: Filter result to show only chosen serviceLabel metrics")

    args = parser.parse_args()
    result_summary = process(args.start, args.end, args.redline, args.isbreached,args.servicelabel)
    build_dashboard_table_from_json(result_summary, args.start, args.end, args.redline)