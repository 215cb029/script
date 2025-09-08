import requests
import json
import argparse
import statistics
from datetime import datetime, timedelta, timezone
import pandas as pd

TENANT_ID_BASE_URL = "http://10.83.43.82/fk-mtl-config-manager/apps/app-name"
# NOTE: Local system path please replace with your own environment's path when using
DASHBOARD_FILE = "/Users/manoranjans.vc/idea/TestDemo/dashboard.json"
STEP = "60s"

# convert date time to utc zulu format
def ist_to_utc_zulu(ist_time_str):
    ist = timezone(timedelta(hours=5, minutes=30))
    ist_time = datetime.strptime(ist_time_str, "%Y-%m-%d %H:%M:%S")
    ist_time = ist_time.replace(tzinfo=ist)
    utc_time = ist_time.astimezone(timezone.utc)
    return utc_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

#get tenant id from from aap id
def get_tenant_id(app_id):
    url = f"{TENANT_ID_BASE_URL}/{app_id}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    tenant_id = data.get("id")
    if not tenant_id:
        raise Exception(f" Tenant ID not found for appId: {app_id}")
    return tenant_id

# Executes a range query against a Prometheus-compatible API for a specific tenant.
def query_prometheus(ip, tenant_id, expr, start_time, end_time):
    url = f"http://{ip}/select/{tenant_id}/prometheus/api/v1/query_range"
    payload = {
        "query": expr,
        "start": str(start_time),
        "end": str(end_time),
        "step": STEP
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()



#Processes Prometheus query results and summarizes statistical data for each metric series.
def summarize_result(query_data, redline,unit,service_label,filter_service_label):
    summaries = {}
    try:
        results = query_data["data"]["result"]
        for series in results:
            metric_labels = series.get("metric", {})
            key = str(tuple(sorted(metric_labels.items())))
            values = [float(v[1]) for v in series["values"] if v[1] is not None]
            if not values:
                summary= {"min": None, "max": None, "mean": None, "last": None,"unit":unit,"service_label":service_label}
                continue
            else:
             summary = {
                "min": {
                    "value": min(values),
                    "isBreached": min(values) > redline
                },
                "max": {
                    "value": max(values),
                    "isBreached": max(values) > redline
                },
                "mean": {
                    "value": round(statistics.mean(values), 2),
                    "isBreached": round(statistics.mean(values), 2) > redline
                },
                "last": {
                    "value": values[-1],
                    "isBreached": values[-1] > redline
                },
                "unit": {
                    "value": unit
                },
                "service-label":{
                    "value": service_label
                }
              }
            if filter_service_label is None or service_label== filter_service_label:
                summaries[key]=summary

    except Exception as e:
        return {"error": str(e)}

    return summaries

#Executes Prometheus queries for a specified zone and time range, then summarizes the results.
def run(zone, start_time, end_time, redline, isbreached=None,filter_service_label=None):
    with open(DASHBOARD_FILE) as f:
        zone_config = json.load(f)

    if zone not in zone_config:
        raise ValueError(f"Zone '{zone}' not found in dashboard JSON.")

    start_utc = ist_to_utc_zulu(start_time)
    end_utc = ist_to_utc_zulu(end_time)
    ip = zone_config[zone]["ip"]
    subpanels = zone_config[zone]["sub-panel"]

    results = {}

    for title, panel in subpanels.items():
        app_id = panel["appId"]
        unit=panel["unit"]
        service_label=panel["service-label"]
        tenant_id = get_tenant_id(app_id)
        panel_summary = {}

        for key, expr in panel.items():
            if key.startswith("expr"):
                try:
                    query_data = query_prometheus(ip, tenant_id, expr, start_utc, end_utc)
                    summary = summarize_result(query_data, redline,unit,service_label,filter_service_label)
                    if isbreached is not None:
                        filtered_summary = {}
                        for metric, stat in summary.items():
                            if isinstance(stat, dict):
                                filtered_metrics = {
                                    k: v for k, v in stat.items()
                                    if isinstance(v, dict) and v.get("isBreached") == isbreached
                                }
                                if "unit" in stat:
                                    filtered_metrics["unit"] = stat["unit"]
                                if "service-label" in stat:
                                    filtered_metrics["service-label"] = stat["service-label"]
                                if filtered_metrics:
                                    filtered_summary[metric] = filtered_metrics
                        summary = filtered_summary
                    panel_summary[expr] = summary

                except Exception as e:
                    panel_summary[key] = {"error": str(e)}

        results[title] = panel_summary
    return results
#generate an excel file from summary data
def build_dashboard_table_from_json(summary,start_date,end_date,redline):

    rows = []

    for panel_title, expressions in summary.items():
        for metric_expr, pod_metrics in expressions.items():
            for pod_label, stats in pod_metrics.items():
                row = {
                    "Graph Title": panel_title,
                    "Expression": metric_expr,
                    "Metric Label": pod_label,
                    "Start_Date":start_date,
                    "End_Date":end_date,
                    "Redline":redline
                }
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
        raise ValueError("No data found in the response. You might have entered the wrong zone or service label or zone with service label check dashboard.json file.")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    filename = f"dashboard_metrics_{timestamp}.xlsx"
    df.to_excel(filename, index=False)
    print(f"Excel file created: {filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Prometheus metric summaries per subpanel")
    parser.add_argument("--zone", required=True, help="Zone name (e.g., hyd, calvin)")
    parser.add_argument("--start", required=True, help="Start time in format 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--end", required=True, help="End time in format 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--redline", required=True, type=int, help="Threshold to flag a metric as breached")
    parser.add_argument(
        "--isbreached",
        type=lambda x: x.lower() == "true",
        required=False,
        help="Optional: Filter result to show only breached (true) or non-breached (false) metrics"
    )
    parser.add_argument("--servicelabel", required=False,
                        help="Optional: Filter result to show only chosen serviceLabel metrics")

    args = parser.parse_args()
    summary = run(args.zone, args.start, args.end, args.redline, args.isbreached,args.servicelabel)
    build_dashboard_table_from_json(summary,args.start,args.end,args.redline)
