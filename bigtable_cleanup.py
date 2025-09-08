import argparse
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_set import RowRange
from google.cloud import bigtable
from google.cloud.bigtable import row_filters
from datetime import datetime, timedelta, timezone
import logging
logging.basicConfig(level=logging.INFO)
from google.cloud.bigtable.row import DirectRow
import itertools
import time
chunk_count=1
def get_args():
    parser = argparse.ArgumentParser(description='Check Where TTL is not effective')
    parser.add_argument('--project_id', help='project id is required', required=True)
    parser.add_argument('--instance_id', help='instance id is required', required=True)
    parser.add_argument('--table_id', help='table id is required', required=True)
    parser.add_argument('--max_bt_size', help='bt size is required', required=True , type=int)
    parser.add_argument('--max_read_size', help='size is required', required=True , type=int)
    parser.add_argument('--bt_sleep_second', help='sleeping time is required', required=True , type=int)
    parser.add_argument('--batch_read_size', help='batch size is required', required=True , type=int)
    parser.add_argument('--batch_sleep_second', help='sleeping time is required', required=True , type=int)
    return parser.parse_args()

def get_table_object():
    args=get_args()
    client = bigtable.Client(project=args.project_id, admin=True)
    instance = client.instance(args.instance_id)
    table = instance.table(args.table_id)
    return table
# This function splits a list of row keys into smaller chunks of a given size for reading purpose.
def get_chunks(partial_rows, chunk_size):
    it = iter(partial_rows)
    while chunk := list(itertools.islice(it, chunk_size)):

        yield chunk


def process_rows(max_read_size, batch_start_key: bytes):
    global chunk_count
    args=get_args()
    chunk_size=args.batch_read_size
    row_set = RowSet()
    row_range = RowRange(start_key=batch_start_key, start_inclusive=True)
    row_set.add_row_range(row_range)
    table=get_table_object()
    partial_rows = list(table.read_rows(row_set=row_set, limit=max_read_size))
    last_row_key = None
    for chunk in get_chunks(partial_rows, chunk_size):
        direct_row_list=[]
        logging.info(f"Starting processing of chunk {chunk_count} with {len(chunk)} rows")
        for row in chunk:
            try:

                latest_update_date=extract_latest_update_at_from_key(row.row_key)
                flag=precondition_check(latest_update_date)


                if flag:
                    logging.info(f"{row.row_key} will delete...")
                    # This creates a DirectRow object for the current row using its row key.
                    direct_row = DirectRow(row_key=row.row_key, table=table)
                    # The delete() method marks the row for deletion from the Bigtable.
                    direct_row.delete()
                    direct_row_list.append(direct_row)
                else:
                    last_row_key = row.row_key
            except Exception as e:
                logging.error(f"Error processing row {row.row_key}: {e}")

        delete_row(direct_row_list)
        chunk_count= chunk_count + 1
    return last_row_key
def precondition_check(latest_update_date):
    microseconds =decode_date(latest_update_date)
    date_time =convert_date_time(microseconds)
    day_difference=get_day_difference_from_today(date_time)
    if day_difference >=30:
        return True
    else:
        return False

def extract_latest_update_at_from_key(row_key):
    column_family = "cf"
    column_qualifier = "updated_at"
    table=get_table_object()

    # Define the filters for updated_at column
    column_filter = row_filters.ColumnQualifierRegexFilter(column_qualifier)
    # Read the row with the filters applied (only update_at column data )
    row = table.read_row(row_key, filter_=column_filter)
    #get the latest update_at column from multiple update_at column
    cell = row.cells[column_family][column_qualifier.encode("utf-8")][0]
    # get the latest update_at column value
    latest_update_date = cell.value
    return latest_update_date


# decode the encoded date & return microseconds
def decode_date(latest_update_date):
    microseconds = int.from_bytes(latest_update_date, byteorder='big', signed=False)
    return microseconds


# convert the microseconds to yyyy-mm-dd hh-mm-ss
def convert_date_time(microseconds):
    timestamp_seconds = microseconds / 1_000_000
    timestamp_dt = datetime.fromtimestamp(timestamp_seconds, timezone.utc)
    time_zone_offset = timedelta(hours=5, minutes=30)
    date_time = timestamp_dt + time_zone_offset
    date_time=date_time.replace(microsecond=0, tzinfo=None)
    return date_time


# check the day difference between today's date & row date
def get_day_difference_from_today(date_time):
    now = datetime.now(timezone.utc)
    time_zone_offset = timedelta(hours=5, minutes=30)
    now_localized = now + time_zone_offset
    if date_time.tzinfo is None:
        date_time = date_time.replace(tzinfo=timezone.utc)
    time_diff = now_localized - date_time
    day_difference = time_diff.days
    return day_difference




# Rows are deleted in chunks to avoid overloading the system, with a pause between each batch.
def delete_row(direct_row_list):
    logging.info(f"start processing delete {len(direct_row_list)} record")
    table=get_table_object()
    args=get_args()
    sleep_second=args.batch_sleep_second


    if direct_row_list:
        table.mutate_rows(direct_row_list)
        logging.info(f"✅ Deleted {len(direct_row_list)} rows...")
        time.sleep(sleep_second)




def main():
    args=get_args()
    max_bt_size=args.max_bt_size
    max_read_size = args.max_read_size
    bt_sleep_second=args.bt_sleep_second
    partition_count = max_bt_size // max_read_size
    remaining_rows = max_bt_size % max_read_size
    batch_start_key = None
    for _ in range(partition_count):
        try:

            batch_start_key= process_rows(max_read_size, batch_start_key)
            time.sleep(bt_sleep_second)
        except Exception as e:
            logging.error(f"Error processing read_rows: {e}")

    if remaining_rows > 0:
        try:
            process_rows(remaining_rows, batch_start_key)
            time.sleep(bt_sleep_second)
        except Exception as e:
            logging.error(f"Error processing read_rows: {e}")
if __name__ == '__main__':
    main()