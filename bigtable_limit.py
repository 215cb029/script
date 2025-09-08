from google.cloud import bigtable
import logging
import datetime
import time
from google.cloud import bigtable
import concurrent.futures
from google.cloud import bigtable

import datetime

logging.basicConfig(level=logging.INFO)

def get_first_10_rows_from_bigtable(project_id, instance_id, table_id):
    # Initialize a Bigtable client
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)
    #with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
   # Create a filter to limit the rows fetched
    partial_rows = table.read_rows(limit=2)
    row_key_to_skip = b"000002daeab685b15a4264cfb73e397e6c84758b8a336952d2d22c112a5b60e9"

# Counter to track the number of rows processed
    row_count = 0
    filter_row = []
    # Fetch and print the first 10 rows
    for row in partial_rows:
     print(f"Row Key: {row.row_key}")

        # Exit after printing 10 rows
     if row.row_key == row_key_to_skip:
        logging.info(f"Skipping row with key {row.row_key}")
     continue


    for column_family_name, column_cells in row.cells.items():
            for column_name, cell_list in column_cells.items():
                for cell in cell_list:

                    print(f"{column_family_name}:{column_name}={cell.value}")

                    # Try decoding the cell value as UTF-8, if fails, print the raw byte string








                    # # Handle timestamp conversion, if possible
                    # try:
                    #     # Attempt to convert the timestamp to a datetime object
                    #     timestamp = cell.timestamp
                    #     if timestamp is not None:
                    #         # Ensure that we are not working with invalid timestamps
                    #         timestamp_dt = datetime.datetime.utcfromtimestamp(timestamp / 1e6)
                    #         print(f"  Timestamp: {timestamp_dt}")
                    #     else:
                    #         print("  Timestamp: None")
                    # except OverflowError:
                    #     # If the timestamp conversion fails, print a message instead
                    #     print("  Timestamp: Invalid or out of range")


    # Example usage
# get_first_10_rows_from_bigtable('your_project_id', 'your_instance_id', 'your_table_id')
def transfer_data(row_key):
    destination_client = bigtable.Client(project='fks-fstream-compute', admin=True)

    destination_instance = destination_client.instance('bigtable-1')

    destination_table = destination_instance.table('shipment')  # Destination table to write to


    source_client = bigtable.Client(project='fkp-fsg-bigtable', admin=True)

    source_instance = source_client.instance('fk-fsg-p-bt-ass1-fni-22ak')

    source_table = source_instance.table('mh_shipment_service_shipment')  # Source table to read from

    # Initialize a counter for the rows transferred
    # Loop through the rows from the source table
    #row_key =  "b41d146e6a3d03e7f2d20d8792d41b60670b32c4368ebe9ee4598b505527fb16"  # Example row key

    row = source_table.read_row(row_key)



    new_row = destination_table.row(row_key)

    #For each column family and its columns
    for column_family_name, column_cells in row.cells.items():
        for column_name, cell_list in column_cells.items():
            # Loop through all versions (cells) of the column family
            for cell in reversed(cell_list):
                try:
                    # Get the current timestamp
                    timestamp = datetime.datetime.now()

                except Exception as e:
                    # Log and skip if there's any issue accessing the timestamp
                    logging.warning(f"Skipping invalid timestamp for row {row.row_key}: {e}")
                    continue

                # Add each cell to the new row with the current timestamp
                new_row.set_cell(
                    column_family_name,  # Column Family name
                    column_name,         # Column Name
                    cell.value,          # Cell Value
                    timestamp=timestamp  # Use current timestamp
                )

                # Insert the row into the destination table
                new_row.commit()
                time.sleep(0.5)



            logging.info(f"Row {row.row_key} transferred successfully")
def main():
    client = bigtable.Client(project='fkp-fsg-bigtable', admin=True)

    instance = client.instance('fk-fsg-p-bt-ass1-fni-22ak')

    table = instance.table('mh_shipment_service_shipment')
    rows=table.read_rows(limit=105)
    rows.consume_all()
    row_list = list(rows.rows.values())
    skip_row = row_list[5:]
    print(len(skip_row))
    row_keys = [row.row_key for row in skip_row]

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
     futures = [executor.submit(transfer_data,key) for key in row_keys]
    # row_keys = [row.row_key for row in rows]
    # for key in row_keys:
    #     transfer_data(key)
    #get_first_10_rows_from_bigtable(project_id,instance_id,table_id)
 # transfer_data()
 # transfer_data('00000000fd223797bf1100baa859acc27e5f70176540b93dca11e73ed2aa4490')
 # with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
 #    futures = [executor.submit(transfer_data,row) for row in ft]
 #    for future in concurrent.futures.as_completed(futures):
 #        try:
 #            future.result()  # If any exceptions were raised, this will raise them
 #        except Exception as exc:
 #            logging.error(f"Error occurred while processing a row: {exc}")
 #        else:
 #            logging.info("Row processed successfully.")
if __name__ == "__main__":

 main()
 # print(len(ft))