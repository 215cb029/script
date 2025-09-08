from google.cloud import bigtable
import logging
import datetime
import time

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize Bigtable client for the source project
source_client = bigtable.Client(project='fkp-fsg-bigtable', admin=True)
source_instance = source_client.instance('fk-fsg-p-bt-ass1-fni-22ak')

# Initialize Bigtable client for the destination project
destination_client = bigtable.Client(project='fks-fstream-compute', admin=True)
destination_instance = destination_client.instance('bigtable-1')

# Define the source and destination tables
source_table = source_instance.table('mh_shipment_service_shipment')  # Source table to read from
destination_table = destination_instance.table('shipment')  # Destination table to write to

# Row keys to skip
row_keys_to_skip = [
    b"000000005e39de0ef610a2d48ec1a2247c0dc4cd42575704ebae6593a768c58d",
    b"00000000b2f3312b2b216eefff32bc996f2ef807c9b15e567a19d6612c9700c6"# First row key to skip
      # Second row key to skip
]

def transfer_data():
    # Initialize a counter for the rows transferred
    rows_transferred = 0

    # Read data from the source table
    rows = source_table.read_rows()

    # Loop through the rows from the source table
    for row in rows:
        # Skip the row if its key matches any of the ones to skip
        if row.row_key in row_keys_to_skip:
            logging.info(f"Skipping row with key {row.row_key.decode('utf-8')}")
            continue  # Skip this row and move to the next one

        # Create a row to insert into the destination table
        new_row = destination_table.row(row.row_key)

        # For each column family and its columns
        for column_family_name, column_cells in row.cells.items():
            for column_name, cell_list in column_cells.items():
                # Loop through all versions (cells) of the column family
                for cell in cell_list:


                    # Add each cell to the new row with the original timestamp
                    new_row.set_cell(
                        column_family_name,  # Column Family name
                        column_name,         # Column Name
                        cell.value,          # Cell Value
                        timestamp=datetime.datetime.now()  # Original timestamp from source
                    )

                    # Commit the row and sleep for 1 second after each cell is processed
                    new_row.commit()
                    time.sleep(0.5)

        # Increment the counter
        rows_transferred += 1

        # Stop after transferring the first 9 rows (for testing purposes)
        if rows_transferred == 9:
            break

    # Log the total number of rows transferred
    logging.info(f"Total rows transferred: {rows_transferred}")

if __name__ == "__main__":
    transfer_data()
