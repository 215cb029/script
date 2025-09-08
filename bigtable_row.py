

from google.cloud import bigtable
from google.cloud.bigtable import row_filters
from google.cloud.bigtable import column_family
from google.cloud.bigtable.row_data import DEFAULT_RETRY_READ_ROWS
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_set import RowRange

def read_single_row(project_id, instance_id, table_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)

    row_key =  "00000216feb3b95f8097d13fb8d5b801d3b4c69ac13413541e5b93f509bc382a"  # Example row key
    
    row = table.read_row(row_key)

    #print(f'Row Key: {row.row_key}')
    for column_family_name, column_cells in row.cells.items():
        for column_name, cell_list in column_cells.items():
            for cell in cell_list:
                print(f"  Column Family: {column_family_name}")
                print(f"  Column Name: {column_name}")
                print(f"  Value: {cell.value}")
               # print(f"  Timestamp: {cell.timestamp}")
def get_all_rows_from_bigtable(project_id, instance_id, table_id):
    # Initialize a Bigtable client
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)
    # start_row_key =  "00000270c5d096a77c2cd35c77b265f176bbb7d891180df98bf5552a3c0f3a30"  # Example row key
    #
    # row_set = RowSet()
    # row_range = RowRange(start_key=start_row_key, start_inclusive=True)
    # row_set.add_row_range(row_range)
    # # Create a row filter to scan all rows in the table
    partial_rows = table.read_rows(limit=12)


    # Fetch and print the rows
    for row in partial_rows:
        print(f"Row Key: {row.row_key}")
        # for column_family_name, column_cells in row.cells.items():
        #     for column_name, cell_list in column_cells.items():
        #         for cell in cell_list:
        #             print(f"  Column Family: {column_family_name}")
        #             print(f"  Column Name: {column_name}")
        #             print(f"  Value: {cell.value}")
        #            #print(f"  Timestamp: {cell.timestamp}")

def delete_row_from_bigtable(project_id, instance_id, table_id, row_key):
 # Initialize the Bigtable client
 client = bigtable.Client(project=project_id, admin=True)

 # Connect to the Bigtable instance
 instance = client.instance(instance_id)

 # Connect to the table within the instance
 table = instance.table(table_id)

 # Create a mutation to delete the row
 row = table.row(row_key)
 row.delete()  # Delete the entire row

 # Commit the mutation
 row.commit()

 print(f"Row with key '{row_key}' has been deleted from the table.")
def get_latest_col_value(project_id, instance_id, table_id, row_key):
    column_family = "personal_data"
    column_qualifier = "name"

    # Create the Bigtable client and instance
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)

    # Define the filters
    column_filter = row_filters.ColumnQualifierRegexFilter(column_qualifier.encode("utf-8"))
    #limit_filter = row_filters.CellsColumnLimitFilter(1)  # Limit to the latest value

    # Read the row with the filters applied
    row = table.read_row(row_key.encode("utf-8"), filter_=column_filter)

    # Check if the row contains the column family and the column qualifier
    if row and column_family in row.cells and column_qualifier.encode("utf-8") in row.cells[column_family]:
        # Get the latest cell for that column
        cell = row.cells[column_family][column_qualifier.encode("utf-8")][0]  # latest cell
        value = cell.value.decode("utf-8")
        print(f"Latest value of {column_family}:{column_qualifier} in row {row_key} is: {value}")
    else:
        print(f"No value found for {column_family}:{column_qualifier} in row {row_key}")

if __name__ == "__main__":

 project_id = "fks-fstream-compute"
 instance_id = "bigtable-1"
 table_id = "shipment"

 delete_row_from_bigtable(project_id,instance_id,table_id,"00000024d63a54b0fd41ebb4a803986affdf524440f85522cb0f0d1514d8d225")
 #get_all_rows_from_bigtable(project_id, instance_id, table_id)
 #read_single_row(project_id,instance_id,table_id)
 # get_latest_col_value(project_id,instance_id,table_id,'88964C73-44AC-4BB3-9FF3-5BF627D8C171')