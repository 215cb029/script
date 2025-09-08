

from google.cloud import bigtable

from google.cloud.bigtable import row_filters

def read_rows(project_id, instance_id,table_id):

    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id) # The table you want to read from
    table = instance.table(table_id)
    row_filter = bigtable.row_filters.CellsColumnLimitFilter(1)
    print("Scanning for all greetings:")
    rows = table.read_rows(filter_=row_filter)
    print(rows)



def main():

    read_rows("fks-fstream-compute", "bigtable-1","emp")

# Running the event loop
if __name__ == "__main__":
    main()

