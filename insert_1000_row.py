from google.cloud import bigtable
from google.cloud.bigtable.row import DirectRow

# ==== Configuration ====
project_id = 'fks-fstream-compute'
instance_id = 'bigtable-1'
table_id = 'shipment'
column_family_id = 'cf'        # Replace with your actual column family ID
column_qualifier = b'name'      # Column name
column_value_prefix = 'Value '  # Optional: prefix for cell values

# ==== Connect to Bigtable ====
client = bigtable.Client(project=project_id, admin=True)
instance = client.instance(instance_id)
table = instance.table(table_id)

# ==== Generate and Write 100 Rows ====
rows = []

for i in range(1, 1001):  # 1 to 1000
    row_key = f'row{i}'.encode()  # e.g., b'row1', b'row2', ...
    value = f'{column_value_prefix}{i}'.encode()  # e.g., b'Value 1'

    row = DirectRow(row_key=row_key)
    row.set_cell(column_family_id, column_qualifier, value)
    rows.append(row)

# ==== Write All Rows to Bigtable ====
response = table.mutate_rows(rows)

print(f"✅ Imported {len(rows)} rows to Bigtable table '{table_id}'")
