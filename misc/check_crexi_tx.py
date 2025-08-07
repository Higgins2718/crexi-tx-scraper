import json
import duckdb

'''# count JSON records
with open('crexi_tx_industrial.json') as f:
    data = json.load(f)
json_count = len(data)

# count DuckDB records
conn = duckdb.connect('crexi_tx_industrial.duckdb')
duck_count = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
conn.close()

# confirm match
assert json_count == duck_count, f"Count mismatch: JSON={json_count} vs DuckDB={duck_count}"
print(f"✅ OK: both JSON and DuckDB contain {json_count} listings")
'''


# 1) First listing from JSON
with open('crexi_tx_industrial.json') as f:
    first_json = json.load(f)[0]
print("First JSON listing:")
print(first_json)

# 2) First listing from DuckDB
conn = duckdb.connect('crexi_tx_industrial.duckdb')
first_db = conn.execute("SELECT * FROM listings LIMIT 1").fetchone()
conn.close()
print("\nFirst DB listing (tuple):")
print(first_db)
