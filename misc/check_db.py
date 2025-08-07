import duckdb
DB_PATH = "crexi_tx_industrial.duckdb"
json_path = "crexi_tx_industrial_2025-07-23.json"
con = duckdb.connect(DB_PATH)
# Check total rows in listings table and compare with JSON structure
print("Checking listings table and JSON structure...")
total_listings = con.execute("SELECT COUNT(DISTINCT id) FROM listings").fetchone()[0]
print(f"Total rows in listings table: {total_listings}")
print("listings cols:")
print(con.execute("SELECT name, type FROM pragma_table_info('listings')").fetchall())

print("\njson cols:")
print(con.execute("SELECT name, type FROM pragma_table_info('read_json_auto(?)', ?)", [json_path]).fetchall())
# or:
con.execute("CREATE OR REPLACE TEMP TABLE tmp AS SELECT * FROM read_json_auto(?) LIMIT 0", [json_path])
print(con.execute("SELECT name, type FROM pragma_table_info('tmp')").fetchall())

con.close()
