import duckdb

MAIN_DB = "databases/crexi_tx_industrial.duckdb"
STOP_DB = "databases/stop_ids.duckdb"

ids = [r[0] for r in duckdb.connect(MAIN_DB).execute(
    "SELECT id FROM listings ORDER BY activatedOn DESC LIMIT 60"
).fetchall()]

con = duckdb.connect(STOP_DB)
con.execute("CREATE OR REPLACE TABLE stop_ids (id BIGINT)")
con.executemany("INSERT INTO stop_ids VALUES (?)", [(i,) for i in ids])
con.close()

print(f"Updated stop_ids with {len(ids)} rows.")
