'''import duckdb
con = duckdb.connect("crexi_tx_industrial.duckdb")
cols = [r[0] for r in con.execute("SELECT name FROM pragma_table_info('listings')").fetchall()]
con.close()
print(cols)
'''

import duckdb
'''
DB_PATH = "stop_ids.duckdb"
IDS = [2032705, 2032770, 2032513, 2011557, 2032837,
       2032967, 2032936, 2032852, 2032699, 2008191]

con = duckdb.connect(DB_PATH)
con.execute("CREATE OR REPLACE TABLE stop_ids (id BIGINT)")
con.executemany("INSERT INTO stop_ids VALUES (?)", [(i,) for i in IDS])
con.close()

print("Wrote", len(IDS), "IDs to", DB_PATH)

'''


STOP_IDS_DB_PATH = "stop_ids.duckdb"
con2 = duckdb.connect(STOP_IDS_DB_PATH)
# Pull the IDs into Python (use this set to know when to stop next scrape)
first_listing_ids_last_run = {
    r[0] for r in con2.execute("SELECT * FROM stop_ids").fetchall()
}
print("first_listing_ids_last_run:", first_listing_ids_last_run)
# first_listing_ids_last_run: {2032705, 2032770, 2032513, 2032837, 2011557, 2032967, 2032936, 2032852, 2032699, 2008191}