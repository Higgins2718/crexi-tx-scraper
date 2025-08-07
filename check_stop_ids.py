import duckdb

STOP_DB = "databases/stop_ids.duckdb"

con = duckdb.connect(STOP_DB)
stop_ids = con.execute("SELECT * FROM stop_ids LIMIT 60").fetchall()
con.close()

print(stop_ids)
# 7/28/25 at 2:45 PM
# [(2008191,), (2011557,), (2032967,), (2032936,), (2032852,), (2032837,), (2032770,), (2032705,), (2032699,), (2032513,)]

# 7/20/25 at 8:48 pm (not sure if correct?)
# {2070343, 2070378, 2070352, 2070609, 2070418, 2070386, 2034069, 2070648, 2070236, 2070269}

