import duckdb

# open the DB and pull the first 5 rows verbatim
con   = duckdb.connect("crexi_data.duckdb")
rows  = con.execute("SELECT * FROM listings LIMIT 5").fetchdf()  # fetch as DataFrame
print(rows.to_string(index=False))  # pretty print without row indices
con.close()
