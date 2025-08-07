import duckdb

DB_PATH = "crexi_tx_industrial.duckdb"
N = 10  # how many “first listings from last run” you keep

sql = f"""
-- 1) Ensure helper columns exist
ALTER TABLE listings ADD COLUMN IF NOT EXISTS rank_newest_first INTEGER;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS is_first_listing_last_run BOOLEAN DEFAULT FALSE;

-- 2) Clear the old flags
UPDATE listings SET is_first_listing_last_run = FALSE;

-- 3) Rank newest → oldest and flag the first {N}
UPDATE listings
SET rank_newest_first        = sub.rn,
    is_first_listing_last_run = sub.rn <= {N}
FROM (
  SELECT id, ROW_NUMBER() OVER (ORDER BY activatedOn DESC) AS rn
  FROM listings
) AS sub
WHERE listings.id = sub.id;

-- 4) Store those rows in a small table for next run
CREATE OR REPLACE TABLE first_listings_last_run AS
SELECT id, activatedOn, rank_newest_first
FROM listings
WHERE is_first_listing_last_run = TRUE
ORDER BY rank_newest_first;
"""

con = duckdb.connect(DB_PATH)
con.execute(sql)

# Pull the IDs into Python (use this set to know when to stop next scrape)
first_listing_ids_last_run = {
    r[0] for r in con.execute("SELECT id FROM first_listings_last_run").fetchall()
}
print("first_listing_ids_last_run:", first_listing_ids_last_run)
# first_listing_ids_last_run: {2032705, 2032770, 2032513, 2011557, 2032837, 2032967, 2032936, 2032852, 2032699, 2008191}
con.close()
