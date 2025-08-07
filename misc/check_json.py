import json

# Read yesterday's JSON
with open('crexi_tx_industrial_2025-07-28.json', 'r') as f:
    yesterday_data = json.load(f)

# Read today's JSON  
with open('crexi_tx_industrial_2025-07-29.json', 'r') as f:
    today_data = json.load(f)

# Extract IDs
yesterday_ids = [item['id'] for item in yesterday_data]
today_ids = [item['id'] for item in today_data]

print("Yesterday IDs:", yesterday_ids)
print("\nToday IDs:", today_ids)

# Check overlap
overlap = set(yesterday_ids) & set(today_ids)
print(f"\nOverlap: {len(overlap)} IDs")
print("Overlapping IDs:", overlap)

print(f"\nYesterday count: {len(yesterday_ids)}")
print(f"Today count: {len(today_ids)}")