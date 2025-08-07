import duckdb
import json

def explore_crexi_db():
    conn = duckdb.connect('crexi_data.duckdb')
    
    print("=== CREXI DATABASE EXPLORATION ===\n")
    
    # 1. Check total count
    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    print(f"✓ Total listings in database: {total}")
    
    # 2. Let's first see what the actual data looks like
    print("\n🔍 Examining data structure:")
    sample = conn.execute("SELECT * FROM listings LIMIT 1").fetchone()
    columns = [desc[0] for desc in conn.description]
    
    # Let's check the actual structure of locations and types
    sample_json = conn.execute("""
        SELECT 
            locations,
            types,
            json_extract_string(locations, '$[0].city') as city_attempt1,
            locations[1].city as city_attempt2,
            types[1] as first_type
        FROM listings 
        LIMIT 1
    """).fetchone()
    
    print(f"  Locations structure: {type(sample_json[0])}")
    print(f"  Types structure: {type(sample_json[1])}")
    
    # 3. Price statistics (this should work fine)
    print("\n💰 Price Statistics:")
    price_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_listings,
            COUNT(askingPrice) as listings_with_price,
            MIN(askingPrice) as min_price,
            AVG(askingPrice) as avg_price,
            MAX(askingPrice) as max_price
        FROM listings
        WHERE askingPrice > 0
    """).fetchone()
    
    print(f"  Total listings: {price_stats[0]}")
    print(f"  Listings with price: {price_stats[1]}")
    if price_stats[2]:
        print(f"  Price range: ${price_stats[2]:,.0f} - ${price_stats[4]:,.0f}")
        print(f"  Average price: ${price_stats[3]:,.0f}")
    
    # 4. Let's properly extract locations
    print("\n📍 Checking location data:")
    locations_check = conn.execute("""
        SELECT 
            id,
            name,
            locations[1].city as city,
            locations[1].state.code as state,
            locations[1].address as address
        FROM listings
        LIMIT 5
    """).fetchall()
    
    for row in locations_check:
        print(f"  {row[0]}: {row[2]}, {row[3]} - {row[4]}")
    
    # 5. Property types - correct extraction
    print("\n🏢 Property Types:")
    types_data = conn.execute("""
        SELECT 
            id,
            name,
            types[1] as property_type
        FROM listings
        WHERE len(types) > 0
        LIMIT 10
    """).fetchall()
    
    for row in types_data:
        print(f"  {row[0]}: {row[2]}")
    
    # 6. Let's see the raw JSON to understand the structure better
    print("\n📄 Raw JSON structure (first listing):")
    # Save one record to examine
    conn.execute("""
        COPY (SELECT * FROM listings LIMIT 1) 
        TO 'sample_listing.json' (FORMAT JSON)
    """)
    
    with open('sample_listing.json', 'r') as f:
        sample_data = json.load(f)
        print(f"  Locations: {sample_data[0].get('locations', [])[:1]}")  # First location
        print(f"  Types: {sample_data[0].get('types', [])}")
    
    # 7. Correct query for locations and types
    print("\n📋 Sample Listings with correct extraction:")
    samples = conn.execute("""
        SELECT 
            id,
            name,
            askingPrice,
            locations[1].address as address,
            locations[1].city as city,
            locations[1].state.code as state,
            types[1] as property_type,
            status
        FROM listings
        LIMIT 5
    """).fetchall()
    
    for i, row in enumerate(samples, 1):
        print(f"\n  {i}. [{row[0]}] {row[1]}")
        print(f"     Type: {row[6]}")
        print(f"     Status: {row[7]}")
        print(f"     Location: {row[3]}, {row[4]}, {row[5]}")
        print(f"     Price: ${row[2]:,.0f}" if row[2] else "     Price: Not listed")
    
    # 8. Export with correct extraction
    print("\n💾 Exporting to CSV with correct data...")
    conn.execute("""
        COPY (
            SELECT 
                id,
                name,
                askingPrice,
                locations[1].address as address,
                locations[1].city as city,
                locations[1].state.code as state,
                locations[1].state.name as state_name,
                locations[1].zip as zip,
                types[1] as property_type,
                status,
                brokerageName,
                activatedOn,
                offersDueOn
            FROM listings
            ORDER BY askingPrice DESC NULLS LAST
        ) TO 'crexi_listings_corrected.csv' (HEADER, DELIMITER ',')
    """)
    
    print("✓ Exported to crexi_listings_corrected.csv")
    
    conn.close()

def check_json_structure():
    """Let's examine the raw JSON to understand the structure"""
    print("\n=== CHECKING RAW JSON STRUCTURE ===")
    
    with open('crexi_listings.json', 'r') as f:
        data = json.load(f)
        
    print(f"Total listings in JSON: {len(data)}")
    
    # Check first listing
    first = data[0]
    print(f"\nFirst listing keys: {list(first.keys())}")
    
    # Check locations structure
    if 'locations' in first:
        print(f"\nLocations is a: {type(first['locations'])}")
        print(f"Number of locations: {len(first['locations'])}")
        if first['locations']:
            print(f"First location: {first['locations'][0]}")
    
    # Check types structure
    if 'types' in first:
        print(f"\nTypes is a: {type(first['types'])}")
        print(f"Types content: {first['types']}")

if __name__ == "__main__":
    # First check the JSON structure
    check_json_structure()
    
    # Then explore the database with corrected queries
    explore_crexi_db()