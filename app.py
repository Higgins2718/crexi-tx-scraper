'''Jackie--
data visualization
networking
interested in vpn proxy management
database projects good'''

from flask import Flask, render_template, jsonify
import duckdb

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

# Connect to your DuckDB database
def get_db_connection():
    # Adjust the path to your DuckDB file
    conn = duckdb.connect('crexi_tx_industrial.duckdb')
    return conn

# Connect to your DuckDB database
def get_gpt_db_connection():
    # Adjust the path to your DuckDB file
    conn2 = duckdb.connect('gpt_analysis.duckdb')
    return conn2

@app.route('/')
def index():
    conn = get_db_connection()
    # Example query - adjust based on your tables
    result = conn.execute("SELECT * FROM listings ORDER BY activatedOn DESC LIMIT 3").fetchall()
    print("First row:", result[0] if result else "No data")
    #print("Row length:", len(result[0]) if result else 0)

     
    # Get analytics data
    analytics = {}
    
    # Price per sq ft by city (top 10 cities with most listings)
    price_sqft_data = conn.execute("""
        SELECT 
            locations[1]['city'] as city, 
            AVG(askingPrice / squareFootage) as avg_price_per_sqft,
            COUNT(*) as property_count
        FROM listings 
        WHERE locations IS NOT NULL 
            AND askingPrice IS NOT NULL 
            AND squareFootage IS NOT NULL 
            AND squareFootage > 0
        GROUP BY locations[1]['city']
        HAVING COUNT(*) >= 3
        ORDER BY property_count DESC
        LIMIT 10
    """).fetchall()
    analytics['price_per_sqft'] = price_sqft_data
    #print("Price per sqft data:", price_sqft_data)

    # Average square footage by type
    sqft_data = conn.execute("""
        SELECT types[1] as property_type, AVG(squareFootage) as avg_sqft
        FROM listings 
        WHERE types IS NOT NULL AND squareFootage IS NOT NULL
        GROUP BY types[1]
        ORDER BY avg_sqft DESC
    """).fetchall()
    analytics['avg_sqft'] = sqft_data
    #print("Average square footage data:", sqft_data)
    
    # Properties by city (top 10)
    city_data = conn.execute("""
        SELECT locations[1]['city'] as city, COUNT(*) as count
        FROM listings 
        WHERE locations IS NOT NULL
        GROUP BY locations[1]['city']
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()
    analytics['cities'] = city_data
    
    # New listings over time (last 30 days)
    timeline_data = conn.execute("""
        SELECT DATE(activatedOn) as date, COUNT(*) as count
        FROM listings 
        WHERE activatedOn >= CURRENT_DATE - INTERVAL 30 DAYS
        GROUP BY DATE(activatedOn)
        ORDER BY date
    """).fetchall()
    analytics['timeline'] = timeline_data
    #print("City data:", city_data, "Timeline data:", timeline_data)
    conn.close()

    gpt_db = get_gpt_db_connection()
    # Example query - adjust based on your tables
    gpt_analysis = gpt_db.execute("SELECT * FROM analyses ORDER BY created_at DESC LIMIT 1").fetchall()
    print("Latest analysis:", result[0] if result else "No data")
    gpt_db.close()

    return render_template('index.html', data=result, analytics=analytics, gpt_analysis=gpt_analysis)


@app.route('/data')
def get_data():
    conn = get_db_connection()
    # Example query - adjust based on your tables
    result = conn.execute("SELECT * FROM listings LIMIT 10").fetchall()
    conn.close()
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)