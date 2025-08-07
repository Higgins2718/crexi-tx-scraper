import duckdb
import json
from openai import OpenAI
from datetime import datetime


API_KEY = "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"

try:
    # Set your API key
    client = OpenAI(api_key=API_KEY)
    # Read JSON file
    with open("crexi_tx_industrial_2025-07-31.json", "r") as f:
        data = json.load(f)
        print(f"Loaded {len(data)} listings from JSON file.")

    
    # Send to o3-mini
    print("Sending data to OpenAI API...")
    response = client.chat.completions.create(
        model="o3",
        messages=[
            {"role": "user", "content": f"Please write a quick analysis based on these new texas industrial real estate listings that were just added to crexi. I am tracking and analyzing the new listings every day. highlight whatever is interesting or unexpected or relevant. use your knowledge about the real world and what's in the news for industrial real estate, especially in texas. Do not attempt to sell me on listings, but rather write a detached analysis.  It's particularly interesting whenever you can draw connections to what is happening in the greater world of texas industrial real estate, using the web and your own knowledge. Here is the data: {json.dumps(data)}"}
        ]
    )

    output_text = response.choices[0].message.content
    print(output_text)
except Exception as e:
    print(f"Error during OpenAI API call: {e}")

try:
    # Save to DuckDB
    conn = duckdb.connect('gpt_analysis.duckdb')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS analyses (
            text_output TEXT,
            created_at TIMESTAMP
        )
    ''')
    conn.execute('INSERT INTO analyses VALUES (?, ?)', [output_text, datetime.now()])
    conn.close()
    print("Saved output to DuckDB database.")

except Exception as e:
    print(f"Error saving to DuckDB: {e}")