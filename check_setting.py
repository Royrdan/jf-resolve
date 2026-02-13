import sqlite3
import json

try:
    conn = sqlite3.connect('data/jfresolve.db')
    cursor = conn.cursor()
    cursor.execute("SELECT key, value FROM settings WHERE key='streams_per_quality'")
    row = cursor.fetchone()
    if row:
        key, value = row
        print(f"Key: {key}, Value (Raw): {value}")
        try:
            parsed = json.loads(value)
            print(f"Value (Parsed): {parsed}")
        except:
            print("Value is not valid JSON")
    else:
        print("Setting 'streams_per_quality' not found in database (using default: 2)")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
