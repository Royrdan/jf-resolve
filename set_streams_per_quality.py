import sqlite3
import json

conn = sqlite3.connect('data/jfresolve.db')
cursor = conn.cursor()

key = 'streams_per_quality'
value = 2
json_value = json.dumps(value)

# Check if exists
cursor.execute("SELECT id FROM settings WHERE key=?", (key,))
if cursor.fetchone():
    cursor.execute("UPDATE settings SET value=? WHERE key=?", (json_value, key))
    print(f"Updated {key} to {value}")
else:
    cursor.execute("INSERT INTO settings (key, value) VALUES (?, ?)", (key, json_value))
    print(f"Inserted {key} with value {value}")

conn.commit()
conn.close()
