import sqlite3

# Connect to your SQLite database
conn = sqlite3.connect('./data/fantasy_data.db')
cursor = conn.cursor()

# Execute your SELECT statement
cursor.execute("select count(*) from performance where last_changed_date > '2024-04-01';")

# Fetch all results
rows = cursor.fetchall()

# Print the results
for row in rows:
    print(row)

# Close connection
conn.close()