import sqlite3
import os 
from dotenv import load_dotenv

load_dotenv()

SQLALCHEMY_DATABASE_URL = os.getenv("SQLALCHEMY_DATABASE_URL")
print(SQLALCHEMY_DATABASE_URL)
# Connect to your SQLite database
conn = sqlite3.connect(SQLALCHEMY_DATABASE_URL)
cursor = conn.cursor()

# Execute your SELECT statement
#cursor.execute("select count(*) from performance where last_changed_date > '2024-04-01';")

cursor.execute("select count(*) from league;")

# Fetch all results
rows = cursor.fetchall()

# Print the results
for row in rows:
    print(row)

# Close connection
conn.close()