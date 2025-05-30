import sqlite3
import pandas as pd
import os

db_path = "./data/fantasy_data.db"

# Enable foreign key support and connect
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("PRAGMA foreign_keys = ON;")

# Load each CSV and insert into the respective table
def import_csv_to_table(csv_file, table_name):
    if not os.path.exists(csv_file):
        print(f"File not found: {csv_file}")
        return
    df = pd.read_csv(csv_file)

    # Insert DataFrame into table
    try:
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Inserted data into table '{table_name}' from '{csv_file}'.")
    except Exception as e:
        print(f"Error inserting into {table_name} from {csv_file}: {e}")

# Define the CSV files and target tables
csv_table_map = {
    "player": "./data/player_data.csv",
    "league": "./data/league_data.csv",
    "team": "./data/team_data.csv",
    "performance": "./data/performance_data.csv",
    "team_player": "./data/team_player_data.csv"
}

# Maintain the correct order due to foreign key dependencies
table_order = ["player", "league", "team", "performance", "team_player"]

# Process the CSV files
for table in table_order:
    import_csv_to_table(csv_table_map[table], table)

conn.commit()
conn.close()