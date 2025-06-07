import sqlite3
import os


db_path = "./data/fantasy_data.db"

if not os.path.exists(db_path):
    open(db_path, 'w').close()

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS player (
        player_id INTEGER NOT NULL, 
        gsis_id VARCHAR, 
        first_name VARCHAR NOT NULL, 
        last_name VARCHAR NOT NULL, 
        position VARCHAR NOT NULL,
        last_changed_date DATE NOT NULL, 
        PRIMARY KEY (player_id)
);
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS performance (
        performance_id INTEGER NOT NULL, 
        week_number VARCHAR NOT NULL, 
        fantasy_points FLOAT NOT NULL, 
        player_id INTEGER NOT NULL, 
        last_changed_date DATE NOT NULL,
        PRIMARY KEY (performance_id), 
        FOREIGN KEY(player_id) REFERENCES player (player_id)
);
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS league (
        league_id INTEGER NOT NULL, 
        league_name VARCHAR NOT NULL, 
        scoring_type VARCHAR NOT NULL,
        last_changed_date DATE NOT NULL,  
        PRIMARY KEY (league_id)
);
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS team (
        team_id INTEGER NOT NULL, 
        team_name VARCHAR NOT NULL, 
        league_id INTEGER NOT NULL, 
        last_changed_date DATE NOT NULL, 
        PRIMARY KEY (team_id), 
        FOREIGN KEY(league_id) REFERENCES league (league_id)
);
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS team_player (
        team_id INTEGER NOT NULL, 
        player_id INTEGER NOT NULL, 
        last_changed_date DATE NOT NULL,
        PRIMARY KEY (team_id, player_id), 
        FOREIGN KEY(team_id) REFERENCES team (team_id), 
        FOREIGN KEY(player_id) REFERENCES player (player_id)
);
''')

conn.commit()
conn.close()