from kafka.producer import KafkaProducer
from time import sleep
import json
from datetime import datetime

import psycopg2
import sqlalchemy as db
from sqlalchemy import text
import pandas as pd

from query_text import query

try:
    engine = db.create_engine("postgresql+psycopg2://amin:1234@localhost:5432/postgres")
    conn = engine.connect()
    print("db connected")
except:
    print("db error")

metadata = db.MetaData()

games = db.Table("games", metadata, autoload=True, autoload_with=engine)
clubs = db.Table("clubs", metadata, autoload=True, autoload_with=engine)
players = db.Table("players", metadata, autoload=True, autoload_with=engine)
appearances = db.Table("appearances", metadata, autoload=True, autoload_with=engine)
competitions = db.Table("competitions", metadata, autoload=True, autoload_with=engine)

query_result = conn.execute(text(query))
df = pd.DataFrame(query_result.fetchall())
print("data fetched correctly <3")

df['player_score'] = df.apply(lambda row: (( row['sum_goals']/ (row['sum_minutes_played']/90)) * 0.6 + # to have a good goal scoring ability
                        (row['height_in_cm']/100) * 0.15 +      # to be tall :)))
                        (1 if 18 <= row['age'] <= 23 else 0.5) * 0.1 +  # to be young
                        (0.5 if row['market_value_in_eur'] > 100000000 else 0.75 if 50000000 <= row['market_value_in_eur'] <= 100000000 else 1) * 0.05 + # to have reasonable price
                        (1 if row['current_club_domestic_competition_id'] in ['GB1', 'L1', 'ES1', 'FR1', 'IT1'] else 0.5) * 0.1 + # to play in a reasanable league
                        (0.1 if row['foot'] == 'both' else 0))*100, axis=1).round(2)        # to be both-footed

desired_position = "Centre-Forward"
num_of_candadates = 100
sorted_df = (df[df["sub_position"]==desired_position].sort_values(by='player_score',ascending = False)).head(num_of_candadates)
#print(sorted_df)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0, 10, 1))

for counter in range (1):
    for i in range(num_of_candadates):
        message = (sorted_df[i:i+1]).to_json()
        producer.send('quickstart-events', json.dumps(message).encode('utf-8'))
        sleep(2)
        print("\n Message sent ",i )
