from kafka.consumer import KafkaConsumer
import pandas as pd
import redis

redis_client = redis.Redis(host='localhost',port=6379, db=0)

consumer = KafkaConsumer('quickstart-events', 
                         bootstrap_servers=['localhost:9092'], 
                         api_version=(0, 10) 
                        )
df = pd.DataFrame(columns=['name','club','age','score'])

for message in consumer:
    counter = 0
    lst = []
    data = message.value.decode("utf-8").replace("\\","").replace("{","").replace("}","").split(',')
    desired_indexes = [1, 3, 10, 12]
    new_data = [data[i] for i in desired_indexes]
    del(data)
    for d in new_data:
        d = d.split(":")
        del(d[1],d[0])
        lst.append(d)
    new_raw_df = pd.DataFrame([lst], columns=df.columns)

    redis_key = str(new_raw_df['name'])
    redis_value = "Score:"+str(new_raw_df["score"]) + "Club:"+str(new_raw_df["club"]) + "Age:"+str(new_raw_df["age"]) 
    redis_client.set(redis_key, redis_value)
    print(redis_key, redis_client.get(redis_key).decode("utf-8"))
    print("\n- - - - - - - - - - - - - - - - - - -")
