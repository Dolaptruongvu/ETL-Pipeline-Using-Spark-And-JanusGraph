import csv 
from kafka import KafkaProducer
import json 
import time 
producer = KafkaProducer(bootstrap_servers = '172.27.16.1:9093',value_serializer=lambda v : json.dumps(v).encode('utf-8'))

csv_file = './dataset/london_tube_map.csv'

with open(csv_file,'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        producer.send('station_topic',value={"src":row["src"],"dst":row["dst"]})
        print(f"Send: {row['src']} -> {row['dst']} ")
        time.sleep(0.1)

producer.flush()

producer.close()