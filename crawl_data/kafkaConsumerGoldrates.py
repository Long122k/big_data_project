# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer

# Import sys module
import sys
import json
from datetime import datetime

#define brokers
brokers=['192.168.31.119:9092', '192.168.31.120:9092', '192.168.31.85:9092']
# Initialize consumer variable and set property for JSON decode
consumer = KafkaConsumer ('Goldrates',bootstrap_servers = brokers,
value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Read data from kafka
file = './goldData/goldData3.json'
data = {}
i = 1
with open(file,'a') as f:
	for message in consumer:
		time = datetime.fromtimestamp(message.timestamp/1000)
		data['time'] = str(time)
		data['value'] = message.value
		json_data=json.dumps(data)
		f.write(json_data)
		f.write(",\n")
		print("Collecting data",i)
		i = i+ 1
# Terminate the script
sys.exit()
