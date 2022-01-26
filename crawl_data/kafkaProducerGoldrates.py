import sys
import time
import json
from json import dumps
from kafka import KafkaProducer
from time import sleep
import requests as req

ticker_url="https://forex-data-feed.swissquote.com/public-quotes/bboquotes/instrument/XAU/USD"
brokers=['192.168.31.85:9092', '192.168.31.120:9092', '192.168.31.119:9092']
topic='Goldrates'
sleep_time=2

producer = KafkaProducer(bootstrap_servers=brokers,value_serializer=lambda x: dumps(x).encode('utf-8'))

i = 1
while True:
	print("Getting new data ", i)
	resp = req.get(ticker_url)
	json_data = json.loads(resp.text)
	producer.send(topic, json_data)
	time.sleep(sleep_time)
	i = i + 1
