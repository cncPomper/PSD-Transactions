import json
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from datetime import datetime

server = 'localhost:9092'
topic_name = 'alarm-final'

consumer = KafkaConsumer(
	topic_name,
	bootstrap_servers=[server],
	auto_offset_reset='latest'
)

timestamps_good = []
timestamps_anomaly = []
amounts_good = []
amounts_anomaly = []

print('Starting consumer')
anomaly_count=0
for msg in consumer:
	data = json.loads(msg.value)
	
	amount = float(data['event']['amount'])
	timestamp = float(data['event']['transaction_time'])
	
	
	anomaly_flag = data['anomaly']
	if anomaly_flag =='Y':
		anomaly_count = anomaly_count+1
		print(f'ANOMALY! count: {anomaly_count}', data)
		amounts_anomaly.append(amount)
		timestamps_anomaly.append(timestamp)

	else:
		print('\t', data)
		amounts_good.append(amount)
		timestamps_good.append(timestamp)
	

	plt.clf()
	plt.scatter(timestamps_good, amounts_good, marker='o', color='green')
	plt.scatter(timestamps_anomaly, amounts_anomaly, marker='o', color='red')
	plt.xlabel('Czas')
	plt.ylabel('Wartosc transakcji')
	plt.title('Anomalie transakcji')
	plt.grid(True)
	plt.pause(0.1) 
