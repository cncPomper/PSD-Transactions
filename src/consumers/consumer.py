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
amounts_good = []

ANOMALIES = {
    "value_anomaly_flag": ("VALUE ANOMALY", "red"),
    "limit_anomaly_flag": ("LIMIT ANOMALY", "orange"),
    "geo_anomaly_flag": ("GEO ANOMALY", "purple"),
    "freq_anomaly_flag": ("FREQ ANOMALY", "blue"),
    "close_to_limit_anomaly_flag": ("CLOSE TO LIMIT ANOMALY", "brown"),
    "card_copy_anomaly_flag": ("CARD COPY ANOMALY", "pink"),
    "micro_anomaly_flag": ("MICRO ANOMALY", "cyan"),
    "rounded_anomaly_flag": ("ROUNDED ANOMALY", "magenta"),
    "identical_anomaly_flag": ("IDENTICAL ANOMALY", "olive"),
    "limit_change_anomaly_flag": ("LIMIT CHANGE ANOMALY", "black"),
    "none": ("NORMAL TRANSACTION", "green"),
}

print('Starting consumer')
anomaly_count = 0
normal_count = 0
anomaly_counters = {key: 0 for key in ANOMALIES if key != "none"}
anomalies_data = {key: {"timestamps": [], "amounts": []} for key in ANOMALIES if key != "none"}

for msg in consumer:
	data = json.loads(msg.value)
	
	amount = float(data['event']['amount'])
	timestamp = float(data['event']['transaction_time'])
	
	
	anomaly_flag = data['anomaly']
	anomaly_type = data['anomaly_name']
	if anomaly_flag =='Y':
		anomaly_count = anomaly_count+1
		anomaly_counters[anomaly_type] += 1
		print(f'ANOMALY! count: {anomaly_count}', data)
		anomalies_data[anomaly_type]["amounts"].append(amount)
		anomalies_data[anomaly_type]["timestamps"].append(timestamp)

	else:
		# print('\t', data)
		normal_count = normal_count+1
		amounts_good.append(amount)
		timestamps_good.append(timestamp)
	

	plt.clf()
	plt.scatter(timestamps_good, amounts_good, marker='o', color='green', label=f"NORMAL TRANSACTION: {normal_count}")

	for anomaly_key, values in anomalies_data.items():
		label, color = ANOMALIES[anomaly_key]
		count = anomaly_counters[anomaly_key]
		if values["timestamps"]:  # tylko jeśli są dane
			plt.scatter(values["timestamps"], values["amounts"], marker='o', color=color, label=f"{label}: {count}")


	plt.xlabel('Czas')
	plt.ylabel('Wartosc transakcji')
	plt.title('Anomalie transakcji')
	plt.grid(True)
	plt.legend(loc='upper left', fontsize='xx-small')
	plt.pause(0.01) 
