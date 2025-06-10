from kafka import KafkaConsumer
import json

server = 'localhost:9092'
topic_name = 'alarm'

consumer = KafkaConsumer(
	topic_name,
	bootstrap_servers=[server],
	auto_offset_reset='latest',
	group_id='consumer-first-0'
)

print('Starting consumer')
for msg in consumer:
	data = json.loads(msg.value)
	amount = data['amount']
	card_limit = data['card_limit']
	if amount > card_limit:
		print(f"There is a scamer or hacker. The amount is {amount}. The position {data['location_1']}/{data['location_2']}")
