import json
import random
import time
from dataclasses import dataclass
from datetime import datetime

from kafka import KafkaProducer

from data_generator import apply_anomaly, generate_basic_data, User, Card, Anomalies


def serializer(message):
    return json.dumps(message).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=["localhost:9092"], value_serializer=serializer)
topic_name = 'alarm'

if __name__ == "__main__":

    # Generowanie 500 uzytkownikow oraz 1000 kart
    # Losowe przypisanie kart do uzytkownikow
    # Lokalizacje bazowe uzytkownikow losowo z rozkladu normalnego [-90 - 90], [-180 - 180]
    # Bazowe kwoty transakcji na kartach na 100zl
    # Limit na karcie ustawiony na 2000zl
    # Wartosci bazowe sluzyc beda jedynie do generowania danych, nie do ich pozniejszej weryfikacji
    users = [User(id, (random.uniform(-90, 90), random.uniform(-180, 180))) for id in range(1, 501)]
    cards = [Card(id, random.choice([user for user in users]), 100, 2000) for id in range(1, 1001)]

    # Wygeneruj dane dla transakcji
    print("Generated cards and user data:")
    for card in cards:
        print(f"Card ID: {card.card_id}, User ID: {card.user.user_id}, Base Location: {card.user.base_location}")

    print("\nStarted Producer...\n")
    t0 = time.time()
    for i in range(1, 100):
        for card in cards[:5]:
            # Wygeneruj dane typowej transakcji
            data = generate_basic_data(card)

            anomaly = None
            # Zastosuj anomalie, generujemy najpierw 3 poprawne petle
            if i> 10:
                data, anomaly = apply_anomaly(data, card)
            

            if not anomaly:
                print(f"\tNormal transaction @{datetime.now()} | Message = {str(data)}")
                producer.send(topic_name, data)
            else:
                if anomaly == Anomalies.FREQ_ANOMALY:
                    for i in range(10):
                        print(f"FREQ_ANOMALY @{datetime.now()} | Message = {str(data)}")
                        producer.send(topic_name, data)
                if anomaly == Anomalies.CLOSE_TO_LIMIT_ANOMALY:
                    for i in range(3):
                        print(f"CLOSE_TO_LIMIT_ANOMALY @{datetime.now()} | Message = {str(data)}")
                        producer.send(topic_name, data)
                        data['amount'] = data['amount'] - 0.01
                if anomaly == Anomalies.CARD_COPY_ANOMALY:
                    for i in range(2):
                        print(f"CARD_COPY_ANOMALY @{datetime.now()} | Message = {str(data)}")
                        producer.send(topic_name, data)
                        data["ocation_1"] = data["location_1"] + 4 
                        data["location_2"] = data["location_2"] + 4
                if anomaly == Anomalies.MICRO_ANOMALY:
                    for i in range(3):
                        print(f"MICRO_ANOMALY @{datetime.now()} | Message = {str(data)}")
                        producer.send(topic_name, data)
                        data['amount'] = data['amount'] - 0.01
                if anomaly == Anomalies.ROUNDED_ANOMALY:
                    for i in range(3):
                        print(f"ROUNDED_ANOMALY @{datetime.now()} | Message = {str(data)}")
                        producer.send(topic_name, data)
                        data['amount'] = data['amount'] + 100
                if anomaly == Anomalies.IDENTICAL_ANOMALY:
                    for i in range(3):
                        print(f"IDENTICAL_ANOMALY @{datetime.now()} | Message = {str(data)}")
                        producer.send(topic_name, data)
                if anomaly == Anomalies.LIMIT_CHANGE_ANOMALY:
                    for i in range(2):
                        print(f"SUDDEN_LIMIT_CHANGE @{datetime.now()} | Message = {str(data)}")
                        producer.send(topic_name, data)
                        data["card_limit"] = data["card_limit"] * 10
                        data["amount"] = data["card_limit"] * 0.9

                else:
                    print(f"ANOMALY @{datetime.now()} | Message = {str(data)}")
                    producer.send(topic_name, data)

            time.sleep(0.5)
            

    producer.flush()

    t1 = time.time()
    print(f'took {(t1 - t0):.2f} seconds')
