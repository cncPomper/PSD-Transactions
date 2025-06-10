import json
import random
import time
from dataclasses import dataclass
from datetime import datetime

from kafka import KafkaProducer

from data_generator import apply_anomaly, generate_basic_data, User, Card


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
    for i in range(1, 10):
        for card in cards:
            # Wygeneruj dane typowej transakcji
            data = generate_basic_data(card)

            # Zastosuj anomalie
            data, anomaly_flag = apply_anomaly(data, card)

            if anomaly_flag:
                print(f"Producing normal transaction @{datetime.now()} | Message = {str(data)}")
            else:
                print(f"Producing ANOMALY @{datetime.now()} | Message = {str(data)}")

            producer.send(topic_name, data)

            time.sleep(0.05)

    producer.flush()

    t1 = time.time()
    print(f'took {(t1 - t0):.2f} seconds')
