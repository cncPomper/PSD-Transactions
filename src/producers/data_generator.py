import random
from enum import Enum
from dataclasses import dataclass
import time

@dataclass
class User:
    user_id: int
    base_location: tuple([float, float])


@dataclass
class Card:
    card_id: int
    user: User
    base_amount: int
    limit: int


class Anomalies(Enum):
    VALUE_ANOMALY = 1  # Anomalia wartosci, gdy wartosc przekracza pewna wielokrotnosc sredniej wartosci
    LIMIT_ANOMALY = 2  # Anomalia gdy przekroczony zostaje limit
    GEO_ANOMALY = 3  # Anomalia lokalizacji, gdy zmienia sie ona o pewna liczbe stopni geograficznych
    FREQ_ANOMALY = 4 # Anomalia czestotliwosci transakcji, gdy przekracza ona wielokrotnie srednia czestotliwosc
    CLOSE_TO_LIMIT_ANOMALY = 5 # Anomalia limitu - wiele transakcji blisko limitu w krotkim czasie
    CARD_COPY_ANOMALY = 6  # Dwie transakcje symultaniczne w odleglych lokalizacjach
    MICRO_ANOMALY = 7  # Anomalia wielu mikrotransakcji typu 1zl z rzedu w krotkim odstepie czasu
    ROUNDED_ANOMALY = 8  # Kilka transakcji o wartosciach zakraglonych typu 1000.00, 500.00, 800.00
    IDENTICAL_ANOMALY = 9  # Wiele identycznych transakcji z rzedu w krotkim czasie
    LIMIT_CHANGE_ANOMALY = 10  # Nagla zmiana limitu i wysoka transakcja bliska nowego limitu

def generate_basic_data(card: Card):

    # Akceptowalne odchylenie lokalizacji przyjmijmy 1 stopni dlugosci i 1 stopni szerokosci
    # Odchylenie standardowe 1/3 i 1/3 (99.7% wartosci zawiera sie do 3*stdev, reszte ucinamy)
    # transaction_location = (None, None)
    transaction_location_1 = max(card.user.base_location[0] - 1, min(card.user.base_location[0] + 1, max(-90, min(90, random.gauss(card.user.base_location[0], 1 / 3)))))
    transaction_location_2 = max(card.user.base_location[1] - 1, min(card.user.base_location[1] + 1, max(-180, min(180, random.gauss(card.user.base_location[1], 1 / 3)))))

    # Przyjmowalne odchylenie do 100% kwoty (dla bazowego 100zl bedzie to zakres [0 - 200])
    # stddev = 33%
    transaction_amount = min(200, max(0, random.gauss(card.base_amount, card.base_amount/3)))

    return {"card_id": card.card_id, "user_id": card.user.user_id,"location_1": transaction_location_1, "location_2": transaction_location_2, "amount": transaction_amount, "card_limit": card.limit, 'transaction_time': time.time() * 1000}


def apply_anomaly(data, card):
    if random.randint(1, 100) >= 90:
        # Nalozenie anomalii
        anomaly = random.choice(list(Anomalies))
        if anomaly == Anomalies.VALUE_ANOMALY: #done
            data["amount"] = 10 * card.base_amount
        elif anomaly == Anomalies.LIMIT_ANOMALY: #done
            data["amount"] = 1 + card.limit
        elif anomaly == Anomalies.GEO_ANOMALY: #done
            data["amount"] = 150
            data["location_1"] = -data["location_1"] 
            data["location_2"] = -data["location_2"] 
        elif anomaly == Anomalies.FREQ_ANOMALY: #done
            data["amount"] = card.base_amount * 1.5
        elif anomaly == Anomalies.CLOSE_TO_LIMIT_ANOMALY: #done
            data["amount"] = card.limit - 1
        elif anomaly == Anomalies.CARD_COPY_ANOMALY: #done
            data["amount"] = card.base_amount * 2
        elif anomaly == Anomalies.MICRO_ANOMALY: #done
            data["amount"] = 0.99
        elif anomaly == Anomalies.ROUNDED_ANOMALY: #done
            data["amount"] = 300.0
        elif anomaly == Anomalies.IDENTICAL_ANOMALY: #done
            data["amount"] = 250.0
        elif anomaly == Anomalies.LIMIT_CHANGE_ANOMALY: #done
            pass

        return data, anomaly

    return data, None
