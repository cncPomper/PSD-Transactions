import random
from enum import Enum
from dataclasses import dataclass
import time
#from card_producer import Card

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
    VALUE_ANOMALY = 1  # Anomalia wartosci, gdy np. wartosc przekracza 10x srednia wartosc
    LIMIT_ANOMALY = 1  # Anomalia gdy przekroczony zostaje limit, w tym wypadku 2000zl


def generate_basic_data(card: Card):

    # Akceptowalne odchylenie lokalizacji przyjmijmy 10 stopni dlugosci i 5 stopni szerokosci
    # Odchylenie standardowe 5/3 i 10/3 (99.7% wartosci zawiera sie do 3*stdev, reszte ucinamy)
    # transaction_location = (None, None)
    transaction_location_1 = max(card.user.base_location[0] - 5, min(card.user.base_location[0] + 5, max(-90, min(90, random.gauss(card.user.base_location[0], 5 / 3)))))
    transaction_location_2 = max(card.user.base_location[1] - 10, min(card.user.base_location[1] + 10, max(-180, min(180, random.gauss(card.user.base_location[1], 10 / 3)))))

    # Przyjmowalne odchylenie do 900% kwoty (dla bazowego 100zl bedzie to zakres [0 - 1000])
    # stddev = 300%
    transaction_amount = random.gauss(card.base_amount, 3 * card.base_amount)

    return {"card_id": card.card_id, "user_id": card.user.user_id,"location_1": transaction_location_1, "location_2": transaction_location_2, "amount": transaction_amount, "card_limit": card.limit, 'transaction_time': time.time() * 1000}


def apply_anomaly(data, card) -> dict:
    if random.randint(1, 100) == 100:
        # Nalozenie anomalii
        anomaly = random.choice(list(Anomalies))
        if anomaly == Anomalies.VALUE_ANOMALY:
            data["amount"] = 100 * card.base_amount
        elif anomaly == Anomalies.LIMIT_ANOMALY:
            data["amount"] = 1 + card.limit

        return data, True

    return data, False
