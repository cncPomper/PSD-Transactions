# PSD-Transactions

### Authors
Maciej Tymoftyjewicz i Piotr Kitlowski

### Technologies used
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)
![Apache Flink](https://img.shields.io/badge/Apache%20Flink-E6526F?style=for-the-badge&logo=Apache%20Flink&logoColor=white)

### Architektura
![Architektura](/imgs/architektura.png)

## Setup
```bash
https://github.com/sindresorhus/guides/blob/main/docker-without-sudo.md
```
```bash
mkdir bin && cd bin
```
```bash
wget https://github.com/docker/compose/releases/download/v2.30.0/docker-compose-linux-x86_64 -O docker-compose
```
```bash
chmod +x docker-compose
```
```bash
cd ~ && nano .bashrc
```
```bash
export PATH="${HOME}/bin:${HOME}"
```
```bash
source .bashrc
```

## Create venv
```bash
python3 -m venv venv
source venv/bin/activate
pip install kafka-python
```


## Instrukcja uruchomienia
### Uruchomienie docker-compose

```bash
docker-compose up
```

### Inicjalizacja struktur bazy danych
```bash
pgcli -h localhost -p 5432 -u postgres -d postgres
```

```
CREATE TABLE transactions (
	card_id INTEGER NOT NULL,
	user_id INTEGER NOT NULL,
	location_1 DECIMAL(10, 6),
	location_2 DECIMAL(10, 6),
	amount DECIMAL(15, 2),
	card_limit INTEGER,
	transaction_time DECIMAL(17, 4),
	anomaly_flag VARCHAR(1)
)
```

### Uruchomienie Joba Flink
```bash
docker-compose exec jobmanager ./bin/flink run -py /opt/src/job/transactions_job.py --pyFiles /opt/src -d
```

### Uruchomienie producenta danych
```bash
python3 src/producers/card_producer.py
```

### Uruchomienie wizualizatora
```bash
python3 src/consumers/consumer.py
```