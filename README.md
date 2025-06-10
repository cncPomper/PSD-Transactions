# PSD-Transactions

### Authors


### Technologies used
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)
![Apache Flink](https://img.shields.io/badge/Apache%20Flink-E6526F?style=for-the-badge&logo=Apache%20Flink&logoColor=white)

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

## Init to DB
```bash
pgcli -h localhost -p 5432 -u postgres -d postgres
```
Create a table
```
CREATE TABLE transactions (
             card_id VARCHAR(50) NOT NULL,
             user_id VARCHAR(50) NOT NULL,
             location_1 DECIMAL(10, 6),
             location_2 DECIMAL(10, 6),
             amount INTEGER,
             card_limit INTEGER,
             transaction_time TIMESTAMP
         )
```
