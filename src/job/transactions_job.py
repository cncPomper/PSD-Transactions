from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
import json
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TimeWindow
from pyflink.common import Row
from datetime import datetime
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common.typeinfo import RowTypeInfo

import psycopg2


from pyflink.datastream.functions import WindowFunction, RuntimeContext

postgres_config = {
    'host': 'postgres',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
}

class AlarmApplyFunction(WindowFunction):

    def open(self, runtime_context: RuntimeContext):
        self.conn = psycopg2.connect(**postgres_config)
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def apply(self, key, window, inputs):
        card_id = key

        self.cursor.execute(f"SELECT AVG(amount) FROM transactions WHERE anomaly_flag='N' AND card_id = {card_id}")
        result = self.cursor.fetchone()
        avg_amount = result[0] if result and result[0] is not None else 0

        for element in inputs:
            if avg_amount > 0 and element['amount'] > 4 * avg_amount:
                yield json.dumps({"anomaly": 'Y', "event": element, "avg_amount": float(avg_amount)})
            else:
                yield json.dumps({"anomaly": 'N', "event": element, "avg_amount": float(avg_amount)})

def parse_event(value):
    data = json.loads(value)
    return data


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

    # Kafka source
    kafka_consumer = FlinkKafkaConsumer(
        topics='alarm',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'redpanda-1:29092',
            'group.id': 'pyflink-alarm-group'
        }
    )

    # Kafka sink
    kafka_producer = FlinkKafkaProducer(
        topic='alarm-final',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'redpanda-1:29092'}
    )

    # Stream processing
    stream = env.add_source(kafka_consumer)\
        .map(parse_event)

    # Optionally parse JSON and extract data

    # Apply tumbling window
    windowed = (
        stream
        .key_by(lambda e: e['card_id'])  # one global window key
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        .apply(AlarmApplyFunction(), output_type=Types.STRING())

        # .process(lambda context, elements, out: [out.collect(json.dumps(e)) for e in elements])
    )

    # Send result to Kafka
    windowed.add_sink(kafka_producer)

    typed_normals = windowed.map(
    lambda d: Row(
        int(json.loads(d)['event']['card_id']),
        int(json.loads(d)['event']['user_id']),
        float(json.loads(d)['event']['location_1']),
        float(json.loads(d)['event']['location_2']),
        float(json.loads(d)['event']['amount']),
        int(json.loads(d)['event']['card_limit']),
        float(json.loads(d)['event']['transaction_time']),
        json.loads(d)['anomaly']
    ),
    output_type=Types.ROW([
        Types.INT(),  # card_id
        Types.INT(),  # user_id
        Types.DOUBLE(),  # location_1
        Types.DOUBLE(),  # location_2
        Types.DOUBLE(),     # amount
        Types.INT(),     # card_limit
        Types.DOUBLE(),  # transaction_time
        Types.STRING()
    ])
)

    row_type_info = RowTypeInfo(
    [Types.INT(), Types.INT(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.INT(), Types.DOUBLE(), Types.STRING()]
)

    typed_normals.add_sink(
        JdbcSink.sink(
            """
            INSERT INTO transactions
            (card_id, user_id, location_1, location_2, amount, card_limit, transaction_time, anomaly_flag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            row_type_info,
            JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .with_url("jdbc:postgresql://postgres:5432/postgres?user=postgres&password=postgres")
                .with_driver_name("org.postgresql.Driver")
                .build(),
            JdbcExecutionOptions.builder()
                .with_batch_size(5)
                .with_batch_interval_ms(200)
                .build()
        )
    )

    # Execute job
    env.execute("Kafka Alarm Window Processing")

if __name__ == '__main__':
    main()
