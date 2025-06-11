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

        self.cursor.execute(f"SELECT AVG(query.amount), AVG(query.location_1), AVG(query.location_2) FROM (select * from transactions WHERE anomaly_flag='N' AND card_id = {card_id}) query")
        result = self.cursor.fetchone()
        avg_amount = float(result[0] if result and result[0] is not None else 0)
        avg_loc1 = float(result[1] if result and result[1] is not None else 0)
        avg_loc2 = float(result[2] if result and result[2] is not None else 0)

        self.cursor.execute(f"SELECT AVG(obecne - poprzednie) as srednia FROM (select transaction_time obecne, LAG(transaction_time) OVER (ORDER BY transaction_time ASC) poprzednie from transactions WHERE anomaly_flag='N' AND card_id = {card_id}) query WHERE poprzednie IS NOT NULL")
        result = self.cursor.fetchone()
        avg_time = float(result[0] if result and result[0] is not None else 0)

        self.cursor.execute(f"SELECT card_limit FROM (select * from transactions WHERE anomaly_flag='N' AND card_id = {card_id}) query order by transaction_time desc")
        rresult = self.cursor.fetchone()
        last_limit = float(result[0] if result and result[0] is not None else 0)

        value_anomaly_flag = False
        limit_anomaly_flag = False
        geo_anomaly_flag = False
        freq_anomaly_flag = False
        clost_to_limit_anomaly_flag = False
        card_copy_anomaly_flag = False
        micro_anomaly_flag = False
        rounded_anomaly_flag = False
        identical_anomaly_flag = False
        limit_change_anomaly_flag = False


        for index, element in enumerate(inputs):
            if avg_amount > 0 and element['amount']> 5 * avg_amount:
                value_anomaly_flag = True

            if element['amount'] > element['card_limit']:
                limit_anomaly_flag = True

            if len(list(inputs)) >= 10:
                freq_anomaly_flag = True
            
            if last_limit != 0 and last_limit < element['card_limit'] and element['amount'] > 0.85*element['card_limit'] and element['amount'] < element['card_limit']:
                limit_change_anomaly_flag = True

            if avg_loc1 != 0 and avg_loc2 != 0 and (abs(element['location_1']-avg_loc1) > 2 or abs(element['location_2']-avg_loc2) > 2):
                geo_anomaly_flag = True

            avg_diffs = 0
            transaction_times = [element_["transaction_time"] for element_ in inputs]
            if len(list(inputs)) >= 3:
                diffs = [transaction_times[i] - transaction_times[i-1] for i in range(1, len(transaction_times))]
                avg_diffs = sum(diffs) / len(diffs)
                if avg_diffs < 1/3 * avg_time:
                    if element['amount'] > 0.95*element['card_limit'] and element['amount'] < element['card_limit']:
                        clost_to_limit_anomaly_flag = True
            
            amounts_window = [element_["amount"] for element_ in inputs]
            if len(list(inputs)) >= 3:
                if sum(1 for amnt in amounts_window if amnt <= 1 ) >= 3:
                    micro_anomaly_flag = True
            
            if len(list(inputs)) >= 3:
                if sum(1 for amnt in amounts_window if amnt%100 == 0 ) >= 3:
                    rounded_anomaly_flag = True
            
            if len(list(inputs)) >= 3:
                if sum(1 for amnt in amounts_window if amnt == amounts_window[index]) >= 3:
                    identical_anomaly_flag = True

            locations_1 = [element_["location_1"] for element_ in inputs]
            locations_2 = [element_["location_2"] for element_ in inputs]
            max_diff = 0
            if len(list(inputs)) >= 2:
                for i in range(1, len(list(inputs))):
                    diff1 = locations_1[i] - locations_1[i-1]
                    diff2 = locations_2[i] - locations_2[i-1]
                    max_diff = max(max_diff, max(diff1, diff2))
                if max_diff > 3:
                    card_copy_anomaly_flag = True




            if clost_to_limit_anomaly_flag:
                yield json.dumps({"anomaly": 'Y', "event": element, "avg_amount": float(avg_amount), "avg_time": avg_time, "avg_diffs": avg_diffs})
            elif freq_anomaly_flag:
                yield json.dumps({"anomaly": 'Y', "event": element, "avg_amount": float(avg_amount), "no_trx": len(list(inputs))})
            elif limit_anomaly_flag:
                yield json.dumps({"anomaly": 'Y', "event": element, "avg_amount": float(avg_amount)})
            elif geo_anomaly_flag:
                yield json.dumps({"anomaly": 'Y', "event": element, "avg_loc1": float(avg_loc1), "avg_loc2": float(avg_loc2)})
            elif value_anomaly_flag:
                yield json.dumps({"anomaly": 'Y', "event": element, "avg_amount": float(avg_amount)})
            elif micro_anomaly_flag:
                yield json.dumps({"anomaly": 'Y', "event": element, "avg_amount": float(avg_amount), "amounts_window": amounts_window})
            elif card_copy_anomaly_flag:
                yield json.dumps({"anomaly": 'Y', "event": element, "avg_amount": float(avg_amount), "max_diff": max_diff})
            elif rounded_anomaly_flag:
                yield json.dumps({"anomaly": 'Y', "event": element, "avg_amount": float(avg_amount), "amounts_window": amounts_window})
            elif identical_anomaly_flag:
                yield json.dumps({"anomaly": 'Y', "event": element, "avg_amount": float(avg_amount), "amounts_window": amounts_window, "count": len(list(inputs))})
            elif limit_change_anomaly_flag:
                yield json.dumps({"anomaly": 'Y', "event": element, "avg_amount": float(avg_amount), "last_limit": last_limit})
            else:
                yield json.dumps({"anomaly": 'N', "event": element, "avg_amount": float(avg_amount), "last_limit": last_limit})

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
        .key_by(lambda e: e['card_id']) 
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
