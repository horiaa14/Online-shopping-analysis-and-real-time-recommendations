import json
import redis
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import WatermarkStrategy

SCORES = {
    "view": 1,
    "cart": 2,
    "purchase": 5,
    "remove_from_cart": -2
}

def update_user_score(event):
    try:
        user_id = str(event.get("user_id"))
        event_type = event.get("event_type")

        if user_id is None or event_type not in SCORES:
            return

        r = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)

        r.incrby(f"user_score:{user_id}", SCORES[event_type])

        event_data_raw = r.get(f"user_events:{user_id}")
        event_data = json.loads(event_data_raw) if event_data_raw else {}

        event_data[event_type] = event_data.get(event_type, 0) + 1
        r.set(f"user_events:{user_id}", json.dumps(event_data))
    except:
        pass

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("shopping-events") \
        .set_group_id("user-activity-score") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    stream \
        .map(lambda value: json.loads(value), output_type=Types.MAP(Types.STRING(), Types.STRING())) \
        .filter(lambda event: event.get("event_type") in SCORES) \
        .map(lambda event: update_user_score(event))

    env.execute("User Activity Score Tracker")

if __name__ == "__main__":
    main()
