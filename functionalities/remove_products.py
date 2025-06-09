import json
import redis
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import WatermarkStrategy
from pyflink.common.time import Time
from pyflink.datastream.window import SlidingProcessingTimeWindows


def write_to_redis(record):
    product_id, count = record
    try:
        r = redis.Redis(host='redis', port=6379, db=0)
        r.set(f"removed:{product_id}", count)
    except Exception as e:
        print(f"[Redis ERROR] {e}")


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("shopping-events") \
        .set_group_id("removed-products-consumer") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    removed_stream = stream \
        .map(lambda value: json.loads(value), output_type=Types.MAP(Types.STRING(), Types.STRING())) \
        .filter(lambda data: data.get("event_type") == "remove_from_cart") \
        .map(lambda data: (data.get("product_id"), 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))

    top_removed = removed_stream \
        .key_by(lambda x: x[0]) \
        .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1))) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))

    top_removed.map(lambda x: write_to_redis(x))

    env.execute("Removed Products Tracker")


if __name__ == "__main__":
    main()
