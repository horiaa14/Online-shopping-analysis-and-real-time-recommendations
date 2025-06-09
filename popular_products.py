import os
import json
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy

if 'FLINK_HOME' not in os.environ:
    os.environ['FLINK_HOME'] = '/opt/flink'

def extract_product_id(record):
    try:
        data = json.loads(record)
        return data.get('product_id', 'unknown')
    except:
        return 'unknown'

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("shopping-events") \
        .set_group_id("shopping-consumer") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source"
    )

    # Extract product_id from JSON event
    product_ids = stream.map(extract_product_id, output_type=Types.STRING())

    # Simple count aggregation: count occurrences per product_id
    counts = product_ids.key_by(lambda pid: pid).reduce(
    lambda a, b: f"{a.split(':')[0]}:{int(a.split(':')[1]) + 1}" if ':' in a else f"{a}:1")

    # Print counts in terminal (simulate real-time updates)
    counts.print()

    env.execute("Real-time Product Counts")

if __name__ == "__main__":
    main()