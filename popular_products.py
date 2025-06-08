import os
import json
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy

if 'FLINK_HOME' not in os.environ:
    os.environ['FLINK_HOME'] = '/opt/flink'


def extract_product_id(record):
    try:
        if not record or record.strip() == '':
            return "Empty record"

        data = json.loads(record)

        event_time = data.get('event_time', 'unknown')
        event_type = data.get('event_type', 'unknown')
        product_id = data.get('product_id', 'unknown')
        brand = data.get('brand', 'unknown')
        price = data.get('price', 'unknown')
        user_id = data.get('user_id', 'unknown')
        session = data.get('user_session', 'unknown')

        return (
            f"[{event_time}] Event: {event_type}, "
            f"Product ID: {product_id}, Brand: {brand}, Price: {price}, "
            f"User ID: {user_id}, Session: {session}"
        )
    except json.JSONDecodeError as e:
        return f"JSON parse error: {str(e)}, Record: {record[:100]}"
    except Exception as e:
        return f"Processing error: {str(e)}, Record: {record[:100]}"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    print("Using modern KafkaSource API...")

    try:
        kafka_source = KafkaSource.builder() \
            .set_bootstrap_servers("kafka:29092") \
            .set_topics("shopping-events") \
            .set_group_id("shopping-consumer") \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        stream = env.from_source(
            source=kafka_source,
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="Kafka Source"
        )

        processed_stream = stream.map(extract_product_id, output_type=Types.STRING())
        processed_stream.print()

        env.execute("Popular Products Consumer (New API)")

    except Exception as e:
        print(f"Error running Flink job: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
