import json
import redis
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import WatermarkStrategy
from pyflink.datastream.functions import RuntimeContext


class InterestedUserDetector(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        self.redis = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        try:
            event = json.loads(value)
            event_type = event.get("event_type")
            user_id = event.get("user_id")
            product_id = event.get("product_id")

            if not user_id or not product_id:
                return

            if event_type == "view":
                self.redis.incr(f"views:{user_id}:{product_id}")
            elif event_type == "cart":
                self.redis.set(f"added:{user_id}:{product_id}", 1)

        except:
            pass


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("shopping-events") \
        .set_group_id("interested-user-detector") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    stream \
        .key_by(lambda x: json.loads(x).get("user_id")) \
        .process(InterestedUserDetector(), output_type=Types.STRING())

    env.execute("Detect Interested Users")


if __name__ == "__main__":
    main()
