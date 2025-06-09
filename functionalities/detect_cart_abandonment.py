import json
import redis
from datetime import datetime
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import WatermarkStrategy
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import RuntimeContext

ABANDON_THRESHOLD_SECONDS = 60

class SmartCartAbandonmentDetector(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        self.has_cart = runtime_context.get_state(
            ValueStateDescriptor("has_cart", Types.BOOLEAN()))
        self.has_purchase = runtime_context.get_state(
            ValueStateDescriptor("has_purchase", Types.BOOLEAN()))
        self.redis = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        event = json.loads(value)
        event_type = event.get("event_type")
        session = ctx.get_current_key()
        product_id = event.get("product_id")
        event_time = event.get("event_time", "").replace(" UTC", "").strip()

        if not event_time or len(event_time.split()) != 2:
            return

        if event_type == "cart":
            self.has_cart.update(True)
            self.redis.lpush(f"cart:{session}", product_id)
            self.redis.set(f"time:cart:{session}", event_time)
            ctx.timer_service().register_processing_time_timer(
                ctx.timer_service().current_processing_time() + (ABANDON_THRESHOLD_SECONDS * 1000))

        elif event_type == "purchase":
            self.has_purchase.update(True)
            self.redis.lpush(f"purchase:{session}", product_id)
            self.redis.set(f"time:purchase:{session}", event_time)

    def on_timer(self, timestamp, ctx: 'KeyedProcessFunction.OnTimerContext'):
        session = ctx.get_current_key()
        cart_time_str = self.redis.get(f"time:cart:{session}")
        purchase_time_str = self.redis.get(f"time:purchase:{session}")
        fmt = "%Y-%m-%d %H:%M:%S"

        try:
            t_cart = datetime.strptime(cart_time_str, fmt)
        except (ValueError, TypeError):
            return iter([])

        abandoned = False

        if purchase_time_str:
            try:
                t_purchase = datetime.strptime(purchase_time_str, fmt)
                if (t_purchase - t_cart).total_seconds() > ABANDON_THRESHOLD_SECONDS:
                    abandoned = True
            except (ValueError, TypeError):
                abandoned = True
        else:
            abandoned = True

        if abandoned:
            cart_products = self.redis.lrange(f"cart:{session}", 0, -1)
            self.redis.lpush("abandoned_sessions", session)
            self.redis.set(f"abandoned:{session}", json.dumps(cart_products))

        self.has_cart.clear()
        self.has_purchase.clear()

        return iter([])


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("shopping-events") \
        .set_group_id("abandon-detector-smart") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    stream \
        .key_by(lambda x: json.loads(x).get("user_session")) \
        .process(SmartCartAbandonmentDetector(), output_type=Types.STRING())

    env.execute("Smart Cart Abandonment Detector")


if __name__ == "__main__":
    main()
