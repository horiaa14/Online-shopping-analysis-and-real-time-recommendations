import os
import json
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer
from pyflink.datastream import StreamExecutionEnvironment, CoProcessFunction
from pyflink.common import WatermarkStrategy
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import RuntimeContext
from pyflink.common.serialization import SerializationSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time

if 'FLINK_HOME' not in os.environ:
    os.environ['FLINK_HOME'] = '/opt/flink'


class CartAbandonmentDetector(CoProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        purchased_desc = ValueStateDescriptor("purchased", Types.BOOLEAN())
        self.purchase_state = runtime_context.get_state(purchased_desc)

    def process_element1(self, add_event, ctx: 'CoProcessFunction.Context', out):
        if self.purchase_state.value() is None:
            self.purchase_state.update(False)
        ctx.timer_service().register_processing_time_timer(
            ctx.timer_service().current_processing_time() + 5 * 1000
        )

    def process_element2(self, purchase_event, ctx: 'CoProcessFunction.Context', out):
        self.purchase_state.update(True)

    def on_timer(self, timestamp, ctx: 'CoProcessFunction.OnTimerContext', out):
        if not self.purchase_state.value():
            user_id, product_id = ctx.get_current_key()
            abandonment_event = json.dumps({
                "user_id": user_id,
                "product_id": product_id,
                "timestamp": timestamp,
                "event": "abandoned_cart"
            })
            out.collect(abandonment_event)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("shopping-events") \
        .set_group_id("combined-consumer") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source"
    )

    # Parse JSON once
    events = stream.map(lambda x: json.loads(x), output_type=Types.MAP(Types.STRING(), Types.STRING()))

    # ========== POPULAR PRODUCTS ==========
    popular_stream = events \
        .filter(lambda e: e.get("event_type") == "cart") \
        .map(lambda e: (e["product_id"], 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))

    popular_json = popular_stream.map(
        lambda x: json.dumps({
            "product_id": x[0],
            "cart_count": x[1],
            "event": "popular_product"
        }),
        output_type=Types.STRING()
    )

    popular_sink = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_record_serializer(SimpleStringSchema()) \
        .set_topic("popular-products") \
        .build()

    popular_json.sink_to(popular_sink)

    # ========== CART ABANDONMENT ==========
    adds = events.filter(lambda e: e["event_type"] == "cart")
    purchases = events.filter(lambda e: e["event_type"] == "purchase")

    adds_kv = adds.map(lambda e: (e["user_id"], e["product_id"]),
                       output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
    purchases_kv = purchases.map(lambda e: (e["user_id"], e["product_id"]),
                                 output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))

    abandonment_events = adds_kv.key_by(lambda x: (x[0], x[1])) \
        .connect(purchases_kv.key_by(lambda x: (x[0], x[1]))) \
        .process(CartAbandonmentDetector(), output_type=Types.STRING())

    abandonment_sink = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_record_serializer(SimpleStringSchema()) \
        .set_topic("abandonment-events") \
        .build()

    abandonment_events.sink_to(abandonment_sink)

    env.execute("Combined Abandonment + Popular Product Job")


if __name__ == "__main__":
    main()