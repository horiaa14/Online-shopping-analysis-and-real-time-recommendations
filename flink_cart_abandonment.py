from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import Time
from pyflink.datastream import CoProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
import json

Set up Flink environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
env.set_parallelism(1)

Kafka consumer properties
properties = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'cart-abandonment-consumer'
}

Kafka consumer
consumer = FlinkKafkaConsumer(
    topics='shopping-events',
    deserialization_schema=SimpleStringSchema(),
    properties=properties
)

Parse JSON string records
events = env.add_source(consumer).map(
    lambda record: json.loads(record),
    output_type=Types.MAP(Types.STRING(), Types.STRING())
)

Filter cart and purchase events
adds = events.filter(lambda e: e['event_type'] == 'cart')
purchases = events.filter(lambda e: e['event_type'] == 'purchase')

Map to (user_id, product_id) tuples
adds_kv = adds.map(lambda e: (e['user_id'], e['product_id']), output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
purchases_kv = purchases.map(lambda e: (e['user_id'], e['product_id']), output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))

Define CoProcessFunction to detect abandonment
class CartAbandonmentDetector(CoProcessFunction):
    def open(self, runtime_context):
        self.purchase_state = runtime_context.get_state(ValueStateDescriptor("purchased", Types.BOOLEAN()))

    def process_element1(self, add_event, ctx):
        if not self.purchase_state.value():
            ctx.timer_service().register_processing_time_timer(
                ctx.timer_service().current_processing_time() + 5 * 60 * 1000
            )
            self.purchase_state.update(False)

    def process_element2(self, purchase_event, ctx):
        self.purchase_state.update(True)

    def on_timer(self, timestamp, ctx):
        if not self.purchase_state.value():
            user_id, product_id = ctx.get_current_key()
            print(f"[ABANDONED] User {user_id} added product {product_id} to cart but did not purchase it.")

Connect the cart and purchase streams
adds_kv.key_by(lambda x: x)\
    .connect(purchases_kv.key_by(lambda x: x))\
    .process(Ca
