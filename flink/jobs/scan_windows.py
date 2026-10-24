"""Count parcel scans per hub in 5-minute event-time windows, with late scans to a side output.

    flink run -py /opt/flink/jobs/scan_windows.py --mode baseline --stop-offset 84712

Reads parcel-scans-replay (one partition) from its first offset up to --stop-offset, so the job ends
by itself and every window fires. Event time is the Kafka record timestamp, which the producer sets
to scan_ts; the watermark trails the newest event time by 10 s. Each window counts all scans, the
delivered scans, and the delivered scans after promised_ts, and goes to hub-delivery-5m as JSON. A
scan whose window has already closed when it arrives goes to scans-late.

    baseline   the window closes at the watermark: no allowed lateness, no dedup
    lateness   the window keeps its scans for 20 minutes after the watermark passes its end; each
               late scan in that time fires it again with the updated counts
    dedup      lateness, plus a keyed dedup before the window: the first send of each scan_id
               passes, a retry does not; the seen flag expires 15 minutes after it was written
"""

import argparse
import json

from pyflink.common import Configuration, Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer, KafkaRecordSerializationSchema, KafkaSink, KafkaSource, KafkaTopicPartition,
)
from pyflink.datastream.functions import KeyedProcessFunction, ProcessWindowFunction, RuntimeContext
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

BOOTSTRAP = "kafka:19092"
SOURCE, OUTPUT, LATE = "parcel-scans-replay", "hub-delivery-5m", "scans-late"
WINDOW = Time.minutes(5)
OUT_OF_ORDERNESS = Duration.of_seconds(10)
ALLOWED_LATENESS_MS = {"baseline": 0, "lateness": 20 * 60_000, "dedup": 20 * 60_000}  # scans arrive at most 20 min late
SEEN_TTL = Time.minutes(15)  # a retry follows its first send by 1 to 10 s

# One scan as a tuple: the fields the windows need, in this order.
FIELDS = ["scan_id", "order_id", "hub_id", "scan_type", "scan_ts", "promised_ts", "sent_ts"]
SCAN_ID, HUB_ID, SCAN_TYPE, SCAN_TS, PROMISED_TS = 0, 2, 3, 4, 5


def scan_type():
    """Return a new type of the scan tuple.

    Each use needs its own: PyFlink pickles the late-data tag with its type, and a type the JVM has
    already seen holds Java handles that cannot be pickled.
    """
    return Types.TUPLE([Types.LONG(), Types.LONG(), Types.INT(), Types.STRING(), Types.LONG(), Types.LONG(), Types.LONG()])


def parse(value: str) -> tuple:
    """Return the scan tuple of one JSON record."""
    event = json.loads(value)
    return tuple(event[f] for f in FIELDS)


def as_json(scan: tuple) -> str:
    """Return a scan tuple as JSON, for the side output."""
    return json.dumps(dict(zip(FIELDS, scan)))


class FirstSend(KeyedProcessFunction):
    """Pass the first send of each scan_id and drop its retries; keyed by scan_id."""

    def open(self, runtime_context: RuntimeContext):
        """Create the seen flag, which expires SEEN_TTL after it was written."""
        ttl = (
            StateTtlConfig.new_builder(SEEN_TTL)
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        seen = ValueStateDescriptor("seen", Types.BOOLEAN())
        seen.enable_time_to_live(ttl)
        self.seen = runtime_context.get_state(seen)

    def process_element(self, scan, ctx):
        """Yield the scan unless this scan_id was seen within the TTL."""
        if self.seen.value() is None:
            self.seen.update(True)
            yield scan


class CountScans(ProcessWindowFunction):
    """Count one hub's scans in one window; each firing sends the counts so far."""

    def __init__(self, mode: str):
        """Keep the mode, so every output record says which run made it."""
        self.mode = mode

    def process(self, key, context, elements):
        """Return the window's counts as one JSON record."""
        scans = delivered = late_delivered = 0
        for scan in elements:
            scans += 1
            if scan[SCAN_TYPE] == "delivered":
                delivered += 1
                late_delivered += scan[SCAN_TS] > scan[PROMISED_TS]
        window = context.window()
        yield json.dumps({
            "mode": self.mode, "hub_id": key, "window_start": window.start, "window_end": window.end,
            "scans": scans, "delivered": delivered, "late_delivered": late_delivered,
            "watermark": context.current_watermark(),
        })


def kafka_sink(topic: str) -> KafkaSink:
    """Return an at-least-once JSON sink to one topic."""
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder().set_topic(topic).set_value_serialization_schema(SimpleStringSchema()).build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def main() -> None:
    """Build and run the job for one mode."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=sorted(ALLOWED_LATENESS_MS), required=True)
    parser.add_argument("--stop-offset", type=int, required=True, help="offset after the last record to read")
    parser.add_argument("--watermark-interval-ms", type=int, default=1)
    args = parser.parse_args()

    config = Configuration()
    config.set_string("python.execution-mode", "thread")  # Python runs in the JVM: no bundles to flush per watermark
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)
    env.get_config().set_auto_watermark_interval(args.watermark_interval_ms)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(SOURCE)
        .set_group_id(f"scan-windows-{args.mode}")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_bounded(KafkaOffsetsInitializer.offsets({KafkaTopicPartition(SOURCE, 0): args.stop_offset}))
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    # No timestamp assigner: the source gives each record its Kafka timestamp, which is scan_ts.
    watermarks = WatermarkStrategy.for_bounded_out_of_orderness(OUT_OF_ORDERNESS)
    scans = env.from_source(source, watermarks, SOURCE).map(parse, output_type=scan_type())
    if args.mode == "dedup":
        scans = scans.key_by(lambda scan: scan[SCAN_ID], key_type=Types.LONG()).process(FirstSend(), output_type=scan_type())

    late = OutputTag("late-scans", scan_type())
    counts = (
        scans.key_by(lambda scan: scan[HUB_ID], key_type=Types.INT())
        .window(TumblingEventTimeWindows.of(WINDOW))
        .allowed_lateness(ALLOWED_LATENESS_MS[args.mode])
        .side_output_late_data(late)
        .process(CountScans(args.mode), output_type=Types.STRING())
    )
    counts.sink_to(kafka_sink(OUTPUT))
    # Its own task, not chained to the window: the Flink UI then counts the late scans as its records in.
    late_scans = counts.get_side_output(late).map(as_json, output_type=Types.STRING()).name("late scans").disable_chaining()
    late_scans.sink_to(kafka_sink(LATE))
    env.execute(f"scan_windows {args.mode}")


if __name__ == "__main__":
    main()
