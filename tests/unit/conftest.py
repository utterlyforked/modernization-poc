"""Make api/ and cdc-consumer/ importable, stubbing deps that are not installed."""
import os
import sys
import types

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
for sub in ("api", "cdc-consumer"):
    sys.path.insert(0, os.path.join(ROOT, sub))

try:
    import confluent_kafka  # noqa: F401
except ImportError:
    stub = types.ModuleType("confluent_kafka")
    stub.Consumer = object
    stub.TopicPartition = lambda topic, partition, offset: (topic, partition, offset)
    stub.KafkaError = type("KafkaError", (), {"_PARTITION_EOF": -191})
    sys.modules["confluent_kafka"] = stub
