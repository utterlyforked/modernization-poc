#!/usr/bin/env python3
"""
CDC Consumer - Reads from Kafka topics and stages to new system
Then triggers dbt to transform staging -> final tables
"""

import os
import json
import time
import subprocess
from confluent_kafka import Consumer, KafkaError, TopicPartition
import psycopg2
from psycopg2.extras import execute_values

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
DB_CONFIG = {
    'host': os.getenv('NEW_SYSTEM_DB_HOST', 'postgres-new-system'),
    'port': os.getenv('NEW_SYSTEM_DB_PORT', '5432'),
    'dbname': os.getenv('NEW_SYSTEM_DB_NAME', 'new_system'),
    'user': os.getenv('NEW_SYSTEM_DB_USER', 'newuser'),
    'password': os.getenv('NEW_SYSTEM_DB_PASS', 'newpass')
}

# Topics to subscribe to (all 4 legacy systems)
TOPICS = [
    'legacy_a1.public.person',
    'legacy_a2.public.person',
    'legacy_b1.public.person',
    'legacy_b2.public.person'
]

# Tenant mapping from topic to tenant_id
TENANT_MAPPING = {
    'legacy_a1.public.person': 'tenant_a1',
    'legacy_a2.public.person': 'tenant_a2',
    'legacy_b1.public.person': 'tenant_b1',
    'legacy_b2.public.person': 'tenant_b2'
}


# Errors that say the database (not the message) is the problem: the message is retried, never skipped.
TRANSIENT_DB_ERRORS = (psycopg2.OperationalError, psycopg2.InterfaceError)
RETRY_DELAY = 5  # seconds before a failed message is retried

OPERATION_MAP = {'c': 'INSERT', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'INSERT'}


def convert_date(value):
    """Convert Debezium date (days since epoch) to proper date"""
    if value is None:
        return None
    # If it's already a string, return it
    if isinstance(value, str):
        return value
    # If it's an integer (days since epoch), convert it
    if isinstance(value, int):
        from datetime import date, timedelta
        epoch = date(1970, 1, 1)
        return (epoch + timedelta(days=value)).isoformat()
    return value


def parse_event(topic, raw):
    """Parse a raw Debezium message into (tenant_id, operation, row).

    Returns None for a topic with no tenant mapping. `row` is the payload's
    `after` image (None for deletes).
    """
    # Debezium JSON format - parse bytes to JSON
    if isinstance(raw, bytes):
        value = json.loads(raw.decode('utf-8'))
    elif isinstance(raw, str):
        value = json.loads(raw)
    else:
        value = raw

    # Get tenant from topic
    tenant_id = TENANT_MAPPING.get(topic)
    if not tenant_id:
        return None

    # Debezium JSON format has payload wrapper
    payload = value.get('payload', value)

    # Get operation type
    operation = OPERATION_MAP.get(payload.get('op', 'c'), 'INSERT')  # c=create, u=update, d=delete, r=read

    return tenant_id, operation, payload.get('after', {})


class CDCConsumer:
    def __init__(self):
        self.consumer = None
        self.db_conn = None
        self.setup_consumer()
        self.connect_db()

    def setup_consumer(self):
        """Setup Kafka consumer"""
        print(f"Setting up Kafka consumer...", flush=True)
        print(f"Bootstrap servers: {KAFKA_BOOTSTRAP}", flush=True)
        print(f"Topics: {TOPICS}", flush=True)

        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP,
            'group.id': 'cdc-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }

        self.consumer = Consumer(conf)
        self.consumer.subscribe(TOPICS)
        print(f"✅ Subscribed to topics: {TOPICS}", flush=True)

    def connect_db(self):
        """Connect to new system database"""
        print(f"Connecting to database at {DB_CONFIG['host']}...", flush=True)
        while True:
            try:
                self.db_conn = psycopg2.connect(**DB_CONFIG)
                print("✅ Connected to new system database", flush=True)
                self.ensure_dead_letter_table()
                break
            except Exception as e:
                print(f"❌ Failed to connect to database: {e}", flush=True)
                print("Retrying in 5 seconds...", flush=True)
                time.sleep(5)

    def ensure_dead_letter_table(self):
        """Create the table that keeps messages which can never be staged"""
        cursor = self.db_conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS person_dead_letter (
                id SERIAL PRIMARY KEY,
                topic VARCHAR(200) NOT NULL,
                kafka_partition INTEGER NOT NULL,
                kafka_offset BIGINT NOT NULL,
                raw_value TEXT,
                error TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.db_conn.commit()
        cursor.close()

    def reconnect_db(self):
        try:
            self.db_conn.close()
        except Exception:
            pass
        self.connect_db()

    def stage_record(self, tenant_id, after, operation):
        """Stage a record in person_staging table. Raises if the record could not be staged."""
        try:
            cursor = self.db_conn.cursor()

            # Fields come from `after` - handles both Legacy A and Legacy B schemas
            insert_sql = """
                INSERT INTO person_staging
                (tenant_id, source_id, source_table, operation, firstname, surname,
                 date_of_birth, city, data_1, data_2, data_3, data_a, data_b, data_c)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(insert_sql, (
                tenant_id,
                after.get('id'),
                'person',
                operation,
                after.get('firstname'),
                after.get('surname'),
                convert_date(after.get('date_of_birth')),
                after.get('city'),
                after.get('data_1'),
                after.get('data_2'),
                after.get('data_3'),
                after.get('data_a'),
                after.get('data_b'),
                after.get('data_c')
            ))

            self.db_conn.commit()
            cursor.close()
            print(f"✅ Staged record: tenant={tenant_id}, id={after.get('id')}, name={after.get('firstname')} {after.get('surname')}", flush=True)

        except Exception as e:
            print(f"Error staging record: {e}", flush=True)
            self.db_conn.rollback()
            raise

    def dead_letter(self, msg, error):
        """Keep a message that can never be staged, so it is not lost when its offset is committed"""
        raw = msg.value()
        if isinstance(raw, bytes):
            raw = raw.decode('utf-8', errors='replace')
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                "INSERT INTO person_dead_letter (topic, kafka_partition, kafka_offset, raw_value, error) "
                "VALUES (%s, %s, %s, %s, %s)",
                (msg.topic(), msg.partition(), msg.offset(), raw, f"{type(error).__name__}: {error}"))
            self.db_conn.commit()
            cursor.close()
        except Exception:
            self.db_conn.rollback()
            raise
        print(f"☠️  Dead-lettered {msg.topic()}[{msg.partition()}]@{msg.offset()}: {error}", flush=True)

    def retry_later(self, msg):
        """Rewind to a message that was not committed, so it is delivered again after a pause"""
        time.sleep(RETRY_DELAY)
        try:
            self.db_conn.rollback()
        except Exception:
            self.reconnect_db()
        self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))

    def run_dbt_transformation(self):
        """Run dbt to transform staging -> final tables"""
        try:
            print("Running dbt transformation...")
            result = subprocess.run(
                ['dbt', 'run', '--project-dir', '/dbt', '--profiles-dir', '/dbt'],
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                print("dbt transformation completed successfully")
                # Mark staging records as processed
                cursor = self.db_conn.cursor()
                cursor.execute("UPDATE person_staging SET processed = TRUE WHERE processed = FALSE")
                self.db_conn.commit()
                cursor.close()
            else:
                # dbt reports errors on stdout, so log the tail of both
                print(f"dbt transformation failed:\n{result.stdout[-2000:]}\n{result.stderr[-2000:]}")

        except Exception as e:
            print(f"Error running dbt: {e}")

    def consume_messages(self):
        """Main consumer loop"""
        print("Starting CDC consumer...", flush=True)
        staged_count = 0
        last_dbt_run = time.time()
        DBT_INTERVAL = 1  # Run dbt every 1 second (reduced for demo responsiveness)
        poll_count = 0

        try:
            while True:
                msg = self.consumer.poll(timeout=0.5)
                poll_count += 1

                if poll_count % 30 == 0:  # Log every 30 seconds
                    print(f"Polling... staged_count={staged_count}", flush=True)

                if msg is None:
                    # No message, check if we should run dbt
                    if staged_count > 0 and (time.time() - last_dbt_run) >= DBT_INTERVAL:
                        print(f"Running dbt transformation ({staged_count} new records)...", flush=True)
                        self.run_dbt_transformation()
                        staged_count = 0
                        last_dbt_run = time.time()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Consumer error: {msg.error()}")
                        continue

                # Process message. The offset is committed only once the message is staged or dead-lettered;
                # anything else rewinds to it and retries.
                try:
                    print(f"📨 Received message from topic: {msg.topic()}", flush=True)

                    # Get message value
                    value = msg.value()
                    if value is None:
                        print("⚠️  Message value is None, skipping", flush=True)
                        continue

                    try:
                        event = parse_event(msg.topic(), value)

                        if event is None:
                            print(f"❌ Unknown topic: {msg.topic()}", flush=True)
                            continue

                        tenant_id, operation, row = event

                        # Stage the record
                        self.stage_record(tenant_id, row, operation)
                        staged_count += 1
                    except TRANSIENT_DB_ERRORS:
                        raise
                    except Exception as e:
                        # The message itself is bad (unparseable, DELETE with no row, value the table rejects)
                        self.dead_letter(msg, e)

                    # Commit offset
                    self.consumer.commit(msg)

                except Exception as e:
                    print(f"❌ Error processing message, will retry: {e}", flush=True)
                    print(f"Message value type: {type(msg.value())}", flush=True)
                    import traceback
                    traceback.print_exc()
                    self.retry_later(msg)

        except KeyboardInterrupt:
            print("Shutting down consumer...")
        finally:
            self.consumer.close()
            if self.db_conn:
                self.db_conn.close()


if __name__ == '__main__':
    # Wait for services to be ready
    print("=== CDC Consumer Starting ===", flush=True)
    print("Waiting for Kafka and Database...", flush=True)
    time.sleep(30)

    print("Initializing CDC Consumer...", flush=True)
    try:
        consumer = CDCConsumer()
        print("Starting message consumption...", flush=True)
        consumer.consume_messages()
    except Exception as e:
        print(f"FATAL ERROR: {e}", flush=True)
        import traceback
        traceback.print_exc()
        raise
