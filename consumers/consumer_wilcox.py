"""
consumer_wilcox.py

Consumes messages from a Kafka topic and stores them in a SQLite database.

Expected message format:
{
    "message": "chiefs gained 7 yards against broncos with a run.",
    "author": "system",
    "timestamp": "2025-10-02 18:45:00",
    "message_length": 54,
    "team": "chiefs",
    "yards": 7,
    "action": "run"
}
"""

#####################################
# Import Modules
#####################################

import json
import os
import pathlib
import sqlite3
import sys
from kafka import KafkaConsumer

# Local utilities
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available


#####################################
# Database Functions
#####################################

def init_db(db_path: pathlib.Path):
    """
    Initialize SQLite DB with streamed_messages table.
    """
    logger.info(f"Initializing SQLite DB at {db_path}.")
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS streamed_messages;")

            cursor.execute("""
                CREATE TABLE streamed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    author TEXT,
                    timestamp TEXT,
                    message_length INTEGER,
                    team TEXT,
                    yards INTEGER,
                    action TEXT
                )
            """)

            conn.commit()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize DB: {e}")
        raise


def insert_message(message: dict, db_path: pathlib.Path):
    """
    Insert a processed message into streamed_messages table.
    """
    logger.debug(f"Inserting message into DB: {message}")
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO streamed_messages (
                    message, author, timestamp, message_length, team, yards, action
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                message["message"],
                message["author"],
                message["timestamp"],
                message["message_length"],
                message["team"],
                message["yards"],
                message["action"]
            ))
            conn.commit()
        logger.info("Inserted message into database.")
    except Exception as e:
        logger.error(f"Failed to insert message: {e}")


def process_message(message: dict) -> dict:
    """
    Validate and convert raw Kafka message fields to correct types.
    """
    logger.debug(f"Processing message: {message}")
    try:
        processed = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "message_length": int(message.get("message_length", 0)),
            "team": message.get("team"),
            "yards": int(message.get("yards", 0)),
            "action": message.get("action")
        }
        logger.debug(f"Processed message: {processed}")
        return processed
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


#####################################
# Kafka Consumption
#####################################

def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str,
    db_path: pathlib.Path,
    interval_secs: int,
):
    """
    Consume JSON messages from Kafka topic and update DB in real-time.
    """
    logger.info("Starting Kafka consumer.")

    try:
        verify_services()
    except Exception as e:
        logger.error(f"Kafka service verification failed: {e}")
        sys.exit(11)

    try:
        consumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"Could not create Kafka consumer: {e}")
        sys.exit(12)

    try:
        is_topic_available(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Topic '{topic}' not available: {e}")
        sys.exit(13)

    try:
        for msg in consumer:
            processed = process_message(msg.value)
            if processed:
                insert_message(processed, db_path)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
        raise
    finally:
        logger.info("Consumer shutting down.")


#####################################
# Main
#####################################

def main():
    logger.info("Starting Kafka consumer application.")

    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs = config.get_message_interval_seconds_as_int()
        sqlite_path = config.get_sqlite_path()
        logger.info("Environment variables read successfully.")
    except Exception as e:
        logger.error(f"Failed to read config/environment variables: {e}")
        sys.exit(1)

    # Delete old DB for a fresh start (optional)
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("Deleted old SQLite DB file.")
        except Exception as e:
            logger.error(f"Failed to delete old DB file: {e}")
            sys.exit(2)

    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"Failed to initialize DB: {e}")
        sys.exit(3)

    consume_messages_from_kafka(topic, kafka_url, group_id, sqlite_path, interval_secs)


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()