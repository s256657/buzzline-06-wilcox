""" consumer_wilcox.py 

Has the following functions:
- init_db(config): Initialize the SQLite database and create the 'streamed_messages' table if it doesn't exist.
- insert_message(message, config): Insert a single processed message into the SQLite database.
- group messages by category
- give sentiment score by category

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import sqlite3
import sys
from kafka import KafkaConsumer
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available


def init_db(db_path: pathlib.Path):
    """
    Initialize SQLite DB with streamed_messages and sequence tables.
    """
    logger.info(f"Initializing SQLite DB at {db_path}.")
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS streamed_messages;")
            cursor.execute("DROP TABLE IF EXISTS sequence;")

            cursor.execute(
                """
                CREATE TABLE streamed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    author TEXT,
                    timestamp TEXT,
                    category TEXT,
                    sentiment REAL,
                    keyword_mentioned TEXT,
                    message_length INTEGER
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE sequence (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category TEXT UNIQUE,
                    avg_sentiment REAL
                )
                """
            )
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
            cursor.execute(
                """
                INSERT INTO streamed_messages (
                    message, author, timestamp, category, sentiment, keyword_mentioned, message_length
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message["message"],
                    message["author"],
                    message["timestamp"],
                    message["category"],
                    message["sentiment"],
                    message["keyword_mentioned"],
                    message["message_length"],
                ),
            )
            conn.commit()
        logger.info("Inserted message into database.")
    except Exception as e:
        logger.error(f"Failed to insert message: {e}")


def update_sequence_table(db_path: pathlib.Path):
    """
    Compute average sentiment per category and update sequence table.
    """
    logger.debug("Updating sequence table with average sentiments per category.")
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT category, AVG(sentiment) AS avg_sentiment
                FROM streamed_messages
                GROUP BY category
                """
            )
            rows = cursor.fetchall()

            cursor.execute("DELETE FROM sequence;")
            for category, avg_sentiment in rows:
                cursor.execute(
                    """
                    INSERT INTO sequence (category, avg_sentiment)
                    VALUES (?, ?)
                    """,
                    (category, round(avg_sentiment, 3)),
                )
            conn.commit()
        logger.info("Sequence table updated successfully.")
    except Exception as e:
        logger.error(f"Failed to update sequence table: {e}")


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
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }
        logger.debug(f"Processed message: {processed}")
        return processed
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


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
        sys.exit(11)

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
                update_sequence_table(db_path)  # update averages after each insert
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
        raise
    finally:
        logger.info("Consumer shutting down.")


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


if __name__ == "__main__":
    main()