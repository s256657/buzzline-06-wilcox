"""
visualization_wilcox.py

Consumes messages from Kafka and live-updates a matplotlib visualization.
Each point on the plot represents a team:
- X-axis: Average yards per play
- Y-axis: Pass percentage
"""

import json
import matplotlib.pyplot as plt
from collections import defaultdict
from kafka import KafkaConsumer

# Local utilities
import utils.utils_config as config
from utils.utils_logger import logger
from utils.utils_consumer import create_kafka_consumer

# Global state tracking
team_stats = defaultdict(lambda: {"plays": 0, "pass": 0, "total_yards": 0})


def update_stats(team, action, yards):
    team_data = team_stats[team]
    team_data["plays"] += 1
    team_data["total_yards"] += yards
    if action == "pass":
        team_data["pass"] += 1


def calculate_plot_data():
    x = []
    y = []
    labels = []

    for team, stats in team_stats.items():
        if stats["plays"] == 0:
            continue
        avg_yards = stats["total_yards"] / stats["plays"]
        pass_pct = stats["pass"] / stats["plays"]
        x.append(avg_yards)
        y.append(pass_pct)
        labels.append(team)

    return x, y, labels


def plot_live(ax):
    ax.clear()
    x, y, labels = calculate_plot_data()

    ax.set_title("Team Efficiency & Strategy")
    ax.set_xlabel("Average Yards per Play")
    ax.set_ylabel("Pass Percentage")

    ax.set_xlim(-10, 50)
    ax.set_ylim(0, 1)

    ax.grid(True, linestyle="--", alpha=0.5)

    # Optional: Visual zones/quadrants (comment out if not needed)
    ax.axhline(0.5, color="gray", linestyle="--", linewidth=1)
    ax.axvline(5, color="gray", linestyle="--", linewidth=1)
    ax.axhspan(0.5, 1.0, facecolor='lightyellow', alpha=0.2)
    ax.axvspan(5, 50, facecolor='lightblue', alpha=0.1)

    # Plot points
    ax.scatter(x, y, color="blue", s=100, edgecolors="black", alpha=0.8)

    # Enhanced annotations
    for i, label in enumerate(labels):
        ax.annotate(
            label,
            (x[i], y[i]),
            textcoords="offset points",
            xytext=(8, 8 if i % 2 == 0 else -12),  # Alternate offsets to avoid overlap
            ha='left',
            fontsize=8,
            bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.9, ec="gray"),
            arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=0.2", color="gray", lw=0.8)
        )

    plt.pause(0.01)


def consume_and_visualize():
    topic = config.get_kafka_topic()
    kafka_url = config.get_kafka_broker_address()
    group_id = config.get_kafka_consumer_group_id()

    logger.info(f"Kafka topic: {topic}")
    logger.info(f"Kafka broker: {kafka_url}")

    consumer = create_kafka_consumer(
        topic_provided=topic,
        group_id_provided=group_id + "_visualizer",
        value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
    )

    plt.ion()
    fig, ax = plt.subplots(figsize=(10, 6))

    try:
        for msg in consumer:
            data = msg.value
            team = data.get("team")
            action = data.get("action")
            yards = int(data.get("yards", 0))

            update_stats(team, action, yards)
            plot_live(ax)

    except KeyboardInterrupt:
        logger.warning("Visualization interrupted by user.")
    finally:
        plt.ioff()
        plt.show()


if __name__ == "__main__":
    consume_and_visualize()