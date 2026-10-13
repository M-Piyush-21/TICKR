"""Airflow-path compatibility wrapper. Logic lives in src/kafka/producer."""

from _delegate import delegate

if __name__ == "__main__":
    delegate("src/kafka/producer/stream_data_producer.py")
