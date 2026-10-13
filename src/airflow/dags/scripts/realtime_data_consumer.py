"""Airflow-path compatibility wrapper. Logic lives in src/kafka/consumer."""

from _delegate import delegate

if __name__ == "__main__":
    delegate("src/kafka/consumer/realtime_data_consumer.py")
