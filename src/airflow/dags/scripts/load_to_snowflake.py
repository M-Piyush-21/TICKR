"""Airflow-path compatibility wrapper. Logic lives in src/snowflake/scripts."""

from _delegate import delegate

if __name__ == "__main__":
    delegate("src/snowflake/scripts/load_to_snowflake.py")
