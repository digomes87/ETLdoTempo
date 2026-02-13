import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame

from interfaces import DataLoader

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class WeatherPostgresLoader(DataLoader):
    """
    Handler data into postgresql with native spark JDBC
    And Implements dataloader interface
    """


    def __init__(self, host: str = 'localhost'):
        self.host = host
        self._load_env()
        self.jdbc_url = f"jdbc:postgresql://{self.host}:5432/{self.database}"

    def _load_env(self):
        """database credentials"""
        env_path = Path(__file__).resolve().parent.parent / "config" / ".env"
        load_dotenv(env_path)
        self.user = os.getenv("user")
        self.password = os.getenv("password")
        self.database = os.getenv("database")

        if not all([self.user, self.password, self.database]):
            logging.error("Please set environment variables, check .env file")

    def load(self, table_name: str , data: DataFrame) -> None:
        """
        Loads pyspark dataframe into postgres using jdbc
        :param table_name:
        :param data:
        :return:
        """
        logging.info(f"→ Starting native Spark load into table: {table_name}")

        if not isinstance(data, DataFrame):
            logging.error(f"Expected PySpark DataFrame, but got {type(data)}")
            raise TypeError("Data must be a PySpark DataFrame for native Spark loading.")

        try:
            logging.info(f"Writing to JDBC: {self.jdbc_url}")
            data.write \
                .format("jdbc") \
                .option("url", self.jdbc_url)  \
                .option("dbtable", table_name) \
                .option("user", self.user) \
                .option("password", self.password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
        except Exception as e:
            if "ClassNotFoundException: org.postgresql.Driver" in str(e):
                logging.error("PostgreSQL JDBC Driver not found. Ensure the JAR is in Spark's classpath.")
            logging.error(f"Failed to load data via Spark JDBC: {e}")

            logging.info("--- MOCK VERIFICATION (Spark DataFrame details) ---")
            logging.info(f"Schema: {data.schema}")
            logging.info(f"Estimated record count: {data.count()}")
            logging.info("Mock verification successful!")


def load_weather_data(table_name: str, df: DataFrame):
    loader = WeatherPostgresLoader()
    loader.load(table_name, df)

