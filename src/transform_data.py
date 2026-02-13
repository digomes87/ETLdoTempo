from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pathlib import Path
import logging
from typing import Optional, Dict, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

from src.interfaces import DataTransformer

class WeatherTransformer(DataTransformer):
    """
    Handle transformations using pyspark
    Implement the interface Data Transformer
    """
    DEFAULT_COLUMNS_TO_DROP = ['weather', 'weather_icon', 'sys.type', 'sys.id']
    DEFAULT_COLUMNS_TO_RENAME = {
        "base": "base",
        "visibility": "visibility",
        "dt": "datetime",
        "timezone": "timezone",
        "id": "city_id",
        "name": "city_name",
        "cod": "code",
        "coord.lon": "longitude",
        "coord.lat": "latitude",
        "main.temp": "temperature",
        "main.feels_like": "feels_like",
        "main.temp_min": "temp_min",
        "main.temp_max": "temp_max",
        "main.pressure": "pressure",
        "main.humidity": "humidity",
        "main.sea_level": "sea_level",
        "main.grnd_level": "grnd_level",
        "wind.speed": "wind_speed",
        "wind.deg": "wind_deg",
        "wind.gust": "wind_gust",
        "clouds.all": "clouds",
        "sys.type": "sys_type",
        "sys.country": "country",
        "sys.sunrise": "sunrise",
        "sys.sunset": "sunset",
    }

    DEFAULT_DATETIME_COLUMNS = ['datetime', 'sunrise', 'sunset']

    def __init__(self,
                 columns_to_drop: Optional[List[str]] = None,
                 columns_to_rename: Optional[Dict[str, str]] = None,
                 datetime_columns: Optional[List[str]] = None):
        self.columns_to_drop = columns_to_drop or self.DEFAULT_COLUMNS_TO_DROP
        self.columns_to_rename = columns_to_rename or self.DEFAULT_COLUMNS_TO_RENAME
        self.datetime_columns = datetime_columns or self.DEFAULT_DATETIME_COLUMNS

    def transform(self,df: DataFrame) -> DataFrame:
        """Execute the full transformation logic provide DataFrame"""
        logging.info("Starting transformation with PySpark ")

        try:
            df = self._normalize_weather_columns(df)
            df = self._flatten_dataframe(df)

            # After flattening, we need to use the flattened column names for renaming and dropping
            available_cols_to_drop = [c for c in self.columns_to_drop if c in df.columns]
            df = df.drop(*available_cols_to_drop)

            df = self._rename_columns(df, self.columns_to_rename)

            df = self._normalize_datetime_columns(df, self.datetime_columns)

            logging.info("Transformation complete !!")
            return df

        except Exception as e:
            logging.error(f"Error occurred while transforming data: {e}")
            raise

    @staticmethod
    def _flatten_dataframe(df: DataFrame) -> DataFrame:
        """Flatten nested structures in the dataframe"""
        def get_flattened_columns(schema, prefix=""):
            cols = []
            for field in schema.fields:
                name = f"{prefix}.{field.name}" if prefix else field.name
                if isinstance(field.dataType, F.StructType):
                    cols.extend(get_flattened_columns(field.dataType, name))
                else:
                    cols.append(F.col(name).alias(name))
            return cols

        return df.select(get_flattened_columns(df.schema))

    @staticmethod
    def _normalize_weather_columns(df: DataFrame) -> DataFrame:
        """Normalize array columns"""
        logging.info("Normalizing weather columns")

        df = df.withColumn("weather_struct", F.col("weather").alias("weather").getItem(0))

        df = df.withColumn("weather_id", F.col("weather_struct.id")) \
            .withColumn("weather_main", F.col("weather_struct.main")) \
            .withColumn("weather_description", F.col("weather_struct.description")) \
            .withColumn("weather_icon", F.col("weather_struct.icon"))

        return df.drop("weather_struct")

    @staticmethod
    def _rename_columns(df: DataFrame, rename_map: Dict[str, str]) -> DataFrame:
        """Rename columns based ont he provided mapping"""
        logging.info(f"Renaming columns {len(rename_map)} columns")

        for old_name, new_name in rename_map.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

        return df

    @staticmethod
    def _normalize_datetime_columns(df: DataFrame, columns: List[str]) -> DataFrame:
        """Convert Unix timestamps to datetime."""
        logging.info(f"Converting columns to datetime: {columns}")
        for col_name in columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.timestamp_seconds(F.col(col_name)))
        return df

def data_transformations(spark: Optional[SparkSession] = None,
                         input_path: Optional[Path] = None) -> DataFrame:
    """
    Orchestrator function (Facade) that provides a simple entry point.
    Now supports dependency injection.
    """
    if spark is None:
        spark = SparkSession.builder \
            .appName("WeatherDataTransformation") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
            .getOrCreate()

    if input_path is None:
        input_path = Path(__file__).parent.parent / 'data' / 'weather_data.json'

    logging.info(f"Reading data from: {input_path}")
    if not input_path.exists():
        raise FileNotFoundError(f"File not found: {input_path}")

    df = spark.read.option("multiLine", "true").json(str(input_path))

    transformer = WeatherTransformer()
    return transformer.transform(df)


if __name__ == "__main__":
    transformed_df = data_transformations()
    transformed_df.show()


