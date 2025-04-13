import os
from typing import Dict
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from data_processor import DataProcessor, MySQLController

load_dotenv(override=True)


def print_header():
    print("*" * 80)
    print("                        HEALTHCARE DATA NORMALIZER SYSTEM")
    print("*" * 80)


def load_config() -> Dict:
    """Load configuration from environment variables"""
    return {
        "client_addr": os.getenv("CLIENT_ADDR"),
        "database_name": os.getenv("DATABASE_NAME"),
        "mysql_driver": os.getenv("MYSQL_DRIVER"),
        "mysql_url": os.getenv("MYSQL_URL"),
        "mysql_user": os.getenv("MYSQL_USER"),
        "mysql_password": os.getenv("MYSQL_PASSWORD"),
        "mysql_db": os.getenv("MYSQL_DB"),
        "output_path": os.getenv("OUTPUT_PATH"),
    }


def create_spark_session(app_name: str = "HealthcareNormalizer") -> SparkSession:
    """
    Create and configure Spark session with MongoDB and MySQL connectors
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.checkpoint.dir", "/tmp/checkpoint/dir")
        .getOrCreate()
    )


if __name__ == "__main__":
    print_header()
    config = load_config()
    csv_path = "./data/legacy_27/legacy_healthcare_data.csv"

    try:

        if not os.path.exists(csv_path):
            print("File does not exist.")
            raise FileNotFoundError(f"File not found: {csv_path}")

        # Initialize DB and connection
        mysql_remote = MySQLController(config)
        mysql_remote.close_root_conn()

        # Initialize processor
        spark = create_spark_session()
        processor = DataProcessor(spark, config)

        # Normalize and create DDL statements
        ddl_statements, table_data = processor.process_data(csv_path)

        # # To save csv before migration
        # for tup in table_data:
        #     temp_df = tup[0]
        #     temp_df_name = tup[1]
        #     processor.save_to_csv(
        #         temp_df,
        #         config["output_path"],
        #         f"{temp_df_name}.csv"
        #     )

        # Migrate to MySQL
        mysql_remote.execute_ddl(ddl_statements)
        processor.migrate_to_mysql(table_data)

        table_names = []
        for table_tuple in table_data:
            table_names.append(table_tuple[1])

        # Query all tables and save to csv
        processor.get_mysql_data(table_names)

    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")

    finally:
        print("\nCleaning up...")
        mysql_remote.close_db_conn()
        spark.stop()
