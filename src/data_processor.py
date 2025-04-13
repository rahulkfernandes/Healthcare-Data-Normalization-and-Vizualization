import os
import glob
import shutil
import mysql.connector
import concurrent.futures
from typing import Dict, List, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DateType,
    DecimalType,
    TimestampType,
    LongType,
)
from pyspark.sql.functions import (
    col,
    udf,
    lpad,
    when,
    concat,
    max as spark_max,
    length as spark_length,
    monotonically_increasing_id,
)

PROV_ID_THRES = 3
LOC_ID_THRES = 4


class DataProcessor:
    __config = None
    partition_num = 4

    # Conventional VARCHAR lengths
    conventional_lengths = [50, 100, 255, 500, 1000, 2000, 4000, 8000]

    def __init__(self, spark: SparkSession, config: Dict):
        self.__config = config
        self.spark = spark
        self.original_data = None
        self.status_cutoff = "2021-12-31"

        # PySpark to MySQL data type mapping
        self.pyspark_to_mysql = {
            StringType: "VARCHAR(255)",
            IntegerType: "INT",
            LongType: "BIGINT",
            DateType: "DATE",
            DecimalType: "DECIMAL(10,2)",
            TimestampType: "TIMESTAMP(6)",
        }

    def set_partitions(self, partition_num: int):
        """Set number of partions for pyspark partioning"""
        self.partition_num = partition_num

    def set_status_cutoff(self, status_cutoff: str):
        """Set a different cut off date to determine patient status"""
        self.status_cutoff = status_cutoff

    def set_conventional_lengths(self, conventional_lengths: List[int]):
        """Set the conventional lengths for VARCHAR in the DDL statements"""
        self.conventional_lengths = conventional_lengths

    def _load_csv(self, path: str):
        """
        Load data from a CSV file.

        :param path: Path to the CSV file
        :return: DataFrame containing the loaded CSV data
        """
        return self.spark.read.csv(path, header=True, inferSchema=True)

    def _get_conventional_length(self, max_length: int) -> int:
        """
        Return the closest conventional VARCHAR length greater than or equal to max_length.

        Args:
            max_length: Maximum length across all string columns.

        Returns:
            int: Conventional VARCHAR length (e.g., 50, 100, 255, 500, 1000, etc.).
        """
        if max_length is None:
            return 50

        for length in self.conventional_lengths:
            if max_length <= length:
                return length

        return ((max_length // 1000) + 1) * 1000

    def _get_global_len(self, df: DataFrame) -> int:
        """
        Calculate the maximum length across all string columns in a PySpark DataFrame
        and return the closest conventional VARCHAR length greater than or equal to it.

        Args:
            df: DataFrame.

        Returns:
            int: Single conventional VARCHAR length for all string columns.
        """
        # Identify string columns from the schema
        string_columns = [
            field.name
            for field in df.schema.fields
            if field.dataType.typeName() == "string"
        ]

        if not string_columns:
            print("No string columns found.")
            return 50

        # Calculate max length for each string column
        max_length_expr = [
            spark_max(spark_length(col(column))).alias(column)
            for column in string_columns
        ]
        max_lengths_df = df.select(max_length_expr)

        # Get the global maximum across all columns
        max_length_row = max_lengths_df.collect()[0]
        max_length = max(
            [max_length_row[col] or 0 for col in string_columns]
        )  # Handle nulls with 0

        # Get the conventional VARCHAR length
        varchar_length = self._get_conventional_length(max_length)

        # Print for verification
        print(f"Global maximum length across all string columns: {max_length}")
        print(
            f"Suggested VARCHAR length for all string columns: VARCHAR({varchar_length})"
        )

        return varchar_length

    def _generate_ddl(
        self, table_name: str, table_info: List[Tuple[DataFrame, str]]
    ) -> str:
        # Get list of columns that should be CHAR(1), default to empty list if not provided
        char_columns = table_info.get("char_columns", [])

        # Generate column definitions with CHAR(1) logic for specified columns
        columns = []
        for field in table_info["schema"].fields:
            field_type = type(field.dataType)
            if field_type == StringType and field.name in char_columns:
                columns.append(
                    f"{field.name} CHAR(1)"
                )  # Use CHAR(1) for length-1 columns
            else:
                columns.append(
                    f"{field.name} {self.pyspark_to_mysql.get(field_type, 'VARCHAR(255)')}"
                )

        # Define primary key constraint
        primary_key_constraint = f"PRIMARY KEY ({table_info['primary_key']})"
        # Define foreign key constraints if they exist
        foreign_key_constraints = [
            f"FOREIGN KEY ({fk['column']}) REFERENCES {fk['references']}"
            for fk in table_info.get("foreign_keys", [])
        ]
        constraints = [primary_key_constraint] + foreign_key_constraints

        # Build the DDL statement
        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        ddl += ",\n".join(columns)
        if constraints:
            ddl += ",\n" + ",\n".join(constraints)
        ddl += "\n);"
        return ddl

    def _find_char_cols(self, df: DataFrame) -> List[str]:
        # Get string columns
        string_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StringType)
        ]
        # Compute max length for each string column
        length_df = df.select(
            [spark_max(spark_length(col)).alias(col) for col in string_cols]
        )
        max_lengths = length_df.collect()[0].asDict()

        # Identify columns with max length of 1
        char_columns = [col for col, max_len in max_lengths.items() if max_len == 1]
        return char_columns

    def _rm_empty_rows(self, df: DataFrame) -> DataFrame:
        # Remove rows where ALL columns are null
        condition = None
        for column in df.columns:
            if condition is None:
                condition = col(column).isNull()
            else:
                condition = condition & col(column).isNull()
        return df.filter(~condition)

    def get_varchar_length(self, df: DataFrame, column_name: str) -> int:
        """
        Calculate the maximum length of a string column in a PySpark DataFrame
        and return the closest conventional VARCHAR length greater than or equal to it.

        Args:
            df: PySpark DataFrame containing the column.
            column_name: Name of the string column to check.

        Returns:
            int: Conventional VARCHAR length (e.g., 50, 100, 255, 500, 1000, etc.).
        """
        # Calculate the maximum length of the column
        max_length = df.select(spark_max(spark_length(col(column_name)))).collect()[0][
            0
        ]

        # Handle null case (if column is all null)
        if max_length is None:
            return 50  # Default to smallest conventional length

        # Find the smallest conventional length >= max_length
        for length in self.conventional_lengths:
            if max_length <= length:
                return length

        # If max_length exceeds all conventional lengths, return max_length rounded up
        # to nearest 1000 (or use TEXT in practice)
        return ((max_length // 1000) + 1) * 1000

    def _normalize(self):
        table_definitions = []
        table_data = []

        global_varchar = self._get_global_len(self.original_data)
        self.pyspark_to_mysql.update({StringType: f"VARCHAR({global_varchar})"})

        # Patient table
        patient_df_base = self.original_data.select(
            "patient_id",
            "patient_first_name",
            "patient_last_name",
            col("patient_date_of_birth").cast("date").alias("patient_date_of_birth"),
            "patient_gender",
            "patient_address_line1",
            "patient_address_line2",
            "patient_city",
            "patient_state",
            col("patient_zip").cast("string").alias("patient_zip"),
            "patient_phone",
            "patient_email",
        ).dropDuplicates(["patient_id"])

        last_visit_df = self.original_data.groupBy("patient_id").agg(
            spark_max("visit_datetime").alias("last_visit_datetime")
        )
        last_visit_df = last_visit_df.withColumn(
            "patient_status",
            when(
                col("last_visit_datetime").cast("date") <= self.status_cutoff,
                "inactive",
            ).otherwise("active"),
        )

        patient_df = patient_df_base.join(
            last_visit_df.select("patient_id", "patient_status"),
            "patient_id",
            "left",  # Left join to keep patients with no visits
        ).fillna(
            {"patient_status": "inactive"}
        )  # Default to inactive if no visits

        table_definitions.append(
            {
                "name": "DimPatient",
                "schema": patient_df.schema,
                "primary_key": "patient_id",
                "foreign_keys": [],
                "char_columns": self._find_char_cols(patient_df),
            }
        )
        table_data.append((patient_df, "DimPatient"))

        # Insurance table
        insurance_df = self.original_data.select(
            "insurance_id",
            "patient_id",
            "insurance_payer_name",
            "insurance_policy_number",
            "insurance_group_number",
            "insurance_plan_type",
        ).dropDuplicates(["insurance_id"])
        table_definitions.append(
            {
                "name": "DimInsurance",
                "schema": insurance_df.schema,
                "primary_key": "insurance_id",
                "foreign_keys": [
                    {"column": "patient_id", "references": "DimPatient(patient_id)"}
                ],
            }
        )
        table_data.append((insurance_df, "DimInsurance"))

        # Billing table
        billing_df = self.original_data.select(
            "billing_id",
            "insurance_id",
            col("billing_total_charge")
            .cast("decimal(10,2)")
            .alias("billing_total_charge"),
            col("billing_amount_paid")
            .cast("decimal(10,2)")
            .alias("billing_amount_paid"),
            col("billing_date").cast("date").alias("billing_date"),
            "billing_payment_status",
        ).dropDuplicates(["billing_id"])
        table_definitions.append(
            {
                "name": "DimBilling",
                "schema": billing_df.schema,
                "primary_key": "billing_id",
                "foreign_keys": [
                    {
                        "column": "insurance_id",
                        "references": "DimInsurance(insurance_id)",
                    }
                ],
            }
        )
        table_data.append((billing_df, "DimBilling"))

        def _provider_id_helper(title: str, department: str):
            dept_code = "".join(word[0] for word in department.split()).upper()

            if len(dept_code) > PROV_ID_THRES:
                dept_code = dept_code[PROV_ID_THRES - 1]

            return dept_code + title

        provider_id_udf = udf(_provider_id_helper, StringType())

        # Provider table (surrogate key needed)
        provider_df = self.original_data.select(
            "doctor_name", "doctor_title", "doctor_department"
        ).distinct()

        provider_df = (
            provider_df.withColumn(
                "prefix", provider_id_udf(col("doctor_title"), col("doctor_department"))
            )
            .withColumn(
                "provider_id",
                concat(
                    col("prefix"),
                    lpad(
                        monotonically_increasing_id().cast("string"), 6, "0"
                    ),  # Pad to 6 digits
                ),
            )
            .select("provider_id", "doctor_name", "doctor_title", "doctor_department")
        )

        table_definitions.append(
            {
                "name": "DimProvider",
                "schema": provider_df.schema,
                "primary_key": "provider_id",
                "foreign_keys": [],
            }
        )
        table_data.append((provider_df, "DimProvider"))

        def _location_id_helper(location: str, room_number: int) -> str:
            loc_code = "".join(word[0] for word in location.split()).upper()

            if len(loc_code) > LOC_ID_THRES:
                loc_code = loc_code[LOC_ID_THRES - 1]

            return loc_code + str(room_number)

        loc_id_udf = udf(_location_id_helper, StringType())

        # Location table (surrogate key needed)
        location_df = self.original_data.select("clinic_name", "room_number").distinct()
        location_df = location_df.withColumn(
            "location_id", loc_id_udf(col("clinic_name"), col("room_number"))
        ).select("location_id", "clinic_name", "room_number")

        table_definitions.append(
            {
                "name": "DimLocation",
                "schema": location_df.schema,
                "primary_key": "location_id",
                "foreign_keys": [],
            }
        )
        table_data.append((location_df, "DimLocation"))

        def _diagnosis_id_helper(desc: str) -> str:
            if desc is None or len(desc) == 0:
                return 0
            return len(desc)

        diag_id_udf = udf(_diagnosis_id_helper, IntegerType())

        # PrimaryDiagnosis table (surrogate key needed)
        primary_diagnosis_df = (
            self.original_data.select(
                "primary_diagnosis_code", "primary_diagnosis_desc"
            )
            .filter(~(col("primary_diagnosis_code").isNull()))
            .distinct()
        )
        primary_diagnosis_df = primary_diagnosis_df.withColumn(
            "primary_diagnosis_id",
            concat(
                col("primary_diagnosis_code"),
                diag_id_udf(col("primary_diagnosis_desc")).cast("string"),
                monotonically_increasing_id().cast("string"),
            ),
        ).select(
            "primary_diagnosis_id", "primary_diagnosis_code", "primary_diagnosis_desc"
        )

        table_definitions.append(
            {
                "name": "DimPrimaryDiagnosis",
                "schema": primary_diagnosis_df.schema,
                "primary_key": "primary_diagnosis_id",
                "foreign_keys": [],
            }
        )
        table_data.append((primary_diagnosis_df, "DimPrimaryDiagnosis"))

        # SecondaryDiagnosis table (surrogate key needed)
        secondary_diagnosis_df = (
            self.original_data.select(
                "secondary_diagnosis_code", "secondary_diagnosis_desc"
            )
            .filter(~(col("secondary_diagnosis_code").isNull()))
            .distinct()
        )
        secondary_diagnosis_df = secondary_diagnosis_df.withColumn(
            "secondary_diagnosis_id",
            concat(
                col("secondary_diagnosis_code"),
                diag_id_udf(col("secondary_diagnosis_desc")).cast("string"),
                monotonically_increasing_id().cast("string"),
            ),
        ).select(
            "secondary_diagnosis_id",
            "secondary_diagnosis_code",
            "secondary_diagnosis_desc",
        )

        table_definitions.append(
            {
                "name": "DimSecondaryDiagnosis",
                "schema": secondary_diagnosis_df.schema,
                "primary_key": "secondary_diagnosis_id",
                "foreign_keys": [],
            }
        )
        table_data.append((secondary_diagnosis_df, "DimSecondaryDiagnosis"))

        # Treatment table (surrogate key needed)
        treatment_df = self.original_data.select(
            "treatment_code", "treatment_desc"
        ).distinct()
        treatment_df = treatment_df.withColumn(
            "treatment_id", concat(col("treatment_code"), monotonically_increasing_id())
        ).select("treatment_id", "treatment_code", "treatment_desc")

        table_definitions.append(
            {
                "name": "DimTreatment",
                "schema": treatment_df.schema,
                "primary_key": "treatment_id",
                "foreign_keys": [],
            }
        )
        table_data.append((treatment_df, "DimTreatment"))

        # Prescription table
        prescription_df = self.original_data.select(
            "prescription_id",
            "prescription_drug_name",
            "prescription_dosage",
            "prescription_frequency",
            col("prescription_duration_days")
            .cast("int")
            .alias("prescription_duration_days"),
        ).dropDuplicates(["prescription_id"])
        prescription_df = self._rm_empty_rows(prescription_df)

        table_definitions.append(
            {
                "name": "DimPrescription",
                "schema": prescription_df.schema,
                "primary_key": "prescription_id",
                "foreign_keys": [],
            }
        )
        table_data.append((prescription_df, "DimPrescription"))

        # LabOrder table
        lab_order_df = self.original_data.select(
            "lab_order_id",
            "lab_test_code",
            "lab_name",
            "lab_result_value",
            "lab_result_units",
            "lab_result_date",
        ).dropDuplicates(["lab_order_id"])
        lab_order_df = self._rm_empty_rows(lab_order_df)

        table_definitions.append(
            {
                "name": "DimLabOrder",
                "schema": lab_order_df.schema,
                "primary_key": "lab_order_id",
                "foreign_keys": [],
            }
        )
        table_data.append((lab_order_df, "DimLabOrder"))
        # # Filter rows where ALL columns are null
        # condition = None
        # for column in lab_order_df.columns:
        #     if condition is None:
        #         condition = col(column).isNull()
        #     else:
        #         condition = condition & col(column).isNull()
        # filtered_df = lab_order_df.filter(condition)
        # filtered_df.show()

        # Visit table (join with other tables to get foreign keys)
        visit_df = self.original_data.select(
            "visit_id",
            "visit_datetime",
            "visit_type",
            "patient_id",
            "insurance_id",
            "billing_id",
            "primary_diagnosis_code",
            "primary_diagnosis_desc",
            "secondary_diagnosis_code",
            "secondary_diagnosis_desc",
            "treatment_code",
            "treatment_desc",
            "prescription_id",
            "lab_order_id",
            "doctor_name",
            "doctor_title",
            "doctor_department",
            "clinic_name",
            "room_number",
        )

        # Join to get foreign keys
        visit_df = (
            visit_df.join(
                provider_df,
                ["doctor_name", "doctor_title", "doctor_department"],
                "left",
            )
            .join(location_df, ["clinic_name", "room_number"], "left")
            .join(
                primary_diagnosis_df,
                ["primary_diagnosis_code", "primary_diagnosis_desc"],
                "left",
            )
            .join(
                secondary_diagnosis_df,
                ["secondary_diagnosis_code", "secondary_diagnosis_desc"],
                "left",
            )
            .join(treatment_df, ["treatment_code", "treatment_desc"], "left")
            .select(
                "visit_id",
                "patient_id",
                "insurance_id",
                "billing_id",
                "provider_id",
                "location_id",
                "primary_diagnosis_id",
                "secondary_diagnosis_id",
                "treatment_id",
                "prescription_id",
                "lab_order_id",
                "visit_datetime",
                "visit_type",
            )
        )

        # Defining table info for each table
        table_definitions.append(
            {
                "name": "FacVisit",
                "schema": visit_df.schema,
                "primary_key": "visit_id",
                "foreign_keys": [
                    {"column": "patient_id", "references": "DimPatient(patient_id)"},
                    {
                        "column": "insurance_id",
                        "references": "DimInsurance(insurance_id)",
                    },
                    {"column": "billing_id", "references": "DimBilling(billing_id)"},
                    {"column": "provider_id", "references": "DimProvider(provider_id)"},
                    {"column": "location_id", "references": "DimLocation(location_id)"},
                    {
                        "column": "primary_diagnosis_id",
                        "references": "DimPrimaryDiagnosis(primary_diagnosis_id)",
                    },
                    {
                        "column": "secondary_diagnosis_id",
                        "references": "DimSecondaryDiagnosis(secondary_diagnosis_id)",
                    },
                    {
                        "column": "treatment_id",
                        "references": "DimTreatment(treatment_id)",
                    },
                    {
                        "column": "prescription_id",
                        "references": "DimPrescription(prescription_id)",
                    },
                    {
                        "column": "lab_order_id",
                        "references": "DimLabOrder(lab_order_id)",
                    },
                ],
            }
        )
        table_data.append((visit_df, "FacVisit"))

        # Generate DDL statements
        ddl_statements = [
            self._generate_ddl(table["name"], table) for table in table_definitions
        ]

        return ddl_statements, table_data

    def execute_mysql_ddl(self, ddl_statements: List[str]):
        ######## NEEDS DEBUGGING (DEPRECATED TILL THEN) ########
        try:
            self.spark._jvm.java.lang.Class.forName(self.__config["mysql_driver"])
            print("MySQL JDBC driver registered successfully.")
        except Exception as e:
            print(f"Error registering driver: {e}")
            print(
                "Ensure the MySQL JDBC driver JAR is correctly specified in spark.jars"
            )
            return

        if isinstance(ddl_statements, str):
            ddl_statements = [ddl_statements]

        conn = self.spark._jvm.java.sql.DriverManager.getConnection(
            self.__config["mysql_url"],
            self.__config["mysql_user"],
            self.__config["mysql_password"],
        )
        for ddl in ddl_statements:
            print("Executing:\n")
            print(ddl)
            try:
                stmt = conn.createStatement()
                stmt.executeUpdate(ddl)
                print(f"Executed DDL: {ddl.splitlines()[0]}")
                stmt.close()
                conn.close()
            except Exception as e:
                print(f"Error executing DDL: {e}")

    def load_mysql_data(
        self, jdbc_url: str, db_table: str, db_user: str, db_password: str
    ) -> DataFrame:
        """
        Load data from MySQL database.

        :param jdbc_url: JDBC URL for the MySQL database
        :param db_table: Name of the table to load data from
        :param db_user: Database username
        :param db_password: Database password
        :return: DataFrame containing the loaded MySQL data
        """
        return (
            self.spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", db_table)
            .option("user", db_user)
            .option("password", db_password)
            .load()
        )

    def migrate_to_mysql(self, table_data: List[Tuple[DataFrame, str]]):
        """
        Migrate input data to MySQL

        :param table_data: list of tuples containing dataframe and table name
        """
        for df, table_name in table_data:
            df.write.format("jdbc").option("url", self.__config["mysql_url"]).option(
                "dbtable", table_name
            ).option("user", self.__config["mysql_user"]).option(
                "password", self.__config["mysql_password"]
            ).option(
                "driver", self.__config["mysql_driver"]
            ).option(
                "numPartitions", self.partition_num
            ).mode(
                "append"
            ).save()

    def process_data(self, path: str) -> Tuple[List, List[Tuple[DataFrame, str]]]:
        if self.__config == None:
            print("Config not loaded.")
            exit(1)

        self.original_data = self._load_csv(path)
        ddl_statements, table_data = self._normalize()

        return ddl_statements, table_data

    def get_mysql_data(self, table_names: str):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit a load task for each table
            futures = [
                (
                    executor.submit(
                        self.load_mysql_data,
                        self.__config["mysql_url"],
                        table,
                        self.__config["mysql_user"],
                        self.__config["mysql_password"],
                    ),
                    table,
                )
                for table in table_names
            ]

            for future in futures:
                temp_df = future[0].result()
                temp_df_name = future[1]
                print(f"{temp_df_name}:")
                temp_df.show()
                self.save_to_csv(
                    temp_df, self.__config["output_path"], f"{temp_df_name}.csv"
                )
            executor.shutdown()

    def save_to_csv(self, df: DataFrame, output_path: str, filename: str) -> None:
        """
        Save DataFrame to a single CSV file.

        :param df: DataFrame to save
        :param output_path: Base directory path
        :param filename: Name of the CSV file
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Create full path for the output file
        full_path = os.path.join(output_path, filename)
        print(f"Saving to: {full_path}")  # Debugging output

        # Create a temporary directory in the correct output path
        temp_dir = os.path.join(output_path, "_temp")
        print(f"Temporary directory: {temp_dir}")  # Debugging output

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        try:
            shutil.rmtree(temp_dir)
        except FileNotFoundError:
            print(f"Folder '{temp_dir}' does not exist.")


class MySQLController:
    """Class to connect to mysql server for db initialization
    and to execute DDL statements"""

    def __init__(self, config: str):
        self.__config = config
        self.db_name = self.__config["database_name"]

        self.root_remote = self.connect_to_root_remote()
        self.root_cursor = self.root_remote.cursor()

        self.initialize_db()

        self.db_remote = self.connect_to_db_remote()
        self.db_cursor = self.db_remote.cursor()

    def connect_to_root_remote(self) -> mysql.connector.connection:
        try:
            root_controller = mysql.connector.connect(
                host=self.__config["client_addr"],
                user=self.__config["mysql_user"],
                password=self.__config["mysql_password"],
            )
            if root_controller is None:
                print("Could not connect to MySQL server.")
            return root_controller
        except:
            print("Could not connect to MySQL server.")
            return None

    def connect_to_db_remote(self) -> mysql.connector.connection:
        if self.db_name is not None:
            db_remote = mysql.connector.connect(
                host=self.__config["client_addr"],
                user=self.__config["mysql_user"],
                password=self.__config["mysql_password"],
                database=self.db_name,
            )
            return db_remote
        else:
            print("Could not connect to MySQL Database.")
            return None

    def initialize_db(self):
        """DELETE database if exists and the creates a new database"""
        if self.root_remote and self.root_cursor:
            try:
                # Drop the database if it exists
                self.root_cursor.execute(f"DROP DATABASE IF EXISTS `{self.db_name}`")
                # Create the database
                self.root_cursor.execute(f"CREATE DATABASE `{self.db_name}`")
                self.root_remote.commit()
                print(f"Database '{self.db_name}' initialized successfully.")
            except mysql.connector.Error as err:
                print(f"Error creating/replacing database: {err}")
        else:
            print("Database connection not established.")

    def execute_ddl(self, ddl_statements: List[str]):
        """Execute a list of DDL statements"""
        try:
            if self.db_remote.is_connected():
                if isinstance(ddl_statements, str):
                    ddl_statements = [ddl_statements]
                for ddl in ddl_statements:
                    print("\nExecuting:")
                    print(ddl)
                    self.db_cursor.execute(ddl)
                self.db_remote.commit()
        except Exception as e:
            print(f"Error while executing ddl statements: {e}")

    def close_root_conn(self):
        """Close root connection"""
        if self.root_remote:
            if self.root_cursor:
                self.root_cursor.close()
            self.root_remote.close()

    def close_db_conn(self):
        """Close database connection"""
        if self.db_remote:
            if self.db_cursor:
                self.db_cursor.close()
            self.db_remote.close()
