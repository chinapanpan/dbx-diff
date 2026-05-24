"""
Setup test data for v5 scheduler testing.

Creates tables on EMR side (Iceberg via Glue):
1. workspace.demo2.v5_test_partitioned - partitioned table with numeric columns
2. workspace.demo2.v5_test_nopart - non-partitioned table with numeric columns
3. workspace.demo2.v5_test_no_numeric - partitioned table without numeric columns (count-only)
4. workspace.demo2.verify_result - Iceberg result table

Also creates same tables on Databricks side via SQL API (with intentional differences for testing).
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F


def get_spark() -> SparkSession:
    return SparkSession.builder \
        .appName("SetupV5Test") \
        .getOrCreate()


def setup_emr_tables(spark: SparkSession):
    """Create test tables on EMR (Iceberg via Glue catalog)."""

    spark.sql("CREATE DATABASE IF NOT EXISTS workspace.demo2")

    # 1. verify_result table (DDL as specified)
    spark.sql("DROP TABLE IF EXISTS workspace.demo2.verify_result")
    spark.sql("""
        CREATE TABLE workspace.demo2.verify_result (
            task_id STRING,
            instance_id STRING,
            attemp_id STRING,
            table_name STRING,
            pt BIGINT,
            result STRING COMMENT 'Y means pass, N means fail',
            details ARRAY<STRUCT<
                field_name: STRING,
                field_type: STRING,
                task_type: STRING COMMENT 'count, max, min, sum',
                delta_value: STRING,
                iceberg_value: STRING,
                result: STRING COMMENT 'Y means pass, N means fail'
            >> COMMENT 'field-level check details'
        )
        USING ICEBERG
        COMMENT 'databricks vs emr data comparison partition result table'
    """)
    print("Created workspace.demo2.verify_result")

    # 2. Partitioned table with numeric columns
    spark.sql("DROP TABLE IF EXISTS workspace.demo2.v5_test_partitioned")
    spark.sql("""
        CREATE TABLE workspace.demo2.v5_test_partitioned (
            id BIGINT,
            name STRING,
            amount DOUBLE,
            quantity INT,
            pt STRING
        )
        USING ICEBERG
        PARTITIONED BY (pt)
    """)
    # Insert data - pt=20260521 will MATCH, pt=20260522 will DIFFER
    spark.sql("""
        INSERT INTO workspace.demo2.v5_test_partitioned VALUES
        (1, 'alice', 100.0, 10, '20260521'),
        (2, 'bob', 200.0, 20, '20260521'),
        (3, 'carol', 300.0, 30, '20260521'),
        (4, 'dave', 150.0, 15, '20260522'),
        (5, 'eve', 250.0, 25, '20260522'),
        (6, 'frank', 350.0, 35, '20260522')
    """)
    print("Created workspace.demo2.v5_test_partitioned (Iceberg)")

    # 3. Non-partitioned table with numeric columns
    spark.sql("DROP TABLE IF EXISTS workspace.demo2.v5_test_nopart")
    spark.sql("""
        CREATE TABLE workspace.demo2.v5_test_nopart (
            id BIGINT,
            value DOUBLE,
            score INT,
            label STRING
        )
        USING ICEBERG
    """)
    spark.sql("""
        INSERT INTO workspace.demo2.v5_test_nopart VALUES
        (1, 10.5, 80, 'good'),
        (2, 20.3, 90, 'excellent'),
        (3, 30.7, 70, 'average'),
        (4, 40.1, 85, 'good'),
        (5, 50.9, 95, 'excellent')
    """)
    print("Created workspace.demo2.v5_test_nopart (Iceberg)")

    # 4. Partitioned table without numeric columns (count-only test)
    spark.sql("DROP TABLE IF EXISTS workspace.demo2.v5_test_no_numeric")
    spark.sql("""
        CREATE TABLE workspace.demo2.v5_test_no_numeric (
            id STRING,
            name STRING,
            status STRING,
            pt STRING
        )
        USING ICEBERG
        PARTITIONED BY (pt)
    """)
    spark.sql("""
        INSERT INTO workspace.demo2.v5_test_no_numeric VALUES
        ('a1', 'alice', 'active', '20260521'),
        ('a2', 'bob', 'active', '20260521'),
        ('a3', 'carol', 'inactive', '20260521'),
        ('a4', 'dave', 'active', '20260522'),
        ('a5', 'eve', 'inactive', '20260522')
    """)
    print("Created workspace.demo2.v5_test_no_numeric (Iceberg)")

    print("\nAll EMR Iceberg tables created successfully.")
    print("verify_result count:", spark.sql("SELECT count(*) FROM workspace.demo2.verify_result").collect()[0][0])


def main():
    spark = get_spark()
    setup_emr_tables(spark)
    spark.stop()


if __name__ == "__main__":
    main()
