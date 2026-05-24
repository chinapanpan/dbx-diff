"""
Create 5 Iceberg test tables in workspace.demo2 on EMR side.
Partitions: pt=20260524/25/26.
Differences vs Databricks:
  - v5_t2_part_match: exact match
  - v5_t2_part_mismatch: pt=25 has different values (cost=44 vs 888 in DBX)
  - v5_t2_nopart_str: exact match
  - v5_t2_nopart_num_diff: 4 rows (DBX has 5, extra row with weight=999.9)
  - v5_t2_part_count_only: pt=26 has 3 rows (DBX has 4)
"""

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Setup5TTest_EMR").getOrCreate()

    # --- 1. v5_t2_part_match: EXACTLY same as Databricks ---
    spark.sql("DROP TABLE IF EXISTS workspace.demo2.v5_t2_part_match")
    spark.sql("""CREATE TABLE workspace.demo2.v5_t2_part_match (
        id BIGINT, revenue DOUBLE, qty INT, pt STRING
    ) USING ICEBERG PARTITIONED BY (pt)""")
    spark.sql("""INSERT INTO workspace.demo2.v5_t2_part_match VALUES
        (1, 150.0, 5, '20260524'), (2, 250.0, 8, '20260524'), (3, 350.0, 12, '20260524'),
        (4, 450.0, 15, '20260525'), (5, 550.0, 20, '20260525'),
        (6, 650.0, 25, '20260526'), (7, 750.0, 30, '20260526'), (8, 850.0, 35, '20260526')""")
    print("1. v5_t2_part_match created (exact match)")

    # --- 2. v5_t2_part_mismatch: pt=24,26 same, pt=25 DIFFERENT ---
    # DBX pt=25: (3,33,6), (4,888,77) → sum_cost=921, max_cost=888
    # EMR pt=25: (3,33,6), (4,44,8)   → sum_cost=77,  max_cost=44
    spark.sql("DROP TABLE IF EXISTS workspace.demo2.v5_t2_part_mismatch")
    spark.sql("""CREATE TABLE workspace.demo2.v5_t2_part_mismatch (
        id BIGINT, cost DOUBLE, units INT, pt STRING
    ) USING ICEBERG PARTITIONED BY (pt)""")
    spark.sql("""INSERT INTO workspace.demo2.v5_t2_part_mismatch VALUES
        (1, 11.0, 2, '20260524'), (2, 22.0, 4, '20260524'),
        (3, 33.0, 6, '20260525'), (4, 44.0, 8, '20260525'),
        (5, 55.0, 10, '20260526'), (6, 66.0, 12, '20260526')""")
    print("2. v5_t2_part_mismatch created (pt=25 differs: cost 44 vs 888)")

    # --- 3. v5_t2_nopart_str: EXACTLY same as Databricks ---
    spark.sql("DROP TABLE IF EXISTS workspace.demo2.v5_t2_nopart_str")
    spark.sql("""CREATE TABLE workspace.demo2.v5_t2_nopart_str (
        uid STRING, city STRING, dept STRING
    ) USING ICEBERG""")
    spark.sql("""INSERT INTO workspace.demo2.v5_t2_nopart_str VALUES
        ('u1', 'Beijing', 'Eng'), ('u2', 'Shanghai', 'Sales'),
        ('u3', 'Shenzhen', 'Eng'), ('u4', 'Hangzhou', 'PM'),
        ('u5', 'Guangzhou', 'Sales')""")
    print("3. v5_t2_nopart_str created (exact match, count=5)")

    # --- 4. v5_t2_nopart_num_diff: DBX has 5 rows, EMR has 4 → count/sum differ ---
    spark.sql("DROP TABLE IF EXISTS workspace.demo2.v5_t2_nopart_num_diff")
    spark.sql("""CREATE TABLE workspace.demo2.v5_t2_nopart_num_diff (
        id BIGINT, weight DOUBLE, height INT, label STRING
    ) USING ICEBERG""")
    spark.sql("""INSERT INTO workspace.demo2.v5_t2_nopart_num_diff VALUES
        (1, 60.5, 170, 'A'), (2, 75.0, 180, 'B'), (3, 55.2, 165, 'C'),
        (4, 90.0, 190, 'D')""")
    print("4. v5_t2_nopart_num_diff created (DBX 5 rows, EMR 4)")

    # --- 5. v5_t2_part_count_only: no numeric, pt=24,25 match, pt=26 count diff ---
    # DBX pt=26: 4 rows (c6,c7,c8,c9), EMR pt=26: 3 rows (c6,c7,c8)
    spark.sql("DROP TABLE IF EXISTS workspace.demo2.v5_t2_part_count_only")
    spark.sql("""CREATE TABLE workspace.demo2.v5_t2_part_count_only (
        code STRING, region STRING, flag STRING, pt STRING
    ) USING ICEBERG PARTITIONED BY (pt)""")
    spark.sql("""INSERT INTO workspace.demo2.v5_t2_part_count_only VALUES
        ('c1', 'east', 'Y', '20260524'), ('c2', 'west', 'N', '20260524'),
        ('c3', 'east', 'Y', '20260525'), ('c4', 'north', 'Y', '20260525'), ('c5', 'south', 'N', '20260525'),
        ('c6', 'east', 'Y', '20260526'), ('c7', 'west', 'N', '20260526'), ('c8', 'north', 'Y', '20260526')""")
    print("5. v5_t2_part_count_only created (pt=26: DBX 4, EMR 3)")

    spark.stop()
    print("\nAll 5 EMR Iceberg tables created!")


if __name__ == "__main__":
    main()
