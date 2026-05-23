"""Auto-generated script to create 90 Iceberg test tables in EMR."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Setup90Tables").getOrCreate()

CATALOG = "workspace"
SCHEMA = "demo2"

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SCHEMA}")

def run(sql):
    spark.sql(sql)

print("Creating test_nopart_001...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_001")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_001 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_001 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_002...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_002")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_002 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_002 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_003...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_003")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_003 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_003 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_004...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_004")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_004 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_004 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_005...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_005")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_005 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_005 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_006...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_006")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_006 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_006 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_007...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_007")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_007 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_007 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_008...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_008")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_008 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_008 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_009...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_009")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_009 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_009 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_010...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_010")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_010 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_010 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_011...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_011")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_011 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_011 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_012...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_012")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_012 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_012 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_013...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_013")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_013 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_013 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_014...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_014")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_014 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_014 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_015...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_015")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_015 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_015 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_016...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_016")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_016 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_016 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_017...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_017")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_017 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_017 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_018...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_018")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_018 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_018 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_019...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_019")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_019 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_019 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_020...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_020")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_020 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_020 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_021...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_021")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_021 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_021 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_022...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_022")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_022 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_022 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_023...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_023")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_023 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_023 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_024...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_024")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_024 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_024 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_025...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_025")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_025 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_025 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_026...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_026")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_026 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_026 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_027...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_027")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_027 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_027 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_028...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_028")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_028 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_028 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_029...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_029")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_029 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_029 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_nopart_030...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_nopart_030")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_nopart_030 (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_nopart_030 VALUES (1, 'user_1', 10.5, 60, 'cat_1'), (2, 'user_2', 21.0, 70, 'cat_2'), (3, 'user_3', 31.5, 80, 'cat_0'), (4, 'user_4', 42.0, 90, 'cat_1'), (5, 'user_5', 52.5, 100, 'cat_2')""")

print("Creating test_pt_001...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_001")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_001 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_001 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_002...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_002")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_002 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_002 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_003...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_003")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_003 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_003 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_004...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_004")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_004 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_004 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_005...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_005")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_005 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_005 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_006...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_006")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_006 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_006 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_007...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_007")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_007 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_007 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_008...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_008")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_008 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_008 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_009...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_009")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_009 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_009 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_010...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_010")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_010 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_010 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_011...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_011")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_011 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_011 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_012...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_012")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_012 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_012 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_013...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_013")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_013 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_013 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_014...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_014")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_014 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_014 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_015...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_015")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_015 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_015 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_016...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_016")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_016 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_016 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_017...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_017")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_017 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_017 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_018...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_018")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_018 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_018 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_019...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_019")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_019 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_019 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_020...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_020")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_020 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_020 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_021...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_021")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_021 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_021 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_022...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_022")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_022 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_022 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_023...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_023")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_023 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_023 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_024...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_024")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_024 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_024 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_025...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_025")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_025 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_025 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_026...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_026")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_026 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_026 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_027...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_027")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_027 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_027 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_028...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_028")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_028 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_028 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_029...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_029")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_029 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_029 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_pt_030...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_pt_030")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_pt_030 (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_pt_030 VALUES (1, 'user_1', 100.0, 70, '20250101'), (2, 'user_2', 200.0, 80, '20250101'), (3, 'user_3', 300.0, 90, '20250101'), (4, 'user_1', 100.0, 70, '20250102'), (5, 'user_2', 200.0, 80, '20250102'), (6, 'user_3', 300.0, 90, '20250102')""")

print("Creating test_other_001 (part by date_key)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_001")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_001 (id INT, name STRING, amount DOUBLE, score INT, date_key STRING) USING iceberg PARTITIONED BY (date_key)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_001 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_002 (part by region)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_002")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_002 (id INT, name STRING, amount DOUBLE, score INT, region STRING) USING iceberg PARTITIONED BY (region)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_002 VALUES (1, 'user_1', 50.0, 55, 'us'), (2, 'user_2', 100.0, 70, 'us'), (3, 'user_3', 150.0, 85, 'us'), (4, 'user_1', 50.0, 55, 'eu'), (5, 'user_2', 100.0, 70, 'eu'), (6, 'user_3', 150.0, 85, 'eu')""")

print("Creating test_other_003 (part by dt)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_003")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_003 (id INT, name STRING, amount DOUBLE, score INT, dt STRING) USING iceberg PARTITIONED BY (dt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_003 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_004 (part by date_key)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_004")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_004 (id INT, name STRING, amount DOUBLE, score INT, date_key STRING) USING iceberg PARTITIONED BY (date_key)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_004 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_005 (part by region)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_005")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_005 (id INT, name STRING, amount DOUBLE, score INT, region STRING) USING iceberg PARTITIONED BY (region)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_005 VALUES (1, 'user_1', 50.0, 55, 'us'), (2, 'user_2', 100.0, 70, 'us'), (3, 'user_3', 150.0, 85, 'us'), (4, 'user_1', 50.0, 55, 'eu'), (5, 'user_2', 100.0, 70, 'eu'), (6, 'user_3', 150.0, 85, 'eu')""")

print("Creating test_other_006 (part by dt)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_006")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_006 (id INT, name STRING, amount DOUBLE, score INT, dt STRING) USING iceberg PARTITIONED BY (dt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_006 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_007 (part by date_key)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_007")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_007 (id INT, name STRING, amount DOUBLE, score INT, date_key STRING) USING iceberg PARTITIONED BY (date_key)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_007 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_008 (part by region)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_008")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_008 (id INT, name STRING, amount DOUBLE, score INT, region STRING) USING iceberg PARTITIONED BY (region)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_008 VALUES (1, 'user_1', 50.0, 55, 'us'), (2, 'user_2', 100.0, 70, 'us'), (3, 'user_3', 150.0, 85, 'us'), (4, 'user_1', 50.0, 55, 'eu'), (5, 'user_2', 100.0, 70, 'eu'), (6, 'user_3', 150.0, 85, 'eu')""")

print("Creating test_other_009 (part by dt)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_009")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_009 (id INT, name STRING, amount DOUBLE, score INT, dt STRING) USING iceberg PARTITIONED BY (dt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_009 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_010 (part by date_key)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_010")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_010 (id INT, name STRING, amount DOUBLE, score INT, date_key STRING) USING iceberg PARTITIONED BY (date_key)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_010 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_011 (part by region)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_011")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_011 (id INT, name STRING, amount DOUBLE, score INT, region STRING) USING iceberg PARTITIONED BY (region)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_011 VALUES (1, 'user_1', 50.0, 55, 'us'), (2, 'user_2', 100.0, 70, 'us'), (3, 'user_3', 150.0, 85, 'us'), (4, 'user_1', 50.0, 55, 'eu'), (5, 'user_2', 100.0, 70, 'eu'), (6, 'user_3', 150.0, 85, 'eu')""")

print("Creating test_other_012 (part by dt)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_012")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_012 (id INT, name STRING, amount DOUBLE, score INT, dt STRING) USING iceberg PARTITIONED BY (dt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_012 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_013 (part by date_key)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_013")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_013 (id INT, name STRING, amount DOUBLE, score INT, date_key STRING) USING iceberg PARTITIONED BY (date_key)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_013 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_014 (part by region)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_014")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_014 (id INT, name STRING, amount DOUBLE, score INT, region STRING) USING iceberg PARTITIONED BY (region)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_014 VALUES (1, 'user_1', 50.0, 55, 'us'), (2, 'user_2', 100.0, 70, 'us'), (3, 'user_3', 150.0, 85, 'us'), (4, 'user_1', 50.0, 55, 'eu'), (5, 'user_2', 100.0, 70, 'eu'), (6, 'user_3', 150.0, 85, 'eu')""")

print("Creating test_other_015 (part by dt)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_015")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_015 (id INT, name STRING, amount DOUBLE, score INT, dt STRING) USING iceberg PARTITIONED BY (dt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_015 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_016 (part by date_key)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_016")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_016 (id INT, name STRING, amount DOUBLE, score INT, date_key STRING) USING iceberg PARTITIONED BY (date_key)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_016 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_017 (part by region)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_017")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_017 (id INT, name STRING, amount DOUBLE, score INT, region STRING) USING iceberg PARTITIONED BY (region)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_017 VALUES (1, 'user_1', 50.0, 55, 'us'), (2, 'user_2', 100.0, 70, 'us'), (3, 'user_3', 150.0, 85, 'us'), (4, 'user_1', 50.0, 55, 'eu'), (5, 'user_2', 100.0, 70, 'eu'), (6, 'user_3', 150.0, 85, 'eu')""")

print("Creating test_other_018 (part by dt)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_018")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_018 (id INT, name STRING, amount DOUBLE, score INT, dt STRING) USING iceberg PARTITIONED BY (dt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_018 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_019 (part by date_key)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_019")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_019 (id INT, name STRING, amount DOUBLE, score INT, date_key STRING) USING iceberg PARTITIONED BY (date_key)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_019 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_020 (part by region)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_020")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_020 (id INT, name STRING, amount DOUBLE, score INT, region STRING) USING iceberg PARTITIONED BY (region)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_020 VALUES (1, 'user_1', 50.0, 55, 'us'), (2, 'user_2', 100.0, 70, 'us'), (3, 'user_3', 150.0, 85, 'us'), (4, 'user_1', 50.0, 55, 'eu'), (5, 'user_2', 100.0, 70, 'eu'), (6, 'user_3', 150.0, 85, 'eu')""")

print("Creating test_other_021 (part by dt)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_021")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_021 (id INT, name STRING, amount DOUBLE, score INT, dt STRING) USING iceberg PARTITIONED BY (dt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_021 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_022 (part by date_key)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_022")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_022 (id INT, name STRING, amount DOUBLE, score INT, date_key STRING) USING iceberg PARTITIONED BY (date_key)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_022 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_023 (part by region)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_023")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_023 (id INT, name STRING, amount DOUBLE, score INT, region STRING) USING iceberg PARTITIONED BY (region)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_023 VALUES (1, 'user_1', 50.0, 55, 'us'), (2, 'user_2', 100.0, 70, 'us'), (3, 'user_3', 150.0, 85, 'us'), (4, 'user_1', 50.0, 55, 'eu'), (5, 'user_2', 100.0, 70, 'eu'), (6, 'user_3', 150.0, 85, 'eu')""")

print("Creating test_other_024 (part by dt)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_024")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_024 (id INT, name STRING, amount DOUBLE, score INT, dt STRING) USING iceberg PARTITIONED BY (dt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_024 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_025 (part by date_key)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_025")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_025 (id INT, name STRING, amount DOUBLE, score INT, date_key STRING) USING iceberg PARTITIONED BY (date_key)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_025 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_026 (part by region)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_026")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_026 (id INT, name STRING, amount DOUBLE, score INT, region STRING) USING iceberg PARTITIONED BY (region)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_026 VALUES (1, 'user_1', 50.0, 55, 'us'), (2, 'user_2', 100.0, 70, 'us'), (3, 'user_3', 150.0, 85, 'us'), (4, 'user_1', 50.0, 55, 'eu'), (5, 'user_2', 100.0, 70, 'eu'), (6, 'user_3', 150.0, 85, 'eu')""")

print("Creating test_other_027 (part by dt)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_027")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_027 (id INT, name STRING, amount DOUBLE, score INT, dt STRING) USING iceberg PARTITIONED BY (dt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_027 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_028 (part by date_key)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_028")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_028 (id INT, name STRING, amount DOUBLE, score INT, date_key STRING) USING iceberg PARTITIONED BY (date_key)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_028 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("Creating test_other_029 (part by region)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_029")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_029 (id INT, name STRING, amount DOUBLE, score INT, region STRING) USING iceberg PARTITIONED BY (region)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_029 VALUES (1, 'user_1', 50.0, 55, 'us'), (2, 'user_2', 100.0, 70, 'us'), (3, 'user_3', 150.0, 85, 'us'), (4, 'user_1', 50.0, 55, 'eu'), (5, 'user_2', 100.0, 70, 'eu'), (6, 'user_3', 150.0, 85, 'eu')""")

print("Creating test_other_030 (part by dt)...")
run(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.test_other_030")
run(f"""CREATE TABLE {CATALOG}.{SCHEMA}.test_other_030 (id INT, name STRING, amount DOUBLE, score INT, dt STRING) USING iceberg PARTITIONED BY (dt)""")
run(f"""INSERT INTO {CATALOG}.{SCHEMA}.test_other_030 VALUES (1, 'user_1', 75.0, 67, '20250101'), (2, 'user_2', 150.0, 79, '20250101'), (3, 'user_3', 225.0, 91, '20250101'), (4, 'user_1', 75.0, 67, '20250102'), (5, 'user_2', 150.0, 79, '20250102'), (6, 'user_3', 225.0, 91, '20250102')""")

print("All 90 EMR tables created!")
spark.stop()
