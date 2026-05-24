"""
Create EMR-side test tables for count-only verification.

Tables:
1. test_no_numeric_multi_pt - pt=20250102 has 3 rows (Databricks has 4) - DIFF
2. test_no_numeric_pt - pt=20250102 has 2 rows (Databricks has 3) - DIFF
3. test_count_only_nopart - non-partitioned, 5 rows (matches Databricks)
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SetupCountOnlyTest").getOrCreate()

CATALOG = "workspace"
SCHEMA = "demo2"

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Table 1: test_no_numeric_multi_pt (already exists from previous setup)
# pt=20250101: 3 rows (matches), pt=20250102: 3 rows (Databricks has 4), pt=20250103: 2 rows (matches)
full = f"{CATALOG}.{SCHEMA}.test_no_numeric_multi_pt"
spark.sql(f"DROP TABLE IF EXISTS {full}")
spark.sql(f"""
    CREATE TABLE {full} (
        name STRING, city STRING, status STRING, pt STRING
    ) USING iceberg PARTITIONED BY (pt)
""")
spark.sql(f"""
    INSERT INTO {full} VALUES
    ('alice','beijing','active','20250101'),
    ('bob','shanghai','inactive','20250101'),
    ('charlie','guangzhou','active','20250101'),
    ('david','shenzhen','active','20250102'),
    ('eve','hangzhou','inactive','20250102'),
    ('frank','nanjing','active','20250102'),
    ('henry','wuhan','inactive','20250103'),
    ('iris','xian','active','20250103')
""")
print(f"Created {full}")
print("  pt=20250101: 3 rows (matches Databricks)")
print("  pt=20250102: 3 rows (Databricks has 4 - DIFF)")
print("  pt=20250103: 2 rows (matches Databricks)")

# Table 2: test_no_numeric_pt
full = f"{CATALOG}.{SCHEMA}.test_no_numeric_pt"
spark.sql(f"DROP TABLE IF EXISTS {full}")
spark.sql(f"""
    CREATE TABLE {full} (
        name STRING, city STRING, status STRING, pt STRING
    ) USING iceberg PARTITIONED BY (pt)
""")
spark.sql(f"""
    INSERT INTO {full} VALUES
    ('alice','beijing','active','20250101'),
    ('bob','shanghai','inactive','20250101'),
    ('charlie','guangzhou','active','20250101'),
    ('david','shenzhen','active','20250102'),
    ('eve','hangzhou','inactive','20250102')
""")
print(f"Created {full}")
print("  pt=20250101: 3 rows (matches Databricks)")
print("  pt=20250102: 2 rows (Databricks has 3 - DIFF)")

# Table 3: test_count_only_nopart (non-partitioned)
full = f"{CATALOG}.{SCHEMA}.test_count_only_nopart"
spark.sql(f"DROP TABLE IF EXISTS {full}")
spark.sql(f"""
    CREATE TABLE {full} (
        name STRING, city STRING, status STRING
    ) USING iceberg
""")
spark.sql(f"""
    INSERT INTO {full} VALUES
    ('alice','beijing','active'),
    ('bob','shanghai','inactive'),
    ('charlie','guangzhou','active'),
    ('david','shenzhen','active'),
    ('eve','hangzhou','inactive')
""")
print(f"Created {full}: 5 rows (matches Databricks)")

spark.stop()
