"""Create test_no_numeric_multi_pt in EMR - pt=20250102 has 3 rows (Databricks has 4)."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SetupNoNumericMulti").getOrCreate()

CATALOG = "workspace"
SCHEMA = "demo2"

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SCHEMA}")

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
spark.stop()
