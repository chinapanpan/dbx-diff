"""Create test_no_numeric_pt in EMR (Iceberg) - baseline data without the extra row."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SetupNoNumeric").getOrCreate()

CATALOG = "workspace"
SCHEMA = "demo2"

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SCHEMA}")

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
print(f"Created {full} (5 rows, no extra row in pt=20250102)")
spark.stop()
