# dbx-diff: Databricks 与 EMR 数据一致性校验工具 (v3)

基于 PySpark 的数据对比工具，通过聚合统计值（count、max、min、avg）比较 Databricks（Delta on S3）和 EMR（Iceberg via Glue Catalog）之间的数据一致性。采用高并发方式执行，通过 `emr_common.Session` 提交至 EMR Serverless 运行。

## 架构

```
┌─────────────────┐         ┌──────────────────┐
│   Databricks    │         │   EMR Spark      │
│  (Delta on S3)  │         │  (Iceberg/Glue)  │
└────────┬────────┘         └────────┬─────────┘
         │                           │
         │  Unity Catalog REST API   │  SparkSession SQL
         │  → 获取 S3 存储路径       │  → 直接查询
         │  → spark.read.format      │
         │    ("delta").load(path)    │
         └───────────┬───────────────┘
                     │
              ┌──────┴──────┐
              │  dbx_diff.py │
              │  (PySpark    │
              │   Driver)    │
              └──────┬───────┘
                     │
              ThreadPoolExecutor
              (15-50 workers)
                     │
              ┌──────┴──────┐
              │  Markdown   │
              │  报告 → S3   │
              │  + stdout   │
              └─────────────┘
```

## 文件说明

| 文件 | 用途 |
|------|------|
| `dbx_diff.py` | 主比较引擎（PySpark 作业），基于聚合统计值对比 |
| `submit_job.py` | 基于 emr_common.Session 的作业提交脚本 |
| `emr_common.py` | EMR 通用类库（管理作业提交、轮询、日志获取） |
| `setup_test_data.py` | 功能测试数据创建（2张表，覆盖分区/非分区场景） |
| `tables.csv` | 输入 CSV 示例 |

## 快速开始

### 前置条件

- EMR Serverless 7.12 应用（emr-7.12.0），已配置好 Spark 相关参数
- Databricks workspace（启用 Unity Catalog）
- S3 存储桶（用于代码和报告）
- Lake Formation 权限（EMR 角色需有 Glue Catalog 访问权限）

### 提交作业

```bash
python3 submit_job.py \
  --csv tables.csv \
  --s3-output s3://your-bucket/reports/diff_report.md \
  --databricks-host https://your-workspace.cloud.databricks.com \
  --databricks-secret-arn arn:aws:secretsmanager:us-west-2:<account>:secret:<name>
```

`--csv` 传入本地文件路径。`submit_job.py` 内部通过 `emr_common.Session` 完成以下流程：
1. 上传本地 CSV 至 S3（带时间戳）
2. 自动将 `dbx_diff.py` 上传至 S3
3. 传入运行时环境变量（Databricks 凭证）
4. 调用 `session.submit_file()` 提交作业
5. 轮询等待完成，输出 Spark UI 链接
6. 清除 S3 上的临时 CSV 文件

> Spark 基础配置（Iceberg catalog、Python venv、动态分配等）已在 EMR Serverless Application 级别预配置，`submit_job.py` 仅传入业务参数。

## 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--csv` | （必填） | 输入 CSV 本地文件路径 |
| `--s3-output` | （必填） | S3 输出报告路径 |
| `--databricks-host` | （必填） | Databricks workspace URL |
| `--databricks-secret-arn` | 无 | AWS Secrets Manager ARN（OAuth2 认证，推荐） |
| `--databricks-token` | 无 | Databricks PAT（备选，secret-arn 优先） |
| `--workers` | 15 | 并行 worker 数 |
| `--timeout` | 600 | 单表超时时间（秒） |
| `--application-id` | （内置默认值） | EMR Serverless 应用 ID |
| `--execution-role-arn` | （内置默认值） | EMR Serverless 执行角色 ARN |

## 输入 CSV 格式

使用**空格**作为分隔符。

| 列名 | 是否必填 | 说明 |
|------|----------|------|
| `table_name` | 是 | 全路径表名：`catalog.schema.table`（两个引擎中表名一致） |
| `primary_keys` | 否 | 主键列，逗号分隔（如 `id,name`），当前版本保留字段 |
| `pt_keys` | 否 | 需精细对比的具体分区值，逗号分隔（如 `20250101,20250102`） |

示例：
```
table_name primary_keys pt_keys
workspace.demo2.test_nopk_nopart
workspace.demo2.test_nopk_part  20250101,20250102
```

## 比较规则

### 分区检测

系统通过 Databricks Unity Catalog API 批量预获取表的分区列信息。若表包含名为 `pt` 的分区列，则按分区表逻辑处理；否则按非分区表处理。

### 对比逻辑

| 场景 | 逻辑 |
|------|------|
| **非分区表** | 识别数值列（基于 Delta 表 schema），对数值列计算聚合值（count、max、min、avg），对比两侧结果 |
| **分区表** | 识别数值列，按 `pt_keys` 过滤分区，`GROUP BY pt` 计算聚合值（count、max、min、avg），逐分区对比 |

### 数值列识别

自动从 Delta 表 schema 中识别以下类型的列作为数值列：
- IntegerType、LongType、ShortType、ByteType
- FloatType、DoubleType、DecimalType

### 聚合指标

对每个数值列计算：
- `count(1)` — 总行数
- `max(col)` — 最大值
- `min(col)` — 最小值
- `avg(col)` — 平均值

## 输出报告

Markdown 格式报告，实时追加写入，完成后同时打印到 driver 日志（stdout）并上传至 S3。

### 非分区表报告示例

```markdown
### workspace.demo2.test_nopk_nopart — Aggregate Check: **FAIL**

- Numeric columns: `['id', 'value', 'score']`
- Delta count: 6, Iceberg count: 5

| Column | Metric | Delta Value | Iceberg Value |
|--------|--------|-------------|---------------|
| * | count | 6 | 5 |
| value | max | 99.9 | 50.9 |
| value | avg | 43.58 | 30.5 |
```

### 分区表报告示例

```markdown
### workspace.demo2.test_nopk_part — Partitioned Aggregate Check: **FAIL**

- Numeric columns: `['id', 'amount', 'score']`
- Partitions checked: `['20250101', '20250102']`

#### Partition pt=20250101 — **FAIL**

- Delta count: 3, Iceberg count: 3

| Column | Metric | Delta Value | Iceberg Value |
|--------|--------|-------------|---------------|
| amount | max | 999.0 | 200.0 |
| amount | avg | 449.67 | 150.0 |

#### Partition pt=20250102 — **FAIL**

- Delta count: 4, Iceberg count: 3

| Column | Metric | Delta Value | Iceberg Value |
|--------|--------|-------------|---------------|
| * | count | 4 | 3 |
```

## 认证方式

推荐使用 **OAuth2 client_credentials** 认证 Databricks API：

1. 在 Databricks 中创建 Service Principal 并生成 OAuth2 client_id 和 client_secret
2. 将凭证存入 AWS Secrets Manager，格式为明文 `client_id:client_secret`
3. 通过 `--databricks-secret-arn` 传入 Secret ARN

程序内部调用：
```python
# 1. 从 Secrets Manager 获取凭证
client_id, client_secret = get_secret_from_sm(secret_arn, region)

# 2. 通过 OAuth2 client_credentials 获取 access_token
token = get_oauth2_token(databricks_host, client_id, client_secret)

# 3. 使用 token 调用 Databricks API（token 自动刷新）
headers = {"Authorization": f"Bearer {token}"}
```

备选方式：通过 `--databricks-token` 传入 Personal Access Token（不推荐用于生产）

## EMR Serverless Application 配置

Spark 相关配置已在 Application 级别通过 `runtimeConfiguration` 预配置，无需在提交作业时重复指定：

```json
{
  "classification": "spark-defaults",
  "properties": {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.iceberg_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.iceberg_catalog.warehouse": "s3://your-bucket/emr/warehouse",
    "spark.executor.memory": "4g",
    "spark.executor.cores": "4",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.maxExecutors": "50",
    "spark.archives": "s3://your-bucket/code/pyspark_py312_venv.tar.gz#environment",
    "spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON": "./environment/bin/python3.12",
    "spark.emr-serverless.driverEnv.PYSPARK_PYTHON": "./environment/bin/python3.12",
    "spark.executorEnv.PYSPARK_PYTHON": "./environment/bin/python3.12",
    "spark.emr-serverless.driverEnv.LD_LIBRARY_PATH": "./environment/lib",
    "spark.executorEnv.LD_LIBRARY_PATH": "./environment/lib",
    "spark.emr-serverless.driverEnv.PYTHONHOME": "./environment",
    "spark.executorEnv.PYTHONHOME": "./environment"
  }
}
```

### 环境要求

1. EMR Serverless 应用版本：**emr-7.12.0**
2. VPC 配置 NAT Gateway（用于访问 Databricks API）
3. VPC 中配置 S3 Gateway Endpoint 和 Glue Interface Endpoint
4. Lake Formation 为执行角色授权
5. 执行角色需有 Secrets Manager `GetSecretValue` 权限

### Python 3.12 Venv 打包

```bash
# 创建 venv
python3.12 -m venv /tmp/pyspark_venv

# 安装依赖
/tmp/pyspark_venv/bin/pip install requests certifi boto3

# 拷贝系统库（确保 OpenSSL、zlib 等可用）
cp /lib64/libcrypto.so.3 /tmp/pyspark_venv/lib/
cp /lib64/libssl.so.3 /tmp/pyspark_venv/lib/
cp /lib64/libz.so.1 /tmp/pyspark_venv/lib/
cp /usr/lib64/libpython3.12.so.1.0 /tmp/pyspark_venv/lib/

# 拷贝 Python 标准库和 C 扩展模块
cp -r /usr/lib64/python3.12/* /tmp/pyspark_venv/lib/python3.12/
cp /usr/lib64/python3.12/lib-dynload/*.so /tmp/pyspark_venv/lib/python3.12/lib-dynload/

# 打包（从 venv 内部打包，确保 bin/lib 在顶层）
cd /tmp/pyspark_venv
tar -czf /tmp/pyspark_py312_venv.tar.gz .

# 上传至 S3
aws s3 cp /tmp/pyspark_py312_venv.tar.gz s3://your-bucket/code/pyspark_py312_venv.tar.gz
```

## emr_common.Session 使用说明

### 基本用法

```python
from emr_common import Session

session = Session(
    application_id="00g5qqg0spv6bq0l",
    jobtype=1,
    region="us-west-2",
    job_role="arn:aws:iam::<account>:role/EMRServerlessExecutionRole",
    logs_s3_path="s3://your-bucket/logs/",
    script_s3_path="s3://your-bucket/code/",
    spark_conf="--conf spark.emr-serverless.driverEnv.DATABRICKS_HOST=https://...",
)

result = session.submit_file(
    jobname="dbx_diff",
    local_file="/path/to/dbx_diff.py",
    args=["--csv", "s3://bucket/tables.csv", "--s3-output", "s3://bucket/report.md"],
)

print(f"状态: {result.status}")  # SUCCESS / FAILED
```

### submit_file 工作流程

1. 将本地脚本上传至 S3（带时间戳避免覆盖）
2. 构造 `sparkSubmit` 作业配置（合并 Application 级别配置 + spark_conf 参数）
3. 提交作业并轮询等待完成
4. 输出 Spark UI 链接
5. 返回 `EMRResult(job_run_id, status)`

### 注意事项

- Application 级别使用 `managedPersistenceMonitoringConfiguration`（不能使用 `s3MonitoringConfiguration`，否则与 `PYTHONHOME` 冲突）
- `spark_conf` 仅需传入运行时动态参数（如 Databricks 凭证），静态配置已在 Application 级别预设
