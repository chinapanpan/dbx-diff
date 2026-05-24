# dbx-diff: Databricks 与 EMR 数据一致性校验工具 (v5 - Scheduler Edition)

基于 PySpark 的数据对比工具，通过聚合统计值（count、sum、max、min）比较 Databricks（Delta on S3）和 EMR（Iceberg via Glue Catalog）之间的数据一致性。结果写入 Iceberg 表。

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
              (15 workers)
                     │
              ┌──────┴──────┐
              │  Iceberg    │
              │  结果表      │
              │  + stdout   │
              └─────────────┘
```

## 文件说明

| 文件 | 用途 |
|------|------|
| `dbx_diff.py` | 主比较引擎（PySpark 作业），参数通过 --widgets JSON 传递 |
| `submit_job.py` | 基于 emr_common.Session 的作业提交脚本 |
| `emr_common.py` | EMR 通用类库（管理作业提交、轮询、日志获取） |
| `setup_v5_test.py` | EMR 侧测试数据创建 |
| `setup_v5_databricks.py` | Databricks 侧测试数据创建 |

## 快速开始

### 前置条件

- EMR Serverless 7.12 应用（emr-7.12.0），已配置好 Spark 相关参数
- Databricks workspace（启用 Unity Catalog）
- S3 存储桶（用于代码）
- Lake Formation 权限（EMR 角色需有 Glue Catalog 访问权限）
- Iceberg 结果表已提前创建

### 提交作业

```bash
python3 submit_job.py \
  --table-name workspace.demo2.test_nopk_part \
  --iceberg-output workspace.demo2.verify_result \
  --databricks-host https://dbc-51ad87e6-c26d.cloud.databricks.com \
  --databricks-secret-arn arn:aws:secretsmanager:us-west-2:785682719467:secret:databricks-6lajvp \
  --pt-start 20260521 \
  --pt-end 20260522 \
  --task-id task001 \
  --instance-id inst001 \
  --attemp-id att001
```

## 命令行参数 (submit_job.py)

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--table-name` | （必填） | 待比较表名，全路径：catalog.db.tablename |
| `--iceberg-output` | （必填） | 结果写入的 Iceberg 表名（全路径） |
| `--databricks-host` | （必填） | Databricks workspace URL |
| `--databricks-secret-arn` | （必填） | AWS Secrets Manager ARN（OAuth2 认证） |
| `--pt-start` | （必填） | 分区起始值（含），如 `20260521` |
| `--pt-end` | （必填） | 分区结束值（含），如 `20260522` |
| `--task-id` | "" | 任务 ID（写入 Iceberg 结果表） |
| `--instance-id` | "" | 实例 ID（写入 Iceberg 结果表） |
| `--attemp-id` | "" | 尝试 ID（写入 Iceberg 结果表） |
| `--workers` | 15 | 并行 worker 数 |
| `--timeout` | 1800 | 单表超时时间（秒） |
| `--region` | us-west-2 | AWS Secrets Manager 区域 |
| `--application-id` | （内置默认值） | EMR Serverless 应用 ID |
| `--execution-role-arn` | （内置默认值） | EMR Serverless 执行角色 ARN |

## dbx_diff.py 参数传递

所有参数通过 `--widgets` 以 JSON 字符串形式传递：

```json
{
  "table_name": "workspace.demo2.test_table",
  "iceberg-output": "workspace.demo2.verify_result",
  "databricks-host": "https://xxx.cloud.databricks.com",
  "databricks-secret-arn": "arn:aws:secretsmanager:...",
  "pt-start": "20260521",
  "pt-end": "20260522",
  "workers": "15",
  "timeout": "1800",
  "region": "us-west-2",
  "task_id": "task001",
  "instance_id": "inst001",
  "attemp_id": "att001"
}
```

## Iceberg 结果表 DDL

```sql
CREATE TABLE workspace.demo2.verify_result (
    task_id STRING,
    instance_id STRING,
    attemp_id STRING,
    table_name STRING,
    pt BIGINT,
    result STRING COMMENT 'Y 表示通过, N 表示失败',
    details ARRAY<STRUCT<
        field_name: STRING,
        field_type: STRING,
        task_type: STRING COMMENT 'count, max, min, sum 中一个',
        delta_value: STRING,
        iceberg_value: STRING,
        result: STRING COMMENT 'Y 表示通过, N 表示失败'
    >> COMMENT '多个字段校验的细节'
)
USING ICEBERG
COMMENT 'databricks与emr 数据比对分区结果表';
```

## 分区过滤逻辑

当指定 `pt-start` 和 `pt-end` 时，对分区表使用以下过滤条件：

```sql
WHERE pt >= '{pt_start}' AND pt < '{pt_end + 1}'
```

## 比较规则

### 分区检测

系统通过 Databricks Unity Catalog API 预获取表的分区列信息。若表包含名为 `pt` 的分区列，按分区表逻辑处理。

### 对比逻辑

| 场景 | 逻辑 |
|------|------|
| **非分区表** | 识别数值列，计算聚合值（count、sum、max、min），对比两侧结果 |
| **分区表** | 识别数值列，按分区范围过滤，GROUP BY pt 计算聚合值，逐分区对比 |
| **无数值列** | 仅对比 count |

### 结果输出

- Driver 日志中打印详细对比信息
- 结果写入 Iceberg 表（每个分区一行记录）

## 认证方式

通过 `--databricks-secret-arn` 传入 AWS Secrets Manager 中存储的凭证：

- **OAuth2 client_credentials**：Secret 内容为 `client_id:client_secret`
- **Personal Access Token**：Secret 内容为 PAT 字符串（以 `dapi` 开头）

## EMR Serverless Application 配置

```json
{
  "classification": "spark-defaults",
  "properties": {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalog.workspace": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.workspace.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.workspace.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.workspace.warehouse": "s3://your-bucket/emr/warehouse",
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
