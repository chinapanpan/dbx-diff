# dbx-diff: Databricks 与 EMR 数据一致性校验工具

基于 PySpark 的数据对比工具，用于验证 Databricks（Delta on S3）和 EMR（Iceberg via Glue Catalog）之间的数据一致性。支持生产规模（5000+ 张表），采用高并发方式执行。

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
              └─────────────┘
```

## 文件说明

| 文件 | 用途 |
|------|------|
| `dbx_diff.py` | 主比较引擎（PySpark 作业） |
| `submit_job.py` | EMR Serverless / YARN 提交脚本 |
| `setup_test_data.py` | 功能测试数据创建（4张表，覆盖所有场景） |
| `perf_test_setup.py` | 性能测试数据创建（批量 N 张表） |
| `tables.csv` | 输入 CSV 示例 |

## 快速开始

### 前置条件

- EMR 7.12 集群 或 EMR Serverless 7.12 应用
- Databricks workspace（启用 Unity Catalog）
- S3 存储桶（用于代码和报告）
- Lake Formation 权限（EMR 角色需有 Glue Catalog 访问权限）
- Python 3.12 venv 打包上传至 S3（用于 EMR Serverless）

### 在 YARN 上运行（EMR 集群）

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"

python3 submit_job.py --mode yarn \
  --csv tables.csv \
  --s3-output s3://your-bucket/reports/diff_report.md \
  --databricks-host $DATABRICKS_HOST \
  --databricks-token $DATABRICKS_TOKEN
```

### 在 EMR Serverless 上运行

```bash
python3 submit_job.py --mode serverless \
  --application-id <emr-serverless-app-id> \
  --execution-role-arn arn:aws:iam::<account>:role/EMRServerlessExecutionRole \
  --csv s3://your-bucket/code/tables.csv \
  --s3-output s3://your-bucket/reports/diff_report.md \
  --databricks-host $DATABRICKS_HOST \
  --databricks-token $DATABRICKS_TOKEN \
  --venv-archive s3://your-bucket/code/pyspark_py312_venv.tar.gz
```

### 直接使用 spark-submit

```bash
spark-submit \
  --master yarn \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.iceberg_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog" \
  --conf "spark.sql.catalog.iceberg_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
  dbx_diff.py \
  --csv tables.csv \
  --s3-output s3://your-bucket/reports/diff_report.md \
  --databricks-host $DATABRICKS_HOST \
  --databricks-token $DATABRICKS_TOKEN
```

## 输入 CSV 格式

使用**空格**作为分隔符（因为 `primary_keys` 列内部使用逗号分隔多个主键）。

| 列名 | 是否必填 | 说明 |
|------|----------|------|
| `table_name` | 是 | 全路径表名：`catalog.schema.table`（两个引擎中表名一致） |
| `primary_keys` | 否 | 主键列，逗号分隔（如 `id,name`） |
| `pt_start` | 否 | 分区范围起始值（如 `20250101`），非分区表留空 |
| `pt_end` | 否 | 分区范围结束值（如 `20250610`），非分区表留空 |
| `pt_keys` | 否 | 需精细对比的具体分区值，逗号分隔（如 `20250101,20250102`） |
| `dbx_location` | 否 | Databricks 表的 S3 路径（留空则自动通过 API 获取） |

示例：
```
table_name primary_keys pt_start pt_end pt_keys
workspace.demo2.test_pk_nopart id
workspace.demo2.test_pk_part id,name 20250101 20250103 20250101,20250102
workspace.demo2.test_nopk_nopart
workspace.demo2.test_nopk_part  20250101 20250103 20250101,20250102
```

## 比较规则

| 场景 | 逻辑 |
|------|------|
| **有主键 + 非分区表** | 逐行对比：仅在 Databricks、仅在 EMR、值不匹配 |
| **有主键 + 分区表** | 比较分区数量 → 各分区记录数 → 对 pt_keys 指定分区做逐行对比 |
| **无主键 + 非分区表** | 比较总记录行数 |
| **无主键 + 分区表** | 比较分区数量 → 各分区记录数 |

**分区检测**：系统通过 Databricks Unity Catalog API 批量预获取表的分区列信息。只有实际包含 `pt` 分区列的表才按分区表逻辑处理，即使 CSV 中填写了 `pt_start`/`pt_end`。

## 输出报告

Markdown 格式报告，实时追加写入，最终上传至 S3。示例：

```markdown
### workspace.demo2.test_pk_nopart — Row Check: **FAIL**

| 指标 | 数量 |
|------|------|
| 仅在 Databricks | 1 |
| 仅在 EMR | 0 |
| 值不匹配 | 1 |

**值不匹配样例**（最多 10 条）：

| 主键 | 列 | Databricks | EMR |
|------|-----|------------|-----|
| {'id': '3'} | value | 999 | 300 |
```

## 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--csv` | （必填） | 输入 CSV 路径（本地路径或 S3 路径） |
| `--s3-output` | （必填） | S3 输出报告路径 |
| `--databricks-host` | 环境变量 `DATABRICKS_HOST` | Databricks workspace URL |
| `--databricks-token` | 环境变量 `DATABRICKS_TOKEN` | Databricks 访问令牌 |
| `--databricks-secret-arn` | 无 | AWS Secrets Manager ARN（OAuth2 模式，优先于 token） |
| `--workers` | 15 | 并行 worker 数 |
| `--timeout` | 600 | 单表超时时间（秒） |
| `--emr-catalog` | `iceberg_catalog` | EMR Iceberg catalog 名称 |
| `--emr-warehouse` | `s3://zpf-databricks-event/emr/demo2` | Iceberg warehouse 路径 |
| `--mode` | `serverless` | 执行模式：`serverless` 或 `yarn` |
| `--venv-archive` | （内置默认值） | Python 3.12 venv tar.gz 的 S3 路径 |

## 认证方式

支持两种 Databricks 认证方式（优先级从高到低）：

1. **OAuth2（推荐）**：通过 `--databricks-secret-arn` 指定 AWS Secrets Manager 中的 client_id/client_secret
2. **Personal Access Token**：通过 `--databricks-token` 或环境变量 `DATABRICKS_TOKEN`

## EMR Serverless 部署

### 环境要求

1. EMR Serverless 应用版本：**emr-7.12.0**
2. VPC 配置 NAT Gateway（用于访问 Databricks API）
3. VPC 中配置 S3 Gateway Endpoint 和 Glue Interface Endpoint
4. Lake Formation 为执行角色授权
5. Python 3.12 venv 打包上传至 S3

### Python 3.12 Venv 打包

EMR Serverless 通过 `spark.archives` 分发自定义 Python 环境：

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

### Spark 配置

提交脚本自动配置以下参数：

```
spark.archives=s3://your-bucket/code/pyspark_py312_venv.tar.gz#environment
spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python3.12
spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python3.12
spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python3.12
spark.emr-serverless.driverEnv.LD_LIBRARY_PATH=./environment/lib
spark.executorEnv.LD_LIBRARY_PATH=./environment/lib
spark.emr-serverless.driverEnv.PYTHONHOME=./environment
spark.executorEnv.PYTHONHOME=./environment
```

## 性能测试

使用 `perf_test_setup.py` 批量创建测试表：

```bash
export DATABRICKS_TOKEN="your-token"
export NUM_TABLES=90

spark-submit perf_test_setup.py
```

每 5 张表会引入一个数据差异用于验证检测能力。

### 测试结果（90 张表）

| 指标 | 数值 |
|------|------|
| 总耗时 | 328.6s |
| 元数据预获取 | 25.7s |
| PASS 数 | 432 |
| FAIL 数 | 18（符合预期） |
| 并行 workers | 15 |
| 运行环境 | EMR Serverless 7.12 |

## 功能测试

使用 `setup_test_data.py` 创建 4 张测试表，覆盖全部对比场景：

```bash
export DATABRICKS_TOKEN="your-token"
spark-submit setup_test_data.py all
```

测试表包含有意引入的差异，用于验证各种比较规则的正确性。
