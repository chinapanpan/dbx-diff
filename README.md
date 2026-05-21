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
| `submit_job.py` | 基于 emr_common.Session 的作业提交脚本 |
| `emr_common.py` | EMR 通用类库（支持 EMR on EC2 和 Serverless） |
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

### 在 EMR Serverless 上运行（推荐）

```bash
python3 submit_job.py --mode serverless \
  --csv s3://your-bucket/code/tables.csv \
  --s3-output s3://your-bucket/reports/diff_report.md \
  --databricks-host https://your-workspace.cloud.databricks.com \
  --databricks-token <token>
```

### 在 EMR on EC2 上运行

```bash
python3 submit_job.py --mode ec2 \
  --csv tables.csv \
  --s3-output s3://your-bucket/reports/diff_report.md \
  --databricks-host https://your-workspace.cloud.databricks.com \
  --databricks-token <token>
```

### 直接使用 spark-submit（本地调试）

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"

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
| `--mode` | `serverless` | 执行模式：`serverless` 或 `ec2` |
| `--application-id` | （内置默认值） | EMR Serverless 应用 ID |
| `--execution-role-arn` | （内置默认值） | EMR Serverless 执行角色 ARN |
| `--venv-archive` | （内置默认值） | Python 3.12 venv tar.gz 的 S3 路径 |

## 认证方式

支持两种 Databricks 认证方式（优先级从高到低）：

1. **OAuth2（推荐）**：通过 `--databricks-secret-arn` 指定 AWS Secrets Manager 中的 client_id/client_secret
2. **Personal Access Token**：通过 `--databricks-token` 或环境变量 `DATABRICKS_TOKEN`

## emr_common.py 类库

`submit_job.py` 基于 `emr_common.py` 提交作业。`emr_common.py` 提供统一的 `Session` 类，支持 EMR on EC2 和 EMR Serverless 两种模式。

### 核心类

| 类 | 说明 |
|------|------|
| `Session` | 入口类，根据 `jobtype` 自动选择 EC2 或 Serverless 后端 |
| `EmrSession` | EMR on EC2 后端（通过 add_job_flow_steps 提交） |
| `EmrServerlessSession` | EMR Serverless 后端（通过 start_job_run 提交） |

### 基本用法

```python
from emr_common import Session

# 创建 session（jobtype=1 为 Serverless，jobtype=0 为 EC2）
session = Session(
    application_id="00g5qqg0spv6bq0l",
    jobtype=1,
    region="us-west-2",
    job_role="arn:aws:iam::415797100173:role/EMRServerlessExecutionRole",
    logs_s3_path="s3://your-bucket/logs/",
    script_s3_path="s3://your-bucket/code/",
    spark_conf="--conf spark.executor.memory=4g --conf spark.executor.cores=4",
)

# 提交本地脚本（自动上传至 S3 并等待完成）
result = session.submit_file(
    jobname="my-job",
    local_file="/path/to/script.py",
    args=["--arg1", "value1"],
)

print(f"状态: {result.status}")  # SUCCESS / FAILED / COMPLETED
```

### Session 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `application_id` | 自动获取 | EMR 集群 ID 或 Serverless 应用 ID |
| `jobtype` | 0 | 0=EMR on EC2，1=EMR Serverless |
| `region` | `ap-southeast-1` | AWS 区域 |
| `job_role` | （内置值） | 执行角色 ARN |
| `logs_s3_path` | （内置值） | 日志存储路径 |
| `script_s3_path` | （内置值） | 脚本上传路径 |
| `spark_conf` | （内置值） | spark-submit 参数字符串 |

### submit_file 工作流程

1. 将本地脚本上传至 S3（带时间戳避免覆盖）
2. 构造 `sparkSubmit` 作业配置
3. 提交作业并轮询等待完成
4. 输出 Spark UI 链接和驱动日志
5. 返回 `EMRResult(job_run_id, status)`

### 注意事项

- 使用 `spark.archives` 分发 Python 3.12 venv 时，`configurationOverrides` 必须使用 `managedPersistenceMonitoringConfiguration`（不能使用 `s3MonitoringConfiguration`，否则会与 `PYTHONHOME` 冲突导致 `No module named 'encodings'`）
- `application_id` 留空时会自动获取第一个可用的集群/应用
- EMR on EC2 模式下会自动设置 `StepConcurrencyLevel=256` 以支持并发步骤

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
