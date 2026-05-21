import gzip
import time
import boto3
import json


class EMRResult:
    def __init__(self, job_run_id, status):
        self.job_run_id = job_run_id
        self.status = status


class Session:
    def __init__(
        self,
        application_id="",
        jobtype=0,  # 0：EMR on EC2; 1: serverless
        region="ap-southeast-1",
        job_role="arn:aws:iam::340636688520:role/AmazonEMR-ExecutionRole-1693493727586",
        logs_s3_path="s3://aws-logs-340636688520-ap-southeast-1/elasticmapreduce/",
        script_s3_path="s3://zpfsingapore/scripts/",
        spark_conf="--conf spark.executor.cores=4 --conf spark.executor.memory=16g --conf spark.driver.cores=4 --conf spark.driver.memory=16g",
    ):
        self.jobtype = jobtype
        self.application_id = application_id

        self.region = region
        self.job_role = job_role
        self.logs_s3_path = logs_s3_path
        self.script_s3_path = script_s3_path
        self.spark_conf = spark_conf

        self.client = boto3.client("emr", region_name=self.region)
        self.client_serverless = boto3.client("emr-serverless", region_name=self.region)

        if self.application_id == "":
            self.application_id = self.getDefaultApplicaitonId()

        if jobtype == 0:
            self.session = EmrSession(
                region=self.region,
                application_id=self.application_id,
                job_role=self.job_role,
                logs_s3_path=self.logs_s3_path,
                spark_conf=self.spark_conf,
            )
        else:
            self.session = EmrServerlessSession(
                region=self.region,
                application_id=self.application_id,
                job_role=self.job_role,
                logs_s3_path=self.logs_s3_path,
                spark_conf=self.spark_conf,
            )

    def submit_file(self, jobname, local_file, args=None):
        import os
        from datetime import datetime

        filename = os.path.basename(local_file)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        s3_key_prefix = "/".join(self.script_s3_path.split("/")[3:])
        s3_bucket = self.script_s3_path.split("/")[2]
        s3_key = f"{s3_key_prefix}{timestamp}_{filename}"

        s3_client = boto3.client("s3")
        s3_client.upload_file(local_file, s3_bucket, s3_key)
        remote_path = f"s3://{s3_bucket}/{s3_key}"
        print(f"已上传 {local_file} -> {remote_path}")

        return self.session.submit_file(jobname, remote_path, args)

    def getDefaultApplicaitonId(self):
        if self.jobtype == 0:
            emr_clusters = self.client.list_clusters(
                ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]
            )
            if emr_clusters["Clusters"]:
                app_id = emr_clusters["Clusters"][0]["Id"]
                print(f"选择默认的集群ID:{app_id}")
                return app_id
            else:
                raise Exception("没有找到活跃的EMR集群")
        else:
            emr_applications = self.client_serverless.list_applications()
            spark_applications = [
                app
                for app in emr_applications["applications"]
                if app["type"] == "Spark"
            ]
            if spark_applications:
                app_id = spark_applications[0]["id"]
                print(f"选择默认的应用ID:{app_id}")
                return app_id
            else:
                raise Exception("没有找到活跃的EMR Serverless应用")


class EmrSession:
    def __init__(self, region, application_id, job_role, logs_s3_path, spark_conf):
        self.region = region
        self.client = boto3.client("emr", region_name=self.region)
        self.application_id = application_id

        self.job_role = job_role
        self.logs_s3_path = logs_s3_path
        self.spark_conf = spark_conf

        self.client.modify_cluster(
            ClusterId=self.application_id, StepConcurrencyLevel=256
        )

    def submit_file(self, jobname, script_s3_path, args=None):
        print(f"Run File :{script_s3_path}")
        result = self._submit_job_emr(jobname, script_s3_path, args)
        return result

    def _submit_job_emr(self, jobname, script_file, args=None):
        spark_conf_args = self.spark_conf.split()
        entry_args = args if args else []

        jobconfig = [
            {
                "Name": f"{jobname}",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "cluster",
                        "--master",
                        "yarn",
                        "--conf",
                        "spark.yarn.submit.waitAppCompletion=true",
                    ]
                    + spark_conf_args
                    + [script_file]
                    + entry_args,
                },
            }
        ]
        response = self.client.add_job_flow_steps(
            JobFlowId=self.application_id, Steps=jobconfig
        )
        print(jobconfig)

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            print("task failed：")
            print(response)

        job_run_id = response["StepIds"][0]
        print(f"Submit job on EMR ,job id: {job_run_id}")
        job_done = False
        status = "PENDING"
        while not job_done:
            status = self.get_job_run(job_run_id)
            print(f"current status:{status}")
            job_done = status in [
                "SUCCESS",
                "FAILED",
                "CANCELLING",
                "CANCELLED",
                "COMPLETED",
            ]
            time.sleep(10)

        self.print_driver_log(job_run_id, log_type="stderr")
        self.print_driver_log(job_run_id, log_type="stdout")
        return EMRResult(job_run_id, status)

    def get_job_run(self, job_run_id: str) -> dict:
        step_status = self.client.describe_step(
            ClusterId=self.application_id, StepId=job_run_id
        )["Step"]["Status"]["State"]
        return step_status.upper()

    def print_driver_log(self, job_run_id: str, log_type: str = "stderr") -> str:
        print("starting download the driver logs")

        s3_client = boto3.client("s3")
        logs_location = (
            f"{self.logs_s3_path}{self.application_id}/steps/{job_run_id}/{log_type}.gz"
        )
        logs_bucket = logs_location.split("/")[2]
        logs_key = "/".join(logs_location.split("/")[3:])
        print(f"Fetching {log_type} from {logs_location}")
        try:
            # 日志生成需要一段时间 ，最长 100 秒
            for _ in range(10):
                try:
                    s3_client.head_object(Bucket=logs_bucket, Key=logs_key)
                    break
                except Exception:
                    print("等待日志生成中...")
                    time.sleep(10)
            response = s3_client.get_object(Bucket=logs_bucket, Key=logs_key)
            file_content = gzip.decompress(response["Body"].read()).decode("utf-8")
        except s3_client.exceptions.NoSuchKey:
            file_content = ""
            print(
                f"等待超时，请稍后到 EMR 集群的步骤中查看错误日志或者手动前往: {logs_location} 下载"
            )
        print(file_content)


class EmrServerlessSession:
    def __init__(self, region, application_id, job_role, logs_s3_path, spark_conf):
        self.region = region
        self.client = boto3.client("emr-serverless", region_name=self.region)
        self.application_id = application_id

        self.job_role = job_role
        self.logs_s3_path = logs_s3_path
        self.spark_conf = spark_conf

    def submit_file(self, jobname, script_s3_path, args=None):
        print(f"计划执行脚本 :{script_s3_path}")
        result = self._submit_job_emr(jobname, script_s3_path, args)
        return result

    def _submit_job_emr(self, name, script_file, args=None):
        job_driver = {
            "sparkSubmit": {
                "entryPoint": f"{script_file}",
                "sparkSubmitParameters": f"{self.spark_conf}",
            }
        }
        if args:
            job_driver["sparkSubmit"]["entryPointArguments"] = args
        print(f"job_driver:{job_driver}")
        response = self.client.start_job_run(
            applicationId=self.application_id,
            executionRoleArn=self.job_role,
            name=name,
            jobDriver=job_driver,
            executionTimeoutMinutes=1440,
            configurationOverrides={
                "monitoringConfiguration": {
                    "managedPersistenceMonitoringConfiguration": {
                        "enabled": True
                    }
                }
            },
        )

        job_run_id = response.get("jobRunId")
        print(f"Emr Serverless Job submitted, job id: {job_run_id}")

        job_done = False
        status = "PENDING"
        while not job_done:
            status = self.get_job_run(job_run_id).get("state")
            print(f"current status:{status}")
            job_done = status in [
                "SUCCESS",
                "FAILED",
                "CANCELLING",
                "CANCELLED",
            ]

            time.sleep(10)

        self.print_driver_log(job_run_id, log_type="stderr")
        self.print_driver_log(job_run_id, log_type="stdout")

        if status == "FAILED":
            raise Exception(f"EMR Serverless job failed:{job_run_id}")

        return EMRResult(job_run_id, status)

    def get_job_run(self, job_run_id: str) -> dict:
        response = self.client.get_job_run(
            applicationId=self.application_id, jobRunId=job_run_id
        )
        return response.get("jobRun")

    def print_driver_log(self, job_run_id: str, log_type: str = "stderr") -> str:
        try:
            resp = self.client.get_dashboard_for_job_run(
                applicationId=self.application_id, jobRunId=job_run_id
            )
            print(f"Spark UI: {resp.get('url', 'N/A')}")
        except Exception:
            pass

        s3_client = boto3.client("s3")
        logs_location = f"{self.logs_s3_path}applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/{log_type}.gz"
        logs_bucket = logs_location.split("/")[2]
        logs_key = "/".join(logs_location.split("/")[3:])
        print(f"Fetching {log_type} from {logs_location}")
        try:
            for _ in range(6):
                try:
                    s3_client.head_object(Bucket=logs_bucket, Key=logs_key)
                    break
                except Exception:
                    print("等待日志生成中...")
                    time.sleep(10)
            response = s3_client.get_object(Bucket=logs_bucket, Key=logs_key)
            file_content = gzip.decompress(response["Body"].read()).decode("utf-8")
        except Exception as e:
            file_content = f"日志通过 managed persistence 存储，请查看 Spark UI 或 EMR 控制台"
        print(file_content)


class DDBUtil:
    @staticmethod
    def setOndemand(table_name):
        # 初始化DynamoDB客户端
        dynamodb_client = boto3.client("dynamodb", region_name="ap-southeast-1")

        # 更新DynamoDB表的计费模式为按需
        dynamodb_client.update_table(
            TableName=table_name, BillingMode="PAY_PER_REQUEST"
        )

    @staticmethod
    def setWriteProvision(table_name, min, max):
        DDBUtil.setProvision(table_name, min, max, "dynamodb:table:WriteCapacityUnits")

    @staticmethod
    def setReadProvision(table_name, min, max):
        DDBUtil.setProvision(table_name, min, max, "dynamodb:table:ReadCapacityUnits")

    @staticmethod
    def setProvision(table_name, min, max, scalable_dimension):
        dynamodb_client = boto3.client("dynamodb", region_name="ap-southeast-1")
        autoscaling_client = boto3.client(
            "application-autoscaling", region_name="ap-southeast-1"
        )

        # 检查当前计费模式是否为预置容量
        current_table = dynamodb_client.describe_table(TableName=table_name)
        if current_table["Table"]["BillingModeSummary"]["BillingMode"] != "PROVISIONED":
            # 更新DynamoDB表的计费模式为预置容量
            dynamodb_client.update_table(
                TableName=table_name,
                BillingMode="PROVISIONED",
                ProvisionedThroughput={
                    "ReadCapacityUnits": 5,  # 使用方法参数
                    "WriteCapacityUnits": 5,  # 使用方法参数
                },
            )

        # 为DynamoDB表设置自动伸缩策略
        resource_id = f"table/{table_name}"

        # 注册或更新可伸缩目标
        autoscaling_client.register_scalable_target(
            ServiceNamespace="dynamodb",
            ResourceId=resource_id,
            ScalableDimension=scalable_dimension,
            MinCapacity=min,
            MaxCapacity=max,
        )

        # 创建或更新伸缩策略
        metric_type = (
            "DynamoDBReadCapacityUtilization"
            if "Read" in scalable_dimension
            else "DynamoDBWriteCapacityUtilization"
        )
        autoscaling_client.put_scaling_policy(
            PolicyName="YourScalingPolicyName",
            ServiceNamespace="dynamodb",
            ResourceId=resource_id,
            ScalableDimension=scalable_dimension,
            PolicyType="TargetTrackingScaling",
            TargetTrackingScalingPolicyConfiguration={
                "TargetValue": 70.0,
                "ScaleInCooldown": 60,
                "ScaleOutCooldown": 60,
                "PredefinedMetricSpecification": {
                    "PredefinedMetricType": metric_type  # 根据scalable_dimension调整
                },
            },
        )

