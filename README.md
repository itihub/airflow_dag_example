
## Airflow 日志  

如何将日志保存到远程存储  

日志本地存储
```
[logging]
# The folder where airflow should store its log files
# This path must be absolute
base_log_folder = /home/ec2-user/airflow/logs
```

日志存储到AWS S3
```
[logging]
# Airflow can store logs remotely in AWS S3, Google Cloud Storage or Elastic Search.
# Set this to True if you want to enable remote logging.
remote_logging = False

# Users must supply an Airflow connection id that provides access to the storage
# location.
remote_log_conn_id =


# Storage bucket URL for remote logging
# S3 buckets should start with "s3://"
# Cloudwatch log groups should start with "cloudwatch://"
# GCS buckets should start with "gs://"
# WASB buckets should start with "wasb" just to help Airflow select correct handler
# Stackdriver logs should start with "stackdriver://"
remote_base_log_folder =

# Use server-side encryption for logs stored in S3
encrypt_s3_logs = False
```

## Airflow CLI

Airflow CLI 执行命令

## Airflow Rest API

## Airflow Config

## Airflow Metadata  


## Airflow 宏变量  

参考链接 https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html