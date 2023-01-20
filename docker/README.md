# docker-compose

The docker-compose environment consists of:

- a standalone Flink cluster (`jobmanager` and `taskmanager` containers).
- an "edge" node from which Flink jobs can be scheduled (`flink-sql-runner-node` container).
- minio instance which serves as AWS S3 substitute.
- a short-living container `createbuckets` which configures minio, namely, creates necessary buckets, policies and
  users.

Flink UI can be accessed at `http://localhost:8081`.
Minio UI can be accessed at `http://localhost:9001` (`user`/`password`).

---

## Running example

`flink-sql-runner-node` container already contains all necessary scripts and example sqls.

Sample run commands:

```bash
docker compose -f docker-compose/docker-compose.yaml up -d

docker exec -it docker-flink-sql-runner-node-1 bash

python3 /opt/flink-sql-runner/deployment-scripts/jobs-deployment/deploy.py \
    --path /opt/flink-sql-runner/example/sql/queries/ \
    --template-file /opt/flink-sql-runner/example/job-template.yaml \
    --pyflink-runner-dir /opt/flink-sql-runner/python/ \
    --pyexec-path /usr/local/bin/python3 \
    --external-job-config-bucket test-bucket \
    --external-job-config-prefix flink-sql-runner/manifests/ \
    --table-definition-path /opt/flink-sql-runner/example/sql/schemas/orders.sql \
    --deployment-target remote \
    --jobmanager-address jobmanager:8081
```

---

## Internals


### Docker image enhancements

- [Using Flink Python on Docker](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/resource-providers/standalone/docker/#using-flink-python-on-docker)
- [Enable HDFS plugin](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/filesystems/s3/#hadooppresto-s3-file-systems-plugins)
  by adding environment variable in docker Flink services.
  See [Using Filesystem Plugins](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/resource-providers/standalone/docker/#using-filesystem-plugins)
  for more details.
  ```yaml
    environment:
      - ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.15.1.jar
  ```

### MinIO

MinIO serves as S3 replacement in docker environment. To make the framework work properly with MinIO, a few adjustments
have been made.

- Specify `endpoint_url` in Python S3 clients. To this end, `AWS_S3_ENDPOINT` environment variable have been introduced.
  If the variable is specified, then `endpoint_url=${AWS_S3_ENDPOINT}`.
- `flink-conf.yaml` has to contain the following properties:
  ```yaml
  s3.endpoint: http://minio:9000
  s3.path-style-access: true
  s3.access-key: flink-sql-runner
  s3.secret-key: secretkey
  ```
  See [Configure Access Credentials](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/filesystems/s3/#configure-access-credentials)
  ,
  [Configure Non-S3 Endpoint](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/filesystems/s3/#configure-access-credentials)
  and [Configure Path Style Access](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/filesystems/s3/#configure-path-style-access).
