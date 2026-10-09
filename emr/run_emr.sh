#!/usr/bin/env bash
set -euo pipefail

: "${S3_CODE_URI:?Set S3_CODE_URI to the uploaded AmazonReviewCoordinationDF.py}"
: "${REVIEWS_PATH:?Set REVIEWS_PATH to an explicit s3:// or s3a:// review-data path}"
: "${METADATA_PATH:?Set METADATA_PATH to an explicit s3:// or s3a:// metadata path}"
: "${OUTPUT_ROOT:?Set OUTPUT_ROOT to a new S3 result prefix}"

RUN_LABEL="${RUN_LABEL:-emr-run}"
LOCAL_CODE="/tmp/AmazonReviewCoordinationDF.py"
aws s3 cp "$S3_CODE_URI" "$LOCAL_CODE"

# EMR does not always export these to non-login shells, and without them
# Spark ignores the cluster's Hadoop config and stages the job on the primary
# node's local disk, where the ApplicationMaster on a core node cannot read
# it.
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"
export YARN_CONF_DIR="${YARN_CONF_DIR:-/etc/hadoop/conf}"

export SPARK_MASTER=yarn
# In client mode the driver shares the primary node with the NameNode, the
# ResourceManager and the instance controller. An 8g heap on an m5.xlarge left
# too little for them: the step-runner restarted mid-job and the driver was
# killed with SIGTERM after about 50 minutes.
export DRIVER_MEMORY="${DRIVER_MEMORY:-4g}"
export SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-400}"
export USE_GRAPHFRAMES=1
export GRAPHFRAMES_PACKAGE="${GRAPHFRAMES_PACKAGE:-graphframes:graphframes:0.8.4-spark3.5-s_2.12}"

# Ask HDFS itself for fs.defaultFS and pass it to Spark explicitly, so the
# staging directory and every scheme-less path land in HDFS rather than on the
# primary node's local disk. Note the full host:port form throughout - an
# "hdfs:///" prefix collapses to a host-less URI that HDFS rejects.
DEFAULT_FS="$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || true)"
EXTRA_CONF=()
if [ -n "$DEFAULT_FS" ]; then
  echo "using fs.defaultFS=$DEFAULT_FS"
  EXTRA_CONF+=(--conf "spark.hadoop.fs.defaultFS=$DEFAULT_FS")
  # Set the staging directory directly as well, so a run does not depend on
  # fs.defaultFS reaching Spark before the YARN client builds its file list.
  EXTRA_CONF+=(--conf "spark.yarn.stagingDir=${DEFAULT_FS%/}/user/hadoop")
  # EMR's own defaults use host-less URIs such as "hdfs:///var/log/spark/apps".
  # Hadoop only fills in the host when fs.defaultFS is itself an hdfs:// URI,
  # so give these the full host:port rather than relying on that.
  EXTRA_CONF+=(--conf "spark.eventLog.dir=${DEFAULT_FS%/}/var/log/spark/apps")
  EXTRA_CONF+=(--conf "spark.sql.warehouse.dir=${DEFAULT_FS%/}/user/spark/warehouse")
  # Rewrite any other host-less hdfs:/// default from spark-defaults.conf.
  while IFS= read -r line; do
    key="${line%%=*}"; val="${line#*=}"
    EXTRA_CONF+=(--conf "${key}=${val/hdfs:\/\/\//${DEFAULT_FS%/}/}")
    echo "  rewriting host-less URI: $key"
  done < <(sed -n 's/^[[:space:]]*\([a-zA-Z0-9._]*\)[[:space:]=]\+\(hdfs:\/\/\/[^[:space:]]*\).*/\1=\2/p' \
             /etc/spark/conf/spark-defaults.conf 2>/dev/null \
           | grep -vE '^(spark.eventLog.dir|spark.sql.warehouse.dir)=' || true)
else
  echo "WARNING: could not read fs.defaultFS; staging may land on local disk" >&2
fi

# setCheckpointDir resolves a scheme-less path against Spark's own
# hadoopConfiguration, so give the checkpoint directory an explicit
# hdfs://host:port prefix and create it up front, where a permissions problem
# surfaces here rather than part-way into the job.
if [ -z "${CHECKPOINT_DIR:-}" ]; then
  export CHECKPOINT_DIR="${DEFAULT_FS%/}/user/hadoop/amazon-review-checkpoints/$RUN_LABEL"
fi
echo "using checkpoint dir=$CHECKPOINT_DIR"
hdfs dfs -mkdir -p "$CHECKPOINT_DIR"

# In client mode the driver JVM is already running by the time the code calls
# SparkSession.builder, so spark.driver.memory must be given to spark-submit.
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory "$DRIVER_MEMORY" \
  "${EXTRA_CONF[@]}" \
  --conf spark.ui.retainedJobs=200 \
  --conf spark.ui.retainedStages=200 \
  --conf spark.ui.retainedTasks=10000 \
  --conf spark.sql.ui.retainedExecutions=200 \
  --repositories https://repos.spark-packages.org \
  --packages "$GRAPHFRAMES_PACKAGE" \
  "$LOCAL_CODE"

