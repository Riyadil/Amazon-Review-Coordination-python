#!/usr/bin/env bash
set -euo pipefail

: "${S3_CODE_URI:?Set S3_CODE_URI to the uploaded AmazonReviewCoordinationDF.py}"
: "${REVIEWS_PATH:?Set REVIEWS_PATH to an explicit s3:// or s3a:// review-data path}"
: "${METADATA_PATH:?Set METADATA_PATH to an explicit s3:// or s3a:// metadata path}"
: "${OUTPUT_ROOT:?Set OUTPUT_ROOT to a new S3 result prefix}"

RUN_LABEL="${RUN_LABEL:-emr-run}"
LOCAL_CODE="/tmp/AmazonReviewCoordinationDF.py"
aws s3 cp "$S3_CODE_URI" "$LOCAL_CODE"

export SPARK_MASTER=yarn
export DRIVER_MEMORY="${DRIVER_MEMORY:-8g}"
export SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-400}"
export CHECKPOINT_DIR="${CHECKPOINT_DIR:-hdfs:///user/hadoop/amazon-review-checkpoints/$RUN_LABEL}"
export USE_GRAPHFRAMES=1
export GRAPHFRAMES_PACKAGE="${GRAPHFRAMES_PACKAGE:-graphframes:graphframes:0.8.4-spark3.5-s_2.12}"

# In client mode the driver JVM is already running by the time the code calls
# SparkSession.builder, so spark.driver.memory must be given to spark-submit.
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory "$DRIVER_MEMORY" \
  --repositories https://repos.spark-packages.org \
  --packages "$GRAPHFRAMES_PACKAGE" \
  "$LOCAL_CODE"

