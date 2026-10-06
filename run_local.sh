#!/usr/bin/env bash
# Run the pipeline locally. Edit JAVA_HOME / the venv path if yours differ.
#
#   ./run_local.sh                 # uses the paths set in the script
#   OUT=output/run2 ./run_local.sh # write somewhere else
set -e
cd "$(dirname "$0")"

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
VENV="${VENV:-$HOME/venvs/project}"
export PYSPARK_PYTHON="$VENV/bin/python"
export SPARK_LOCAL_DIRS="${SPARK_LOCAL_DIRS:-/tmp/sparktmp}"
mkdir -p "$SPARK_LOCAL_DIRS" logs

STAMP=$(date +%Y%m%d-%H%M%S)
echo "starting run $STAMP  (log: logs/run-$STAMP.log)"
"$VENV/bin/python" AmazonReviewCoordination_single.py 2>&1 | tee "logs/run-$STAMP.log"
