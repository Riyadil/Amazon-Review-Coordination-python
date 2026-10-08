#!/usr/bin/env bash
# Run the pipeline once per category, sequentially.
#
# Optional diagnostic. The authoritative final result should use one all-category
# run, because separate runs miss pairs whose shared groups cross categories.
set -u
cd "$(dirname "$0")"

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
VENV="${VENV:-$HOME/venvs/project}"
export PYSPARK_PYTHON="$VENV/bin/python"
export SPARK_LOCAL_DIRS="${SPARK_LOCAL_DIRS:-/tmp/amazon-review-sparktmp}"
export SPARK_MASTER="${SPARK_MASTER:-local[4]}"
export DRIVER_MEMORY="${DRIVER_MEMORY:-8g}"
mkdir -p "$SPARK_LOCAL_DIRS" logs

CATEGORIES="Video_Games CDs_and_Vinyl Arts_Crafts_and_Sewing Baby_Products"
SUMMARY="logs/per_category_summary.txt"
: > "$SUMMARY"

for c in $CATEGORIES; do
  out="output/$(echo "$c" | tr 'A-Z' 'a-z')"
  log="logs/run-$c.log"
  echo "=== $c -> $out ==="
  rm -rf "$out"
  start=$(date +%s)
  REVIEWS_PATH="data/reviews/$c.jsonl.gz" \
  METADATA_PATH="data/meta/meta_$c.jsonl.gz" \
  OUTPUT_ROOT="$out" \
  "$VENV/bin/python" AmazonReviewCoordinationDF.py > "$log" 2>&1
  status=$?
  secs=$(( $(date +%s) - start ))
  if [ $status -eq 0 ] && grep -q "PIPELINE COMPLETED SUCCESSFULLY" "$log"; then
    result="OK"
  else
    result="FAILED"
  fi
  reviews=$(grep -oP 'Reviews processed\s*:\s*\K[0-9]+' "$log" | tail -1)
  pairs=$(grep -oP 'Repeated pairs\s*:\s*\K[0-9]+' "$log" | tail -1)
  verts=$(grep -oP 'Graph vertices: \K[0-9]+' "$log" | tail -1)
  printf '%-24s %-7s %5ss  reviews=%-10s pairs=%-6s vertices=%s\n' \
         "$c" "$result" "$secs" "${reviews:-?}" "${pairs:-?}" "${verts:-?}" | tee -a "$SUMMARY"
done

echo
echo "=== summary ==="
cat "$SUMMARY"
echo "ALL_CATEGORY_RUNS_FINISHED"
