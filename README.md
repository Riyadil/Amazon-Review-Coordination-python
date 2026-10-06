# Amazon Review Coordination

CSC 7740 final project. A PySpark pipeline that looks for groups of Amazon
accounts which repeatedly review the same products within the same short time
windows, across the Amazon Reviews'23 dataset.

Group members: Asif Faisal Chowdhury, Souhardya Saha Dip, Riyadil Zannat.

## Source of truth

`AmazonReviewCoordination_single.py` is the only implementation. It is
self-contained: configuration, helpers and the pipeline all live in that file,
and it imports nothing beyond PySpark and the standard library.

The earlier split version (`AmazonReviewPipeline.py` + `Config.py` + `Utils.py`)
has been removed. It had drifted out of date and still expected the older
Amazon field names, so it silently produced no results on Reviews'23.

## Repository layout

```text
AmazonReviewCoordination_single.py   the pipeline (single source of truth)
run_local.sh                         convenience runner, writes logs/run-<stamp>.log
requirements.txt                     pyspark 3.5.7, numpy, setuptools
data/reviews/                        *.jsonl.gz review files      (git-ignored)
data/meta/                           *.jsonl.gz metadata files    (git-ignored)
output/                              pipeline results, Parquet    (git-ignored)
logs/                                run logs (kept; small)
analysis/                            standalone measurement scripts + download script
project_topic.pdf                    the submitted proposal
dataset_screenshot.png               dataset screenshot from the exploration assignment
```

Datasets and results are excluded from Git by `.gitignore` - together they are
about 5.6 GB. `data/` and `output/` keep `.gitkeep` files so the layout survives
a fresh clone.

## Requirements

- Python 3.11 or 3.12
- Java 17 (or 11)
- `pip install -r requirements.txt`

`numpy` is required because `pyspark.ml` imports it. On Python 3.12 `setuptools`
is also required, because PySpark still imports `distutils`, which was removed
from the standard library in 3.12.

In Codespaces the dev container installs all of this. See `.devcontainer/`.

## Data layout

Configure the two paths at the top of the script:

```python
REVIEWS_PATH  = "data/reviews/*.jsonl.gz"
METADATA_PATH = "data/meta/*.jsonl.gz"
```

Download the review and metadata files for the categories you want from
<https://amazon-reviews-2023.github.io/> and put them in those folders. Spark
reads `.jsonl.gz` directly, so there is no need to decompress them. The data is
excluded from Git by `.gitignore`.

## Running

```bash
./run_local.sh
```

or directly:

```bash
python AmazonReviewCoordination_single.py
```

Results are written under `output/`: enriched reviews as Parquet partitioned by
category and year, plus coordination groups, text-similarity pairs, suspicious
reviews and a runtime benchmark.

`OUTPUT_PATH` is written with mode `errorifexists`, so delete `output/` between
runs or point it somewhere new.

## Dataset notes

Reviews'23 differs from the older Amazon dumps, and the script handles each of
these. Anything editing the parsers should preserve them:

- the review body field is `text` (not `reviewText`)
- helpful votes are in `helpful_vote`, singular
- `timestamp` is in **milliseconds** and is divided by 1000
- reviews and product metadata join on `parent_asin`; the metadata has no
  `asin` field at all
- the metadata `details` field is free-form JSON whose keys sometimes differ
  only in case, so the job sets `spark.sql.caseSensitive = true`

## Memory

Large runs persist with `MEMORY_AND_DISK` rather than `cache()`, which is
memory-only and fails with `OutOfMemoryError` once the data no longer fits.
On a single machine, lower `MASTER` to something like `local[4]`: each Python
worker costs memory outside the JVM heap.

## Current limitations

- Connected components use custom label propagation, not GraphFrames.
- `local[*]` is a single machine, not a cluster. The multi-node experiment and
  the one-worker versus multi-worker comparison are still to be done, and the
  report should not describe a local run as a distributed experiment.
- `write_pair_audit` writes with ordinary Python file I/O, so it only works
  where the driver can reach the path. It needs a distributed writer before
  running against HDFS or S3.
