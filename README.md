# Amazon Review Coordination

CSC 7740 final project. A PySpark pipeline that looks for groups of Amazon
accounts which repeatedly review the same products within the same short time
windows, across the Amazon Reviews'23 dataset.

Group members: Asif Faisal Chowdhury, Souhardya Saha Dip, Riyadil Zannat.

## Source of truth

`AmazonReviewCoordinationDF.py` is the only implementation.

It keeps the work inside Spark SQL: parsing and enrichment are column
expressions, candidate pairs come from a self-join, and connected components
use GraphFrames. Two earlier versions have been removed:

- the split version (`AmazonReviewPipeline.py` + `Config.py` + `Utils.py`),
  which still expected the older Amazon field names and silently produced no
  results on Reviews'23
- the single-file row-based version, which converted every review into a
  Python object (`reviews_df.rdd.map(parse_review)`). That conversion ran out
  of heap on the full 24.4M-review dataset, because the cost is paid in flight
  in the JVM-to-Python writer, where neither disk spill nor a smaller cache
  helps.

The current version was validated against the row-based one on the Video Games
category and reproduces every figure exactly - 4,624,615 enriched reviews,
3,428,395 candidate groups, 232 repeated pairs, 349 graph vertices and 8
flagged same-account pairs - in 607s against 1003s.

## Repository layout

```text
AmazonReviewCoordinationDF.py        the pipeline (single source of truth)
run_local.sh                         one run over everything in data/
run_all_categories.sh                one run per category, sequentially
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
./run_local.sh                 # one run over everything in data/
./run_all_categories.sh        # one run per category, sequentially
```

or directly:

```bash
python AmazonReviewCoordinationDF.py
```

Paths and Spark settings can be overridden per run:

```bash
REVIEWS_PATH=data/reviews/Video_Games.jsonl.gz \
METADATA_PATH=data/meta/meta_Video_Games.jsonl.gz \
OUTPUT_ROOT=output/video_games \
SPARK_MASTER=local[4] DRIVER_MEMORY=8g ./run_local.sh
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

## GraphFrames

Connected components use GraphFrames, fetched at startup:

```text
spark.jars.packages     graphframes:graphframes:0.8.4-spark3.5-s_2.12
spark.jars.repositories https://repos.spark-packages.org
```

It runs with `algorithm="graphx"`; the default implementation launches
hundreds of stages even on a graph of a few hundred vertices. Set
`USE_GRAPHFRAMES=0` to fall back to the built-in DataFrame label propagation,
which is useful offline or to check the two agree.

## Current limitations
- `local[*]` is a single machine, not a cluster. The multi-node experiment and
  the one-worker versus multi-worker comparison are still to be done, and the
  report should not describe a local run as a distributed experiment.
- The full 24.4M-review run has not completed on a single machine yet. Use
  `run_all_categories.sh` if a single pass runs out of memory; note that
  account pairs whose shared groups straddle two categories are then missed.
