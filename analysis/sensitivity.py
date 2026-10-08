# Sensitivity of the coordination signal to the two parameters that drive it:
# the time-window size and the minimum number of shared product-time groups.
#
# Mirrors the pipeline's grouping logic (including the re-bucketing of groups
# busier than LARGE_GROUP_THRESHOLD) and its same-account filter, but stops
# after the graph is formed - scoring and text similarity are not involved.
#
#   REVIEWS_PATH=data/reviews/Video_Games.jsonl.gz \
#   METADATA_PATH=data/meta/meta_Video_Games.jsonl.gz \
#   python analysis/sensitivity.py

import os, sys, time, csv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

REVIEWS_PATH = os.environ.get("REVIEWS_PATH", "data/reviews/Video_Games.jsonl.gz")
METADATA_PATH = os.environ.get("METADATA_PATH", "data/meta/meta_Video_Games.jsonl.gz")
OUT_CSV = os.environ.get("OUT_CSV", "output/sensitivity.csv")

WINDOWS_HOURS = [int(x) for x in os.environ.get("WINDOWS", "6,12,24,72,168").split(",")]
THRESHOLDS = [int(x) for x in os.environ.get("THRESHOLDS", "2,3,5,10").split(",")]
LARGE_GROUP_THRESHOLD = 100
LARGE_GROUP_BUCKET_HOURS = 6

REVIEW_SCHEMA = StructType([
    StructField("user_id", StringType()), StructField("parent_asin", StringType()),
    StructField("timestamp", LongType()),
])


def components_from_edges(pairs):
    """Union-find on the driver. Edge sets here are thousands of rows at most."""
    parent = {}
    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb
    for u1, u2 in pairs:
        union(u1, u2)
    sizes = {}
    for node in list(parent):
        root = find(node)
        sizes[root] = sizes.get(root, 0) + 1
    return len(sizes), (max(sizes.values()) if sizes else 0)


def main():
    spark = (SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[4]"))
             .appName("sensitivity")
             .config("spark.driver.memory", os.environ.get("DRIVER_MEMORY", "8g"))
             .config("spark.sql.shuffle.partitions", "200")
             .config("spark.sql.caseSensitive", "true")
             .config("spark.hadoop.fs.defaultFS", "file:///")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    reviews = (spark.read.schema(REVIEW_SCHEMA).json(REVIEWS_PATH)
               .where(F.col("user_id").isNotNull() & F.col("parent_asin").isNotNull()
                      & F.col("timestamp").isNotNull())
               .withColumn("ts", (F.col("timestamp") / F.lit(1000)).cast("long"))
               .select("user_id", "parent_asin", "ts")
               .cache())
    total = reviews.count()
    print("reviews: %d\n" % total)

    strip = lambda c: F.regexp_replace(c, r"([_-][0-9]+|\([0-9]+\))+$", "")
    rows = []
    header = "%-9s %9s %9s %8s %8s %8s %8s %9s" % (
        "window", "groups", "candidates", "thresh", "pairs", "users", "comps", "largest")
    print(header); print("-" * len(header))

    for hours in WINDOWS_HOURS:
        t0 = time.time()
        base, small = hours * 3600, LARGE_GROUP_BUCKET_HOURS * 3600

        with_base = reviews.withColumn("base_bucket", (F.col("ts") / F.lit(base)).cast("long"))
        sizes = with_base.groupBy("parent_asin", "base_bucket").agg(F.count("*").alias("n"))
        grouped = (with_base.join(sizes, ["parent_asin", "base_bucket"])
                   .withColumn("is_large", F.col("n") > F.lit(LARGE_GROUP_THRESHOLD))
                   .withColumn("bucket", F.when(F.col("is_large"),
                                                (F.col("ts") / F.lit(small)).cast("long"))
                                          .otherwise(F.col("base_bucket")))
                   .withColumn("group_key", F.concat_ws("|", F.col("parent_asin"),
                                                        F.when(F.col("is_large"), F.lit("S")).otherwise(F.lit("B")),
                                                        F.col("bucket")))
                   .select("group_key", "user_id").distinct())

        n_groups = grouped.select("group_key").distinct().count()

        a = grouped.withColumnRenamed("user_id", "user1")
        b = grouped.withColumnRenamed("user_id", "user2")
        pair_counts = (a.join(b, "group_key").where(F.col("user1") < F.col("user2"))
                       .groupBy("user1", "user2")
                       .agg(F.countDistinct("group_key").alias("repeated_groups"))
                       # same-account artefacts removed, as the pipeline does
                       .where(strip(F.col("user1")) != strip(F.col("user2")))
                       .cache())
        # All unique account pairs that share at least one product-time group.
        # This count is independent of the tested repetition threshold.
        candidate_pairs = pair_counts.count()

        for t in THRESHOLDS:
            kept = pair_counts.where(F.col("repeated_groups") >= F.lit(t))
            collected = [(r["user1"], r["user2"]) for r in kept.collect()]
            n_pairs = len(collected)
            users = len({u for p in collected for u in p})
            comps, largest = components_from_edges(collected)
            print("%-9s %9d %9d %8d %8d %8d %8d %9d"
                  % ("%dh" % hours, n_groups, candidate_pairs, t, n_pairs, users, comps, largest))
            rows.append(dict(window_hours=hours, groups=n_groups,
                             candidate_pairs=candidate_pairs,
                             threshold=t, pairs=n_pairs, users=users,
                             components=comps, largest_component=largest))
        pair_counts.unpersist()
        print("  (%dh window took %.0fs)\n" % (hours, time.time() - t0))

    os.makedirs(os.path.dirname(OUT_CSV) or ".", exist_ok=True)
    with open(OUT_CSV, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
    print("wrote %s (%d rows)" % (OUT_CSV, len(rows)))
    spark.stop()


if __name__ == "__main__":
    main()
