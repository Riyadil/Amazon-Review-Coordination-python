# Synthetic injection test.
#
# Plants coordinated groups with known membership into the real reviews, runs
# the pipeline's detection logic over the combined data, and reports how much
# of each planted ring is recovered.
#
# The rings deliberately span the detector's edge: some share fewer products
# than the minimum-repeats threshold (should NOT be found), some are spread
# across days (should degrade), some have partial participation.
#
#   REVIEWS_PATH=data/reviews/Video_Games.jsonl.gz python analysis/injection.py

import os, random, time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

REVIEWS_PATH = os.environ.get("REVIEWS_PATH", "data/reviews/Video_Games.jsonl.gz")
OUT_CSV = os.environ.get("OUT_CSV", "output/injection_results.csv")
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "24"))
MIN_REPEATED = int(os.environ.get("MIN_REPEATED", "3"))
LARGE_GROUP_THRESHOLD, LARGE_GROUP_BUCKET_HOURS = 100, 6
BASE_TS = 1579000000          # mid-January 2020, inside the data range
random.seed(20260101)

# (name, accounts, products, hours spread over, participation rate, expectation)
RINGS = [
    ("R1_tight_3acct_5prod",   3,  5,   0, 1.0,  "detect"),
    ("R2_tight_5acct_3prod",   5,  3,   0, 1.0,  "detect"),
    ("R3_large_10acct_3prod", 10,  3,   0, 1.0,  "detect"),
    ("R4_below_thresh_2prod",  5,  2,   0, 1.0,  "miss (2 < %d)" % MIN_REPEATED),
    ("R5_spread_5acct_5prod",  5,  5,  96, 1.0,  "degrade"),
    ("R6_partial_8acct_6prod", 8,  6,   0, 0.6,  "partial"),
]

SCHEMA = StructType([StructField("user_id", StringType()),
                     StructField("parent_asin", StringType()),
                     StructField("timestamp", LongType())])


def make_rings():
    rows, truth = [], {}
    for name, n_acct, n_prod, spread_h, participation, _ in RINGS:
        accounts = ["SYN_%s_U%02d" % (name[:2], i) for i in range(n_acct)]
        products = ["SYN_%s_P%02d" % (name[:2], j) for j in range(n_prod)]
        truth[name] = set(accounts)
        for j, product in enumerate(products):
            # each product reviewed inside one hour, products spread if asked
            offset = 0 if spread_h == 0 else int(j * spread_h * 3600 / max(1, n_prod - 1))
            for account in accounts:
                if participation < 1.0 and random.random() > participation:
                    continue
                jitter = random.randint(0, 1800)
                rows.append((account, product, (BASE_TS + offset + jitter) * 1000))
    return rows, truth


def main():
    spark = (SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[4]"))
             .appName("injection-test")
             .config("spark.driver.memory", os.environ.get("DRIVER_MEMORY", "8g"))
             .config("spark.sql.shuffle.partitions", "200")
             .config("spark.sql.caseSensitive", "true").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    real = (spark.read.schema(SCHEMA).json(REVIEWS_PATH)
            .where(F.col("user_id").isNotNull() & F.col("parent_asin").isNotNull()
                   & F.col("timestamp").isNotNull()))
    planted_rows, truth = make_rings()
    planted = spark.createDataFrame(planted_rows, SCHEMA)
    print("real reviews: %d   planted reviews: %d   rings: %d"
          % (real.count(), len(planted_rows), len(RINGS)))

    combined = (real.unionByName(planted)
                .withColumn("ts", (F.col("timestamp") / F.lit(1000)).cast("long"))
                .select("user_id", "parent_asin", "ts"))

    base, small = WINDOW_HOURS * 3600, LARGE_GROUP_BUCKET_HOURS * 3600
    with_base = combined.withColumn("base_bucket", (F.col("ts") / F.lit(base)).cast("long"))
    sizes = with_base.groupBy("parent_asin", "base_bucket").agg(F.count("*").alias("n"))
    grouped = (with_base.join(sizes, ["parent_asin", "base_bucket"])
               .withColumn("is_large", F.col("n") > F.lit(LARGE_GROUP_THRESHOLD))
               .withColumn("bucket", F.when(F.col("is_large"), (F.col("ts") / F.lit(small)).cast("long"))
                                      .otherwise(F.col("base_bucket")))
               .withColumn("group_key", F.concat_ws("|", F.col("parent_asin"),
                                                    F.when(F.col("is_large"), F.lit("S")).otherwise(F.lit("B")),
                                                    F.col("bucket")))
               .select("group_key", "user_id").distinct())

    strip = lambda c: F.regexp_replace(c, r"([_-][0-9]+|\([0-9]+\))+$", "")
    a = grouped.withColumnRenamed("user_id", "user1")
    b = grouped.withColumnRenamed("user_id", "user2")
    edges = (a.join(b, "group_key").where(F.col("user1") < F.col("user2"))
             .groupBy("user1", "user2").agg(F.countDistinct("group_key").alias("shared"))
             .where(F.col("shared") >= F.lit(MIN_REPEATED))
             .where(strip(F.col("user1")) != strip(F.col("user2"))))
    pairs = [(r["user1"], r["user2"]) for r in edges.collect()]
    print("detected pairs (window %dh, threshold %d): %d" % (WINDOW_HOURS, MIN_REPEATED, len(pairs)))

    parent = {}
    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]; x = parent[x]
        return x
    for u1, u2 in pairs:
        r1, r2 = find(u1), find(u2)
        if r1 != r2:
            parent[r1] = r2
    members = {}
    for node in list(parent):
        members.setdefault(find(node), set()).add(node)

    print()
    header = "%-26s %6s %9s %9s %9s  %s" % ("ring", "acct", "found", "recall", "foreign", "expected")
    print(header); print("-" * len(header))
    rows = []
    for name, n_acct, n_prod, spread_h, participation, expectation in RINGS:
        planted_accounts = truth[name]
        best, foreign = set(), 0
        for comp in members.values():
            overlap = comp & planted_accounts
            if len(overlap) > len(best):
                best, foreign = overlap, len(comp - planted_accounts)
        recall = len(best) / float(n_acct)
        print("%-26s %6d %9d %8.0f%% %9d  %s"
              % (name, n_acct, len(best), recall * 100, foreign, expectation))
        rows.append((name, n_acct, n_prod, spread_h, participation,
                     len(best), round(recall, 3), foreign, expectation))

    os.makedirs(os.path.dirname(OUT_CSV) or ".", exist_ok=True)
    with open(OUT_CSV, "w") as fh:
        fh.write("ring,accounts,products,spread_hours,participation,found,recall,foreign_accounts,expected\n")
        for r in rows:
            fh.write(",".join(str(x) for x in r) + "\n")
    print("\nwrote %s" % OUT_CSV)
    spark.stop()


if __name__ == "__main__":
    main()
