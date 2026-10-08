# ============================================================
# AmazonReviewCoordinationDF.py
# ============================================================
#
# DataFrame implementation of the coordination pipeline.
#
# The original row-based implementation converts every review into a Python
# object (reviews_df.rdd.map(parse_review)). That conversion is what fails
# with OutOfMemoryError on the full 24.4M-review dataset: the cost is paid in
# flight, in the JVM-to-Python writer, so neither disk spill nor a smaller
# cache helps.
#
# This version keeps the data in Spark SQL. Parsing and enrichment become
# column expressions, candidate pairs come from a self-join instead of
# itertools.combinations, and component labels propagate through joins.
#
# Group Members:
#   1. Asif Faisal Chowdhury
#   2. Souhardya Saha Dip
#   3. Riyadil Zannat
# ============================================================

import os
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType
)


# ============================================================
# Configuration
# ============================================================

REVIEWS_PATH = os.environ.get("REVIEWS_PATH", "data/reviews/*.jsonl.gz")
METADATA_PATH = os.environ.get("METADATA_PATH", "data/meta/*.jsonl.gz")
OUTPUT_ROOT = os.environ.get("OUTPUT_ROOT", "output/df")

MASTER = os.environ.get("SPARK_MASTER", "local[4]")
DRIVER_MEMORY = os.environ.get("DRIVER_MEMORY", "8g")
SHUFFLE_PARTITIONS = int(os.environ.get("SHUFFLE_PARTITIONS", "200"))

# GraphFrames is fetched at startup from the Spark Packages repo. Set
# USE_GRAPHFRAMES=0 to skip it (offline, or to compare against the fallback).
USE_GRAPHFRAMES = os.environ.get("USE_GRAPHFRAMES", "1") == "1"
GRAPHFRAMES_PACKAGE = os.environ.get(
    "GRAPHFRAMES_PACKAGE", "graphframes:graphframes:0.8.4-spark3.5-s_2.12")
CHECKPOINT_DIR = os.environ.get(
    "CHECKPOINT_DIR", "/tmp/amazon-review-coordination-checkpoint")

TIME_BUCKET_HOURS = 24
LARGE_GROUP_THRESHOLD = 100
LARGE_GROUP_BUCKET_HOURS = 6
MIN_REPEATED_GROUPS = 3
LABEL_PROPAGATION_ITERATIONS = 20

MIN_TEXT_LENGTH = 30
MIN_TOKEN_LENGTH = 2
TEXT_SIMILARITY_THRESHOLD = 0.70
MINHASH_NUM_HASH_TABLES = 5
MINHASH_SEED = int(os.environ.get("MINHASH_SEED", "42"))

SIZE_NORMALIZATION = 20.0
REPETITION_NORMALIZATION = 10.0
# A group whose ratings sit two stars away from the product's catalog average
# is already extreme, so that is where this signal saturates.
DEVIATION_NORMALIZATION = 2.0

# Seven signals, weights sum to 1.0. The proposal lists deviation from the
# catalog average rating as a ranking factor, so it carries its own weight
# rather than only being reported.
W_SIZE, W_DENSITY, W_REPETITION = 0.18, 0.18, 0.18
W_TIME, W_RATING, W_TEXT, W_DEVIATION = 0.13, 0.09, 0.14, 0.10

assert abs(W_SIZE + W_DENSITY + W_REPETITION
           + W_TIME + W_RATING + W_TEXT + W_DEVIATION - 1.0) < 1e-9

TOP_N = 20
MAX_REVIEWS_PER_COMPONENT = int(os.environ.get("MAX_REVIEWS_PER_COMPONENT", "500"))

REVIEW_SCHEMA = StructType([
    StructField("user_id", StringType()),
    StructField("asin", StringType()),
    StructField("parent_asin", StringType()),
    StructField("rating", DoubleType()),
    StructField("timestamp", LongType()),
    StructField("text", StringType()),
    StructField("helpful_vote", LongType()),
    StructField("verified_purchase", BooleanType()),
])

METADATA_SCHEMA = StructType([
    StructField("parent_asin", StringType()),
    StructField("main_category", StringType()),
    StructField("title", StringType()),
    StructField("average_rating", DoubleType()),
])


def banner(text):
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)


# ============================================================
# Stage 1 - load and enrich
# ============================================================

def load_enriched(spark):
    reviews = (
        spark.read.schema(REVIEW_SCHEMA).json(REVIEWS_PATH)
        .where(F.col("user_id").isNotNull()
               & F.col("parent_asin").isNotNull()
               & F.col("timestamp").isNotNull())
        # Reviews'23 stores the timestamp in milliseconds.
        .withColumn("ts", (F.col("timestamp") / F.lit(1000)).cast("long"))
        .withColumn("year", F.year(F.to_timestamp(F.col("ts"))))
        .withColumn("review_text", F.coalesce(F.col("text"), F.lit("")))
        .withColumn("text_eligible",
                    F.length(F.trim(F.col("review_text"))) >= F.lit(MIN_TEXT_LENGTH))
        .withColumn("helpful_votes", F.coalesce(F.col("helpful_vote"), F.lit(0)))
        .withColumn("verified", F.coalesce(F.col("verified_purchase"), F.lit(False)))
        .select("user_id", "parent_asin", "rating", "ts", "year",
                "review_text", "text_eligible", "helpful_votes", "verified")
    )

    metadata = (
        spark.read.schema(METADATA_SCHEMA).json(METADATA_PATH)
        .where(F.col("parent_asin").isNotNull())
        .select(
            F.col("parent_asin"),
            F.coalesce(F.col("main_category"), F.lit("Unknown")).alias("category"),
            F.coalesce(F.col("title"), F.lit("")).alias("product_title"),
            F.col("average_rating").alias("catalog_average_rating"),
        )
        .dropDuplicates(["parent_asin"])
    )

    enriched = (
        reviews.join(metadata, on="parent_asin", how="inner")
        .withColumn("rating_deviation",
                    F.abs(F.col("rating") - F.col("catalog_average_rating")))
    )
    return enriched


# ============================================================
# Stage 2 - candidate groups (product + time bucket)
# ============================================================

def add_group_key(enriched):
    """
    24-hour buckets, except that groups busier than LARGE_GROUP_THRESHOLD are
    re-bucketed at LARGE_GROUP_BUCKET_HOURS so one popular product-day does not
    dominate. The granularity is part of the key so the two never collide.
    """
    day_seconds = TIME_BUCKET_HOURS * 3600
    small_seconds = LARGE_GROUP_BUCKET_HOURS * 3600

    with_day = enriched.withColumn("day_bucket", (F.col("ts") / F.lit(day_seconds)).cast("long"))
    day_sizes = (with_day.groupBy("parent_asin", "day_bucket")
                 .agg(F.count("*").alias("day_count")))

    return (
        with_day.join(day_sizes, ["parent_asin", "day_bucket"])
        .withColumn("is_large", F.col("day_count") > F.lit(LARGE_GROUP_THRESHOLD))
        .withColumn("bucket",
                    F.when(F.col("is_large"), (F.col("ts") / F.lit(small_seconds)).cast("long"))
                     .otherwise(F.col("day_bucket")))
        .withColumn("group_key",
                    F.concat_ws("|", F.col("parent_asin"),
                                F.when(F.col("is_large"), F.lit("6h")).otherwise(F.lit("24h")),
                                F.col("bucket")))
        .drop("day_count", "is_large", "day_bucket", "bucket")
    )


# ============================================================
# Stage 3 - repeated account pairs (self-join, no Python)
# ============================================================

def repeated_pairs(grouped):
    members = grouped.select("group_key", "user_id").distinct()
    a = members.withColumnRenamed("user_id", "user1")
    b = members.withColumnRenamed("user_id", "user2")
    return (
        a.join(b, "group_key")
        .where(F.col("user1") < F.col("user2"))          # one direction only
        .groupBy("user1", "user2")
        .agg(F.countDistinct("group_key").alias("repeated_groups"))
        .where(F.col("repeated_groups") >= F.lit(MIN_REPEATED_GROUPS))
    )


# ============================================================
# Stage 4 - connected components by label propagation
# ============================================================

def connected_components(spark, edges):
    """
    Connected components of the co-review graph.

    Uses GraphFrames when it is available, which is what the project proposal
    specifies, and falls back to the DataFrame label propagation below so the
    pipeline still runs offline or if the package cannot be resolved.
    """
    if USE_GRAPHFRAMES:
        try:
            from graphframes import GraphFrame

            vertices = (
                edges.select(F.col("user1").alias("id"))
                .unionByName(edges.select(F.col("user2").alias("id")))
                .distinct()
            )
            graph_edges = edges.select(
                F.col("user1").alias("src"), F.col("user2").alias("dst"))

            # The default implementation runs hundreds of stages even on tiny
            # graphs; the graphx algorithm is much cheaper at this scale.
            result = GraphFrame(vertices, graph_edges).connectedComponents(
                algorithm="graphx")
            print("  components computed with GraphFrames")
            return result.select(
                F.col("id").alias("user_id"),
                F.col("component").alias("component_id"))

        except Exception as exc:
            print("  GraphFrames unavailable (%s)" % exc)
            print("  falling back to DataFrame label propagation")

    return label_propagation(spark, edges)


def label_propagation(spark, edges):
    """
    Iterative minimum-label propagation, entirely in DataFrames.
    Each vertex starts as its own label and repeatedly adopts the smallest
    label among itself and its neighbours.
    """
    undirected = (
        edges.select(F.col("user1").alias("src"), F.col("user2").alias("dst"))
        .unionByName(edges.select(F.col("user2").alias("src"), F.col("user1").alias("dst")))
        .distinct()
    )
    labels = (
        undirected.select(F.col("src").alias("id")).distinct()
        .withColumn("label", F.col("id"))
    )

    for _ in range(LABEL_PROPAGATION_ITERATIONS):
        proposed = (
            undirected.join(labels, undirected.src == labels.id)
            .select(F.col("dst").alias("id"), F.col("label"))
        )
        new_labels = (
            proposed.unionByName(labels)
            .groupBy("id").agg(F.min("label").alias("label"))
        ).localCheckpoint(eager=True)   # keep the lineage from growing each round

        if new_labels.join(labels, "id").where(
                new_labels.label != labels.label).limit(1).count() == 0:
            labels = new_labels
            break
        labels = new_labels

    return labels.withColumnRenamed("id", "user_id").withColumnRenamed("label", "component_id")


# ============================================================
# Stage 5 - per-component statistics and score
# ============================================================

def component_stats(enriched, components, edges):
    member_reviews = enriched.join(components, "user_id")

    per_component = (
        member_reviews
        .withColumn("hour_bucket", (F.col("ts") / F.lit(3600)).cast("long"))
        .groupBy("component_id")
        .agg(
            F.countDistinct("user_id").alias("users"),
            F.count("*").alias("reviews"),
            F.avg("rating").alias("mean_rating"),
            F.avg(F.coalesce(F.col("rating_deviation"), F.lit(0.0))).alias("avg_rating_deviation"),
            F.avg(F.col("verified").cast("double")).alias("verified_rate"),
            F.avg(F.col("helpful_votes").cast("double")).alias("avg_helpful_votes"),
            F.max("hour_bucket").alias("_mx"), F.min("hour_bucket").alias("_mn"),
        )
    )

    # busiest single hour / total reviews
    busiest = (
        member_reviews
        .withColumn("hour_bucket", (F.col("ts") / F.lit(3600)).cast("long"))
        .groupBy("component_id", "hour_bucket").agg(F.count("*").alias("c"))
        .groupBy("component_id").agg(F.max("c").alias("busiest_hour"))
    )

    # mean absolute deviation of ratings, as a 0-1 agreement score
    agreement = (
        member_reviews.join(per_component.select("component_id", "mean_rating"), "component_id")
        .groupBy("component_id")
        .agg(F.avg(F.abs(F.col("rating") - F.col("mean_rating"))).alias("mad"))
        .withColumn("rating_agreement",
                    F.greatest(F.lit(0.0), F.least(F.lit(1.0), F.lit(1.0) - F.col("mad") / F.lit(4.0))))
        .select("component_id", "rating_agreement")
    )

    edge_stats = (
        edges.join(components, edges.user1 == components.user_id)
        .groupBy("component_id")
        .agg(F.count("*").alias("edges"), F.avg("repeated_groups").alias("avg_repetition"))
    )

    return (
        per_component.drop("_mx", "_mn")
        .join(busiest, "component_id", "left")
        .join(agreement, "component_id", "left")
        .join(edge_stats, "component_id", "left")
        .withColumn("time_concentration", F.col("busiest_hour") / F.col("reviews"))
        .withColumn("density",
                    F.when(F.col("users") > 1,
                           F.col("edges") / (F.col("users") * (F.col("users") - 1) / 2.0))
                     .otherwise(F.lit(0.0)))
    )


def score(stats, text_similarity):
    clamp = lambda c: F.greatest(F.lit(0.0), F.least(F.lit(1.0), c))
    return (
        stats.join(text_similarity, "component_id", "left")
        .withColumn("avg_text_similarity", F.coalesce(F.col("avg_text_similarity"), F.lit(0.0)))
        .withColumn("similar_pairs", F.coalesce(F.col("similar_pairs"), F.lit(0)))
        .withColumn("size_signal", clamp(F.col("users") / F.lit(SIZE_NORMALIZATION)))
        .withColumn("repetition_signal", clamp(F.col("avg_repetition") / F.lit(REPETITION_NORMALIZATION)))
        .withColumn("deviation_signal",
                    clamp(F.coalesce(F.col("avg_rating_deviation"), F.lit(0.0))
                          / F.lit(DEVIATION_NORMALIZATION)))
        .withColumn("coordination_score",
                    F.lit(W_SIZE) * clamp(F.col("size_signal"))
                    + F.lit(W_DENSITY) * clamp(F.col("density"))
                    + F.lit(W_REPETITION) * clamp(F.col("repetition_signal"))
                    + F.lit(W_TIME) * clamp(F.coalesce(F.col("time_concentration"), F.lit(0.0)))
                    + F.lit(W_RATING) * clamp(F.coalesce(F.col("rating_agreement"), F.lit(0.0)))
                    + F.lit(W_TEXT) * clamp(F.col("avg_text_similarity"))
                    + F.lit(W_DEVIATION) * clamp(F.col("deviation_signal")))
    )


# ============================================================
# Stage 6 - MinHash text similarity inside components
# ============================================================

def text_similarity(spark, enriched, components):
    from pyspark.ml.feature import HashingTF, MinHashLSH

    docs = (
        enriched.join(components, "user_id")
        .where(F.col("text_eligible"))
        .withColumn("tokens",
                    F.array_distinct(
                        F.expr("filter(split(lower(review_text), '[^a-z0-9]+'), x -> length(x) >= %d)"
                               % MIN_TOKEN_LENGTH)))
        .where(F.size("tokens") > 0)
        .withColumn("doc_id", F.monotonically_increasing_id())
        .select("doc_id", "user_id", "component_id", "tokens")
    )

    empty_agg = spark.createDataFrame(
        [], "component_id string, avg_text_similarity double, similar_pairs long")
    empty_pairs = spark.createDataFrame(
        [], "component_id string, doc_a long, doc_b long, "
            "user_a string, user_b string, similarity double")

    if docs.limit(1).count() == 0:
        return empty_agg, empty_pairs

    # tokens are already non-empty, so HashingTF cannot produce an all-zero
    # vector here (MinHashLSH rejects those).
    featurised = HashingTF(
        inputCol="tokens", outputCol="features", numFeatures=1 << 18).transform(docs)

    # Seeded so the hash functions - and therefore the similarity scores and
    # the final ranking - are reproducible between runs.
    model = MinHashLSH(inputCol="features", outputCol="hashes",
                       numHashTables=MINHASH_NUM_HASH_TABLES,
                       seed=MINHASH_SEED).fit(featurised)

    max_distance = 1.0 - TEXT_SIMILARITY_THRESHOLD
    pairs = (
        model.approxSimilarityJoin(featurised, featurised, max_distance, distCol="jaccard_distance")
        .where(F.col("datasetA.doc_id") < F.col("datasetB.doc_id"))
        .where(F.col("datasetA.component_id") == F.col("datasetB.component_id"))
        # Two reviews by the same person are not evidence of coordination
        # between accounts.
        .where(F.col("datasetA.user_id") != F.col("datasetB.user_id"))
        .select(F.col("datasetA.component_id").alias("component_id"),
                F.col("datasetA.doc_id").alias("doc_a"),
                F.col("datasetB.doc_id").alias("doc_b"),
                F.col("datasetA.user_id").alias("user_a"),
                F.col("datasetB.user_id").alias("user_b"),
                (F.lit(1.0) - F.col("jaccard_distance")).alias("similarity"))
    ).cache()

    aggregate = pairs.groupBy("component_id").agg(
        F.avg("similarity").alias("avg_text_similarity"),
        F.count("*").alias("similar_pairs"))
    return aggregate, pairs


# ============================================================
# Stage 7 - audits and exports
# ============================================================

def pair_audit(edges):
    """
    Flag account pairs whose IDs differ only by a numeric suffix.

    Amazon Reviews'23 contains user IDs of the form ABC and ABC_1 (roughly 276
    per 2M reviews). Those are almost certainly one person, so a pair built
    from them is an artefact rather than coordination, and it otherwise ranks
    near the top because the two "accounts" co-review constantly.
    """
    strip_suffix = lambda c: F.regexp_replace(c, r"([_-][0-9]+|\([0-9]+\))+$", "")
    looks_suffixed = lambda c: c.rlike(r"([_-][0-9]+|\([0-9]+\))$")

    return (
        edges
        .withColumn("user1_base", strip_suffix(F.col("user1")))
        .withColumn("user2_base", strip_suffix(F.col("user2")))
        .withColumn("has_suffix_id",
                    looks_suffixed(F.col("user1")) | looks_suffixed(F.col("user2")))
        .withColumn("same_base_account", F.col("user1_base") == F.col("user2_base"))
        .where(F.col("has_suffix_id") | F.col("same_base_account"))
        .select("user1", "user2", "repeated_groups",
                "has_suffix_id", "same_base_account")
    )


def drop_same_account_pairs(edges):
    """
    Remove edges joining two IDs that differ only by a numeric suffix.

    Those are one person (ABC and ABC_1), so an edge between them is an
    artefact. They must go before connected components, not after: such pairs
    co-review constantly, score high on repetition, and would otherwise merge
    real accounts into inflated components and rank near the top.
    """
    strip_suffix = lambda c: F.regexp_replace(c, r"([_-][0-9]+|\([0-9]+\))+$", "")
    return edges.where(
        strip_suffix(F.col("user1")) != strip_suffix(F.col("user2")))


def suspicious_reviews(enriched, components):
    """
    The reviews written by members of each candidate group, capped per
    component so one large component cannot dominate the export.
    """
    from pyspark.sql.window import Window

    ordered = Window.partitionBy("component_id").orderBy(F.col("ts").asc())
    return (
        enriched.join(components, "user_id")
        .withColumn("rn", F.row_number().over(ordered))
        .where(F.col("rn") <= F.lit(MAX_REVIEWS_PER_COMPONENT))
        .drop("rn")
        .select("component_id", "user_id", "parent_asin", "category",
                "rating", "catalog_average_rating", "rating_deviation",
                "ts", "year", "verified", "helpful_votes",
                "text_eligible", "review_text")
    )


# ============================================================
# Main
# ============================================================

def main():
    started = time.time()
    builder = (
        SparkSession.builder
        .appName("Amazon Review Coordination (DataFrame)")
        .master(MASTER)
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.caseSensitive", "true")
        .config("spark.hadoop.fs.defaultFS", "file:///")
    )
    if USE_GRAPHFRAMES:
        builder = (builder
                   .config("spark.jars.packages", GRAPHFRAMES_PACKAGE)
                   .config("spark.jars.repositories",
                           "https://repos.spark-packages.org"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)
    timings = {}

    def step(name, fn):
        t = time.time()
        result = fn()
        timings[name] = round(time.time() - t, 1)
        print("  %-28s %8.1fs" % (name, timings[name]))
        return result

    banner("Amazon Review Coordination - DataFrame pipeline")
    print("Reviews : %s" % REVIEWS_PATH)
    print("Metadata: %s" % METADATA_PATH)
    print("Output  : %s" % OUTPUT_ROOT)
    print("Master  : %s   driver memory: %s" % (MASTER, DRIVER_MEMORY))

    banner("Stage timings")
    enriched = load_enriched(spark)
    n_reviews = step("enrich + join", lambda: enriched.count())
    print("Enriched reviews: %d" % n_reviews)

    step("write enriched parquet",
         lambda: enriched.write.mode("overwrite").partitionBy("category", "year")
                 .parquet(OUTPUT_ROOT + "/amazon_output"))

    grouped = add_group_key(enriched).select("group_key", "user_id").distinct().cache()
    n_groups = step("candidate groups", lambda: grouped.select("group_key").distinct().count())
    print("Candidate groups: %d" % n_groups)

    edges_all = repeated_pairs(grouped).cache()
    n_edges_all = step("repeated pairs (self-join)", lambda: edges_all.count())
    print("Repeated user pairs: %d" % n_edges_all)

    audit = pair_audit(edges_all).cache()
    n_flagged = step("pair audit", lambda: audit.count())
    edges = drop_same_account_pairs(edges_all).cache()
    n_edges = edges.count()
    print("Flagged as possible same-account artefacts: %d" % n_flagged)
    print("Dropped before graph construction: %d; pairs remaining: %d"
          % (n_edges_all - n_edges, n_edges))
    if n_flagged:
        audit.select("user1", "user2", "repeated_groups", "same_base_account") \
             .orderBy(F.col("repeated_groups").desc()).show(10, truncate=False)

    if n_edges == 0:
        print("\nNo usable account pairs after filtering; nothing to rank.")
        spark.stop()
        return

    components = connected_components(spark, edges).cache()
    n_vertices = step("connected components", lambda: components.count())
    n_components = components.select("component_id").distinct().count()
    print("Graph vertices: %d   components: %d" % (n_vertices, n_components))

    sim, sim_pairs = step("MinHash text similarity",
                          lambda: text_similarity(spark, enriched, components))
    sim = sim.cache()

    stats = component_stats(enriched, components, edges)
    ranked = score(stats, sim).orderBy(F.col("coordination_score").desc()).cache()
    step("component stats + score", lambda: ranked.count())

    step("write results", lambda: (
        ranked.write.mode("overwrite").parquet(OUTPUT_ROOT + "/coordination_groups"),
        edges.write.mode("overwrite").parquet(OUTPUT_ROOT + "/repeated_pairs"),
        components.write.mode("overwrite").parquet(OUTPUT_ROOT + "/components"),
        sim_pairs.write.mode("overwrite").parquet(OUTPUT_ROOT + "/text_similarity_pairs"),
    ))

    audit.coalesce(1).write.mode("overwrite").option("header", True) \
         .csv(OUTPUT_ROOT + "/repeated_pair_audit")

    step("write suspicious reviews", lambda:
         suspicious_reviews(enriched, components)
         .write.mode("overwrite").parquet(OUTPUT_ROOT + "/suspicious_reviews"))

    banner("Top %d coordination groups" % TOP_N)
    (ranked.select("component_id", "users", "edges", "reviews", "density",
                   "avg_repetition", "time_concentration", "rating_agreement",
                   "avg_text_similarity", "similar_pairs", "coordination_score")
     .show(TOP_N, truncate=False))

    total = round(time.time() - started, 1)
    spark.createDataFrame(
        [(k, float(v)) for k, v in timings.items()] + [("total", float(total))],
        "stage string, seconds double"
    ).write.mode("overwrite").parquet(OUTPUT_ROOT + "/benchmark")

    banner("PIPELINE COMPLETED SUCCESSFULLY")
    print("Reviews processed : %d" % n_reviews)
    print("Candidate groups  : %d" % n_groups)
    print("Repeated pairs    : %d" % n_edges)
    print("Graph vertices    : %d" % n_vertices)
    print("Components        : %d" % n_components)
    print("Flagged pairs     : %d (dropped before the graph)" % (n_edges_all - n_edges))
    print("Total runtime     : %.1f seconds" % total)
    spark.stop()


if __name__ == "__main__":
    main()
