# ============================================================
# AmazonReviewPipeline.py
# ============================================================
#
#
# Group Members:
#   1. Asif Faisal Chowdhury
#   2. Souhardya Saha Dip
#   3. Riyadil Zannat
#
# This program:
#   1. Loads Amazon reviews and metadata
#   2. Cleans and enriches review data
#   3. Groups reviews into time buckets
#   4. Identifies repeated user pairs
#   5. Builds a user coordination graph
#   6. Finds connected components using label propagation
#   7. Calculates coordination scores
#   8. Saves enriched data as Parquet by category and year
#   9. Attaches component IDs to reviews
#  10. Calculates detailed component statistics
#  11. Runs distributed MinHash text similarity analysis
#  12. Aggregates text similarity by component
#  13. Calculates extended coordination scores
#  14. Ranks final coordination groups
#  15. Saves component and suspicious-review results
#  16. Generates a runtime/performance summary
#
# ============================================================


# ============================================================
# Configuration
# ============================================================

# ============================================================
# Config.py
# ============================================================
# Configuration for Amazon Review Coordination Analysis
# ============================================================


# ============================================================
# File Paths
# ============================================================

REVIEWS_PATH = "data/All_Beauty.jsonl"

METADATA_PATH = "data/meta_All_Beauty.jsonl"

# Use a new directory for each run.
OUTPUT_PATH = "output/amazon_output"

# Local CSV for manual review of repeated user pairs.
PAIR_AUDIT_PATH = "output/repeated_pair_audit.csv"


# ============================================================
# Spark Configuration
# ============================================================

APP_NAME = "Amazon Review Coordination Analysis"

MASTER = "local[*]"

DRIVER_MEMORY = "4g"

# Number of Spark partitions for large operations.
DEFAULT_PARTITIONS = 200

# Set to True when running on a multi-node Spark cluster.
CLOUD_MODE = False


# ============================================================
# Time-Bucket Configuration
# ============================================================

# Initial time bucket size.
TIME_BUCKET_HOURS = 24

# If a time bucket contains more than this number
# of reviews, it will be split into smaller buckets.
LARGE_GROUP_THRESHOLD = 100

# Time bucket size used for oversized groups.
LARGE_GROUP_BUCKET_HOURS = 6


# ============================================================
# Graph Configuration
# ============================================================

# Minimum number of distinct product-time groups
# in which a user pair must appear.
MIN_REPEATED_GROUPS = 3

# Number of iterations for connected-component
# label propagation.
LABEL_PROPAGATION_ITERATIONS = 20


# ============================================================
# Result Configuration
# ============================================================

# Number of top coordination groups to display.
TOP_N = 20

# Maximum number of reviews sampled/displayed
# for detailed analysis of one coordination group.
MAX_REVIEWS_PER_GROUP = 500


# ============================================================
# Original Coordination Score
# ============================================================

SIZE_WEIGHT = 0.35

DENSITY_WEIGHT = 0.35

REPETITION_WEIGHT = 0.30

SIZE_NORMALIZATION = 20.0

REPETITION_NORMALIZATION = 10.0


# ============================================================
# Text Analysis
# ============================================================

# Reviews shorter than this are excluded from
# text-similarity analysis but remain in the
# co-review graph.
MIN_TEXT_LENGTH = 30

# Minimum token length for text processing.
MIN_TOKEN_LENGTH = 2

# Maximum number of tokens used for a review.
# 0 means use all tokens.
MAX_TEXT_TOKENS = 0


# ============================================================
# Text Similarity Configuration
# ============================================================

# Minimum Jaccard similarity required for a
# text-similarity match.
TEXT_SIMILARITY_THRESHOLD = 0.70

# Number of hash tables used by MinHash LSH.
MINHASH_NUM_HASH_TABLES = 5

# Minimum number of text-similar review pairs
# required before increasing a group's score.
MIN_TEXT_SIMILAR_REVIEWS = 1

# Maximum number of text-similarity pairs retained
# per coordination component.
MAX_TEXT_PAIRS_PER_COMPONENT = 10000


# ============================================================
# Additional Coordination Signals
# ============================================================

# Weight for review-text similarity.
TEXT_SIMILARITY_WEIGHT = 0.20

# Weight for concentration of reviews in time.
TIME_CONCENTRATION_WEIGHT = 0.15

# Weight for agreement between reviewers' ratings.
RATING_AGREEMENT_WEIGHT = 0.10

# Weight for verified-purchase behavior.
VERIFIED_PURCHASE_WEIGHT = 0.05

# Weight for helpful-vote behavior.
HELPFUL_VOTE_WEIGHT = 0.05


# ============================================================
# Extended Coordination Score
# ============================================================

# Weights for the six-signal coordination score.
#
# These should normally sum to 1.0:
#
#   Size
#   Density
#   Repetition
#   Time concentration
#   Rating agreement
#   Text similarity
#
# The current values match the implementation
# in Utils.py.

COMBINED_SIZE_WEIGHT = 0.20

COMBINED_DENSITY_WEIGHT = 0.20

COMBINED_REPETITION_WEIGHT = 0.20

COMBINED_TIME_WEIGHT = 0.15

COMBINED_RATING_WEIGHT = 0.10

COMBINED_TEXT_WEIGHT = 0.15


# ============================================================
# Component Review Analysis
# ============================================================

# Calculate descriptive statistics for reviews
# belonging to each coordination component.
ENABLE_COMPONENT_REVIEW_ANALYSIS = True

# Calculate time concentration.
ENABLE_TIME_CONCENTRATION = True

# Calculate rating agreement.
ENABLE_RATING_AGREEMENT = True

# Calculate verified purchase rate.
ENABLE_VERIFIED_PURCHASE_ANALYSIS = True

# Calculate average helpful votes.
ENABLE_HELPFUL_VOTE_ANALYSIS = True


# ============================================================
# Text Similarity Processing
# ============================================================

# Enable distributed Spark MinHashLSH text analysis.
ENABLE_TEXT_SIMILARITY = True

# Only reviews with text_eligible=True participate
# in text similarity processing.
USE_TEXT_ELIGIBLE_ONLY = True

# Generate text similarity candidates only inside
# coordination components.
TEXT_SIMILARITY_WITHIN_COMPONENTS_ONLY = True


# ============================================================
# Component Sampling
# ============================================================

# Prevent very large components from producing
# excessive text-analysis work.
MAX_REVIEWS_PER_COMPONENT = 500

# Maximum number of users retained when creating
# detailed component-level analysis.
MAX_USERS_PER_COMPONENT = 1000


# ============================================================
# Output Paths
# ============================================================

# Enriched review data.
TEXT_SIMILARITY_OUTPUT_PATH = (
    "output/text_similarity"
)

# Coordination-group summaries.
GROUP_OUTPUT_PATH = (
    "output/coordination_groups"
)

# Runtime benchmark results.
BENCHMARK_OUTPUT_PATH = (
    "output/benchmark"
)

# Detailed component-level review statistics.
COMPONENT_REVIEW_OUTPUT_PATH = (
    "output/component_review_analysis"
)

# Text-similarity pair output.
TEXT_PAIR_OUTPUT_PATH = (
    "output/text_similarity_pairs"
)

# Final coordination-group results.
FINAL_GROUP_OUTPUT_PATH = (
    "output/final_coordination_groups"
)

# Names used by the main pipeline. Keep these aliases in one place so
# every output written by the pipeline has an explicit configuration.
COMPONENT_OUTPUT_PATH = GROUP_OUTPUT_PATH
TEXT_OUTPUT_PATH = TEXT_SIMILARITY_OUTPUT_PATH
SUSPICIOUS_REVIEW_OUTPUT_PATH = (
    "output/suspicious_reviews"
)
PERFORMANCE_OUTPUT_PATH = BENCHMARK_OUTPUT_PATH

# Spark ML settings. HashingTF creates a binary feature vector for
# MinHashLSH; 2^18 features is a conservative default for this dataset.
TEXT_NUM_FEATURES = 1 << 18

# approxSimilarityJoin expects Jaccard DISTANCE. Convert the desired
# similarity threshold to the corresponding distance threshold.
TEXT_JOIN_THRESHOLD = 1.0 - TEXT_SIMILARITY_THRESHOLD

# Keep the implementation and configuration synchronized.
MAX_TEXT_REVIEWS_PER_COMPONENT = MAX_REVIEWS_PER_COMPONENT

# Use overwrite during development so a second validation run does not
# fail merely because an output directory already exists.
OUTPUT_WRITE_MODE = "overwrite"

# Configuration sanity checks. Fail early instead of after expensive Spark jobs.
if not 0.0 <= TEXT_SIMILARITY_THRESHOLD <= 1.0:
    raise ValueError("TEXT_SIMILARITY_THRESHOLD must be between 0 and 1")

if MINHASH_NUM_HASH_TABLES < 1:
    raise ValueError("MINHASH_NUM_HASH_TABLES must be at least 1")

if TEXT_NUM_FEATURES < 2:
    raise ValueError("TEXT_NUM_FEATURES must be at least 2")


# ============================================================
# Output Format Configuration
# ============================================================

# Save Spark output as Parquet.
WRITE_PARQUET_OUTPUT = True

# Save coordination-group summaries as CSV as well.
WRITE_CSV_OUTPUT = False

# Save text-similarity pairs.
WRITE_TEXT_SIMILARITY_OUTPUT = True


# ============================================================
# Runtime Benchmarking
# ============================================================

ENABLE_RUNTIME_BENCHMARK = True

# Record runtime for individual pipeline stages.
BENCHMARK_PIPELINE_STAGES = True


# ============================================================
# Caching / Performance
# ============================================================

# Cache intermediate RDDs/DataFrames when useful.
ENABLE_CACHING = True

# Persist enriched reviews because they are reused
# by multiple later analysis stages.
CACHE_ENRICHED_REVIEWS = True

# Persist candidate groups because they are reused
# by graph construction and additional analysis.
CACHE_CANDIDATE_GROUPS = True

# Persist graph components for later analysis.
CACHE_COMPONENT_LABELS = True


# ============================================================
# Safety / Validation
# ============================================================

# Validate coordination scores before writing output.
VALIDATE_COORDINATION_SCORES = True

# Reject invalid similarity values.
VALIDATE_TEXT_SIMILARITY = True

# Keep the reviewer-ID audit enabled.
ENABLE_PAIR_AUDIT = True




# ============================================================
# Imports and Reusable Utility Functions
# ============================================================

# ============================================================
# Utils.py
# ============================================================
#
# Reusable parsing, enrichment, graph, scoring, text-analysis,
# audit, and output helper functions.
#
# ============================================================

import itertools
import re

from pyspark.sql import Row


# ============================================================
# Generic Conversion Helpers
# ============================================================

def safe_float(value, default=None):
    """
    Safely convert a value to float.
    """

    if value is None:
        return default

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value, default=None):
    """
    Safely convert a value to int.
    """

    if value is None:
        return default

    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_bool(value, default=False):
    """
    Convert common representations of boolean values.
    """

    if value is None:
        return default

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    value = str(value).strip().lower()

    if value in (
        "true",
        "1",
        "yes",
        "y"
    ):
        return True

    if value in (
        "false",
        "0",
        "no",
        "n"
    ):
        return False

    return default


# ============================================================
# Review Parsing
# ============================================================

def parse_review(record):
    """
    Parse one raw Amazon review record.

    This function intentionally supports several common field
    names used by Amazon review datasets.
    """

    try:

        user_id = (
            record.get("user_id")
            or record.get("reviewerID")
        )

        # Amazon Reviews'23 uses parent_asin as the product key that
        # matches the product metadata parent_asin field.
        product_id = (
            record.get("parent_asin")
            or record.get("product_id")
            or record.get("asin")
        )

        rating = (
            record.get("rating")
            if record.get("rating") is not None
            else record.get("overall")
        )

        timestamp = (
            record.get("timestamp")
        )

        if timestamp is None:

            timestamp = (
                record.get("unixReviewTime")
            )

        review_text = (
            record.get("review_text")
        )

        if review_text is None:

            review_text = (
                record.get("text")
            )

        if review_text is None:

            review_text = (
                record.get("reviewText")
            )

        verified_purchase = (
            record.get("verified_purchase")
        )

        if verified_purchase is None:

            verified_purchase = (
                record.get("verified")
            )

        helpful_votes = (
            record.get("helpful_votes")
        )

        if helpful_votes is None:

            helpful_votes = (
                record.get("helpful_vote")
            )

        if helpful_votes is None:

            helpful_votes = (
                record.get("helpful")
            )

        # Amazon datasets sometimes represent helpful votes
        # as [helpful_votes, total_votes].
        if isinstance(helpful_votes, list):

            if helpful_votes:

                helpful_votes = (
                    safe_int(
                        helpful_votes[0],
                        0
                    )
                )

            else:

                helpful_votes = 0

        user_id = (
            str(user_id).strip()
            if user_id is not None
            else None
        )

        product_id = (
            str(product_id).strip()
            if product_id is not None
            else None
        )

        review_text = (
            str(review_text).strip()
            if review_text is not None
            else ""
        )

        timestamp = safe_int(
            timestamp
        )

        # Amazon Reviews'23 stores timestamp in milliseconds.
        # Normalize to Unix seconds for bucketing and year extraction.
        if timestamp is not None and timestamp > 10_000_000_000:
            timestamp = timestamp // 1000

        rating = safe_float(
            rating
        )

        verified_purchase = safe_bool(
            verified_purchase
        )

        helpful_votes = safe_int(
            helpful_votes,
            0
        )

        if not user_id:
            return None

        if not product_id:
            return None

        if timestamp is None:
            return None

        return Row(
            user_id=user_id,
            product_id=product_id,
            rating=rating,
            timestamp=timestamp,
            review_text=review_text,
            verified_purchase=verified_purchase,
            helpful_votes=helpful_votes
        )

    except Exception:

        return None


# ============================================================
# Metadata Parsing
# ============================================================

def parse_metadata(record):
    """
    Parse one product metadata record.

    Amazon Reviews'23 product metadata uses parent_asin as the
    product identifier corresponding to review parent_asin.
    """

    try:

        product_id = (
            record.get("parent_asin")
            or record.get("product_id")
            or record.get("asin")
        )

        if product_id is None:
            return None

        product_id = str(
            product_id
        ).strip()

        if not product_id:
            return None

        category = (
            record.get("main_category")
        )

        if category is None:

            category = (
                record.get("category")
            )

        if category is None:

            categories = (
                record.get("categories")
            )

            if isinstance(categories, list):

                if categories:

                    # Flatten nested category lists.
                    flattened = []

                    for item in categories:

                        if isinstance(item, list):

                            flattened.extend(
                                str(x)
                                for x in item
                                if x is not None
                            )

                        elif item is not None:

                            flattened.append(
                                str(item)
                            )

                    category = (
                        flattened[-1]
                        if flattened
                        else "Unknown"
                    )

                else:

                    category = "Unknown"

            else:

                category = "Unknown"

        if isinstance(category, list):

            category = (
                category[-1]
                if category
                else "Unknown"
            )

        category = str(
            category
        ).strip()

        if not category:
            category = "Unknown"

        product_title = (
            record.get("product_title")
        )

        if product_title is None:

            product_title = (
                record.get("title")
            )

        if product_title is None:

            product_title = ""

        product_title = str(
            product_title
        ).strip()

        catalog_average_rating = (
            record.get("catalog_average_rating")
        )

        if catalog_average_rating is None:

            catalog_average_rating = (
                record.get("average_rating")
            )

        if catalog_average_rating is None:

            catalog_average_rating = (
                record.get("avg_rating")
            )

        catalog_average_rating = safe_float(
            catalog_average_rating
        )

        return Row(
            product_id=product_id,
            category=category,
            product_title=product_title,
            catalog_average_rating=catalog_average_rating
        )

    except Exception:

        return None


# ============================================================
# Enrichment
# ============================================================

def make_enriched(review, metadata):
    """
    Combine review and product metadata.
    """

    try:

        rating_deviation = None

        if (
            review.rating is not None
            and
            metadata.catalog_average_rating is not None
        ):

            rating_deviation = abs(
                review.rating -
                metadata.catalog_average_rating
            )

        year = get_year_from_timestamp(
            review.timestamp
        )

        text_eligible = (
            len(
                review.review_text.strip()
            )
            >= 30
        )

        return Row(
            user_id=str(review.user_id),
            product_id=str(review.product_id),
            rating=review.rating,
            timestamp=int(review.timestamp),
            review_text=str(review.review_text),
            verified_purchase=bool(
                review.verified_purchase
            ),
            helpful_votes=int(
                review.helpful_votes
            ),
            category=str(
                metadata.category
            ),
            product_title=str(
                metadata.product_title
            ),
            catalog_average_rating=(
                metadata.catalog_average_rating
            ),
            year=int(year),
            rating_deviation=rating_deviation,
            text_eligible=bool(
                text_eligible
            )
        )

    except Exception:

        return None


# ============================================================
# Time Functions
# ============================================================

def get_time_bucket(timestamp, bucket_hours):
    """
    Convert a Unix timestamp into a fixed-size time bucket.

    The returned value is the beginning of the bucket in
    Unix seconds.
    """

    bucket_seconds = (
        int(bucket_hours) * 60 * 60
    )

    if bucket_seconds <= 0:
        raise ValueError(
            "bucket_hours must be greater than zero"
        )

    timestamp = int(timestamp)

    return (
        timestamp // bucket_seconds
    ) * bucket_seconds


def get_year_from_timestamp(timestamp):
    """
    Convert Unix seconds to UTC calendar year.

    Uses UTC so results are deterministic across Spark workers.
    """

    import datetime

    return datetime.datetime.utcfromtimestamp(
        int(timestamp)
    ).year


# ============================================================
# Pair Audit
# ============================================================

def looks_like_id_suffix_artifact(user_id):
    """
    Heuristic for IDs that may contain an accidental numeric
    suffix.

    This is an audit signal only. It does not modify IDs.
    """

    if user_id is None:
        return False

    value = str(user_id).strip()

    patterns = [
        r"_[0-9]+$",
        r"-[0-9]+$",
        r"\([0-9]+\)$"
    ]

    return any(
        re.search(
            pattern,
            value
        )
        for pattern in patterns
    )


def write_pair_audit(
    pair_iterator,
    output_path
):
    """
    Write a simple text audit of graph edges whose user IDs
    may contain suffix artifacts.

    Returns the number of flagged pairs.
    """

    flagged = []

    for edge in pair_iterator:

        if len(edge) < 2:
            continue

        user1 = edge[0]
        user2 = edge[1]

        if (
            looks_like_id_suffix_artifact(user1)
            or
            looks_like_id_suffix_artifact(user2)
        ):

            flagged.append(
                (
                    str(user1),
                    str(user2),
                    int(edge[2])
                    if len(edge) > 2
                    else 0
                )
            )

    # This helper intentionally uses normal Python file I/O
    # because the original pipeline expects a single audit file.
    #
    # For HDFS/S3-only deployments, replace this with an
    # appropriate distributed writer.
    with open(
        output_path,
        "w",
        encoding="utf-8"
    ) as output_file:

        output_file.write(
            "user1,user2,repeated_groups\n"
        )

        for user1, user2, repetitions in flagged:

            output_file.write(
                "{},{},{}\n".format(
                    user1,
                    user2,
                    repetitions
                )
            )

    return len(flagged)


# ============================================================
# Connected Components
# ============================================================

def connected_components(
    vertices,
    edges,
    iterations
):
    """
    Find connected components using iterative minimum-label
    propagation.

    Parameters
    ----------
    vertices:
        RDD containing user IDs.

    edges:
        RDD containing (user1, user2).

    iterations:
        Maximum number of propagation rounds.

    Returns
    -------
    RDD:
        (user_id, component_id)

    Notes
    -----
    Every vertex initially labels itself.

    During every iteration, each vertex exchanges its smallest
    known label with its neighbors.

    Because edges are made undirected here, labels propagate
    through the complete connected component.
    """

    labels = (
        vertices
        .map(
            lambda user_id: (
                user_id,
                user_id
            )
        )
    )

    # Make graph explicitly undirected.
    undirected_edges = (
        edges
        .flatMap(
            lambda edge: [
                (
                    edge[0],
                    edge[1]
                ),
                (
                    edge[1],
                    edge[0]
                )
            ]
        )
        .distinct()
        .cache()
    )

    # Build adjacency lists.
    adjacency = (
        undirected_edges
        .groupByKey()
        .mapValues(
            lambda neighbors:
                list(set(neighbors))
        )
    )

    # Keep isolated vertices.
    adjacency = (
        vertices
        .map(
            lambda user_id: (
                user_id,
                []
            )
        )
        .leftOuterJoin(adjacency)
        .mapValues(
            lambda value:
                value[1]
                if value[1] is not None
                else []
        )
    )

    for _ in range(
        max(1, int(iterations))
    ):

        propagated = (
            labels
            .join(adjacency)
            .flatMap(
                lambda item:
                    [
                        (
                            item[0],
                            item[1][0]
                        )
                    ]
                    +
                    [
                        (
                            neighbor,
                            item[1][0]
                        )
                        for neighbor
                        in item[1][1]
                    ]
            )
            .reduceByKey(
                min
            )
        )

        new_labels = propagated

        # Materialize before comparing if desired by caller.
        labels = new_labels

    return labels


# ============================================================
# Component / Review Statistics
# ============================================================

def calculate_time_concentration(
    timestamps
):
    """
    Measure concentration of reviews in the busiest hourly
    bucket.

    Returns a value in [0, 1].
    """

    timestamps = list(
        timestamps
    )

    if not timestamps:
        return 0.0

    from collections import Counter

    hour_buckets = Counter()

    for timestamp in timestamps:

        bucket = (
            int(timestamp) // 3600
        )

        hour_buckets[bucket] += 1

    busiest = max(
        hour_buckets.values()
    )

    return (
        busiest /
        float(len(timestamps))
    )


def calculate_rating_agreement(
    ratings
):
    """
    Measure how concentrated ratings are around their mean.

    Returns a value approximately in [0, 1].

    A value near 1 means ratings are highly similar.
    """

    ratings = [
        float(x)
        for x in ratings
        if x is not None
    ]

    if len(ratings) <= 1:
        return 1.0

    mean_rating = (
        sum(ratings) /
        float(len(ratings))
    )

    mean_abs_deviation = (
        sum(
            abs(
                rating -
                mean_rating
            )
            for rating in ratings
        )
        /
        float(len(ratings))
    )

    # Amazon ratings are normally on a 1-5 scale.
    normalized = (
        mean_abs_deviation /
        4.0
    )

    return max(
        0.0,
        min(
            1.0,
            1.0 - normalized
        )
    )


def calculate_verified_purchase_rate(
    verified
):
    """
    Percentage of reviews marked as verified purchases.
    """

    verified = list(
        verified
    )

    if not verified:
        return 0.0

    return (
        sum(
            1
            for value in verified
            if bool(value)
        )
        /
        float(len(verified))
    )


def calculate_helpful_vote_average(
    helpful
):
    """
    Average number of helpful votes.
    """

    helpful = [
        float(x)
        for x in helpful
        if x is not None
    ]

    if not helpful:
        return 0.0

    return (
        sum(helpful) /
        float(len(helpful))
    )


def calculate_average_similarity(
    values
):
    """
    Calculate average text similarity.
    """

    values = [
        float(value)
        for value in values
        if value is not None
    ]

    if not values:
        return 0.0

    return (
        sum(values) /
        float(len(values))
    )


# ============================================================
# Combined Coordination Score
# ============================================================

def calculate_combined_coordination_score(
    size_signal,
    density,
    repetition_signal,
    time_concentration,
    rating_agreement,
    average_text_similarity
):
    """
    Combine structural, temporal, rating, and textual signals.

    The weights are explicit so the scoring model is
    reproducible.
    """

    values = [
        size_signal,
        density,
        repetition_signal,
        time_concentration,
        rating_agreement,
        average_text_similarity
    ]

    cleaned = []

    for value in values:

        try:

            value = float(value)

            if value != value:
                value = 0.0

        except (TypeError, ValueError):

            value = 0.0

        cleaned.append(
            max(
                0.0,
                min(
                    1.0,
                    value
                )
            )
        )

    (
        size_signal,
        density,
        repetition_signal,
        time_concentration,
        rating_agreement,
        average_text_similarity
    ) = cleaned

    return (
        COMBINED_SIZE_WEIGHT * size_signal
        +
        COMBINED_DENSITY_WEIGHT * density
        +
        COMBINED_REPETITION_WEIGHT * repetition_signal
        +
        COMBINED_TIME_WEIGHT * time_concentration
        +
        COMBINED_RATING_WEIGHT * rating_agreement
        +
        COMBINED_TEXT_WEIGHT * average_text_similarity
    )


# ============================================================
# Text Tokenization
# ============================================================

def tokenize_review_text(
    text
):
    """
    Basic deterministic tokenizer for review similarity.

    Steps:
      - lowercase
      - retain alphabetic/numeric tokens
      - discard very short tokens
    """

    if text is None:
        return []

    text = str(
        text
    ).lower()

    tokens = re.findall(
        r"[a-z0-9]+",
        text
    )

    tokens = [
        token
        for token in tokens
        if len(token) >= MIN_TOKEN_LENGTH
    ]

    # HashingTF works better with a set of terms for this
    # particular Jaccard-style similarity analysis.
    return sorted(
        set(tokens)
    )


# ============================================================
# Parquet Output
# ============================================================

def write_parquet(
    dataframe,
    output_path
):
    """
    Write enriched review data partitioned by category and year.
    """

    (
        dataframe
        .write
        .mode(OUTPUT_WRITE_MODE)
        .partitionBy(
            "category",
            "year"
        )
        .parquet(
            output_path
        )
    )




# ============================================================
# Main Pipeline
# ============================================================

# ============================================================
# AmazonReviewPipeline.py
# ============================================================
#
# Main orchestration program.
#
# Configuration lives in Config.py.
# Reusable logic lives in Utils.py.
#
# Group Members:
#   1. Asif Faisal Chowdhury
#   2. Souhardya Saha Dip
#   3. Riyadil Zannat
#
# This program:
#   1. Loads Amazon reviews and metadata
#   2. Cleans and enriches review data
#   3. Groups reviews into time buckets
#   4. Identifies repeated user pairs
#   5. Builds a user coordination graph
#   6. Finds connected components using label propagation
#   7. Calculates coordination scores
#   8. Saves enriched data as Parquet by category and year
#   9. Attaches component IDs to reviews
#  10. Calculates detailed component statistics
#  11. Runs distributed MinHash text similarity analysis
#  12. Aggregates text similarity by component
#  13. Calculates extended coordination scores
#  14. Ranks final coordination groups
#  15. Saves component and suspicious-review results
#  16. Generates a runtime/performance summary
#
# ============================================================


import time

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import (
    col,
    udf,
    row_number,
    least,
    greatest
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
    IntegerType,
    ArrayType
)

from pyspark.ml.feature import (
    HashingTF,
    MinHashLSH
)



# ============================================================
# Local Helper Functions
# ============================================================

def aggregate_component(values):

    values = list(values)

    edge_count = len(values)

    total_repeated_groups = sum(
        value[2]
        for value in values
    )

    return (
        edge_count,
        total_repeated_groups
    )


def calculate_initial_score(record):

    component_id, data = record

    statistics, user_count = data

    edge_count = statistics[0]

    total_repeated_groups = statistics[1]

    if user_count <= 1:

        possible_edges = 1.0

    else:

        possible_edges = (
            user_count *
            (user_count - 1)
            / 2.0
        )

    density = (
        edge_count /
        possible_edges
    )

    if edge_count > 0:

        average_repetition = (
            total_repeated_groups /
            float(edge_count)
        )

    else:

        average_repetition = 0.0

    size_signal = min(
        user_count / SIZE_NORMALIZATION,
        1.0
    )

    repetition_signal = min(
        average_repetition / REPETITION_NORMALIZATION,
        1.0
    )

    coordination_score = (
        SIZE_WEIGHT * size_signal
        +
        DENSITY_WEIGHT * density
        +
        REPETITION_WEIGHT * repetition_signal
    )

    return Row(
        component_id=str(
            component_id
        ),
        user_count=int(
            user_count
        ),
        edge_count=int(
            edge_count
        ),
        total_repeated_groups=int(
            total_repeated_groups
        ),
        density=float(
            density
        ),
        average_repetition=float(
            average_repetition
        ),
        size_signal=float(
            size_signal
        ),
        repetition_signal=float(
            repetition_signal
        ),
        coordination_score=float(
            coordination_score
        )
    )


def aggregate_review_statistics(values):

    reviews = list(values)

    ratings = [
        review.rating
        for review in reviews
        if review.rating is not None
    ]

    rating_deviations = [
        review.rating_deviation
        for review in reviews
        if review.rating_deviation is not None
    ]

    timestamps = [
        review.timestamp
        for review in reviews
        if review.timestamp is not None
    ]

    verified = [
        review.verified_purchase
        for review in reviews
        if review.verified_purchase is not None
    ]

    helpful = [
        review.helpful_votes
        for review in reviews
        if review.helpful_votes is not None
    ]

    return (
        len(reviews),

        (
            sum(ratings) / len(ratings)
            if ratings
            else 0.0
        ),

        (
            sum(rating_deviations)
            / len(rating_deviations)
            if rating_deviations
            else 0.0
        ),

        calculate_time_concentration(
            timestamps
        ),

        calculate_rating_agreement(
            ratings
        ),

        calculate_verified_purchase_rate(
            verified
        ),

        calculate_helpful_vote_average(
            helpful
        )
    )


def aggregate_text_similarity(values):

    # IMPORTANT:
    # groupByKey() provides an iterator.
    # Materialize it exactly once.
    values = list(values)

    if not values:

        return (
            0.0,
            0
        )

    return (
        calculate_average_similarity(
            values
        ),
        len(values)
    )


# ============================================================
# Main
# ============================================================

def main():

    pipeline_start = time.time()

    print("=" * 70)
    print("Amazon Review Coordination Analysis")
    print("=" * 70)

    print()
    print("Configuration")
    print("-" * 70)

    print(
        "Reviews:       {}".format(
            REVIEWS_PATH
        )
    )

    print(
        "Metadata:      {}".format(
            METADATA_PATH
        )
    )

    print(
        "Output:        {}".format(
            OUTPUT_PATH
        )
    )

    print(
        "Spark master:  {}".format(
            MASTER
        )
    )

    print(
        "Driver memory: {}".format(
            DRIVER_MEMORY
        )
    )

    print(
        "Text similarity threshold: {:.2f}".format(
            TEXT_SIMILARITY_THRESHOLD
        )
    )

    print(
        "MinHash hash tables: {}".format(
            MINHASH_NUM_HASH_TABLES
        )
    )

    print(
        "MinHash join distance: {:.2f}".format(
            TEXT_JOIN_THRESHOLD
        )
    )

    print()
    print("=" * 70)


    # ========================================================
    # STEP 1
    # ========================================================

    print("Starting Spark...")

    conf = (
        SparkConf()
        .setAppName(APP_NAME)
        .setMaster(MASTER)
        .set(
            "spark.driver.memory",
            DRIVER_MEMORY
        )
    )

    sc = SparkContext(
        conf=conf
    )

    sql_context = SQLContext(
        sc
    )

    print("Spark started.")

    print(
        "Spark version: {}".format(
            sc.version
        )
    )


    # --------------------------------------------------------
    # Load Reviews
    # --------------------------------------------------------

    print()
    print("Loading reviews...")

    reviews_df = (
        sql_context
        .read
        .json(
            REVIEWS_PATH
        )
    )

    review_count = (
        reviews_df.count()
    )

    print(
        "Review records loaded: {}".format(
            review_count
        )
    )


    # --------------------------------------------------------
    # Load Metadata
    # --------------------------------------------------------

    print()
    print("Loading product metadata...")

    metadata_df = (
        sql_context
        .read
        .json(
            METADATA_PATH
        )
    )

    metadata_count = (
        metadata_df.count()
    )

    print(
        "Metadata records loaded: {}".format(
            metadata_count
        )
    )


    # ========================================================
    # STEP 2
    # ========================================================

    print()
    print("Parsing reviews...")

    parsed_reviews = (
        reviews_df
        .rdd
        .map(parse_review)
        .filter(
            lambda x:
                x is not None
        )
        .cache()
    )

    valid_review_count = (
        parsed_reviews.count()
    )

    print(
        "Valid reviews: {}".format(
            valid_review_count
        )
    )


    print()
    print("Parsing metadata...")

    parsed_metadata = (
        metadata_df
        .rdd
        .map(parse_metadata)
        .filter(
            lambda x:
                x is not None
        )
        .cache()
    )

    valid_metadata_count = (
        parsed_metadata.count()
    )

    print(
        "Valid metadata records: {}".format(
            valid_metadata_count
        )
    )


    metadata_by_product = (
        parsed_metadata
        .map(
            lambda x: (
                x.product_id,
                x
            )
        )
    )

    reviews_by_product = (
        parsed_reviews
        .map(
            lambda x: (
                x.product_id,
                x
            )
        )
    )


    print()
    print("Joining reviews with metadata...")

    enriched_rdd = (
        reviews_by_product
        .join(
            metadata_by_product
        )
        .map(
            lambda x:
                make_enriched(
                    x[1][0],
                    x[1][1]
                )
        )
        .filter(
            lambda x:
                x is not None
        )
        .cache()
    )

    enriched_count = (
        enriched_rdd.count()
    )

    print(
        "Enriched review records: {}".format(
            enriched_count
        )
    )


    # ========================================================
    # STEP 3
    # ========================================================

    print()
    print(
        "Creating {}-hour time buckets...".format(
            TIME_BUCKET_HOURS
        )
    )

    bucketed_reviews = (
        enriched_rdd
        .map(
            lambda review: (
                (
                    review.product_id,
                    get_time_bucket(
                        review.timestamp,
                        TIME_BUCKET_HOURS
                    )
                ),
                review
            )
        )
    )

    grouped_reviews = (
        bucketed_reviews
        .groupByKey()
        .mapValues(list)
    )


    def split_large_group(group):

        key, reviews = group

        product_id, time_bucket = key

        reviews = list(reviews)

        if (
            len(reviews)
            <= LARGE_GROUP_THRESHOLD
        ):

            return [
                (
                    product_id,
                    time_bucket,
                    reviews
                )
            ]

        subgroups = {}

        for review in reviews:

            sub_bucket = get_time_bucket(
                review.timestamp,
                LARGE_GROUP_BUCKET_HOURS
            )

            subgroup_key = (
                product_id,
                sub_bucket
            )

            subgroups.setdefault(
                subgroup_key,
                []
            ).append(
                review
            )

        return [
            (
                product,
                bucket,
                group_reviews
            )
            for (
                product,
                bucket
            ), group_reviews
            in subgroups.items()
        ]


    candidate_groups = (
        grouped_reviews
        .flatMap(
            split_large_group
        )
        .cache()
    )

    candidate_group_count = (
        candidate_groups.count()
    )

    print(
        "Candidate groups: {}".format(
            candidate_group_count
        )
    )


    # ========================================================
    # STEP 4
    # ========================================================

    print()
    print("Generating user pairs...")

    def generate_user_pairs(group):

        product_id, time_bucket, reviews = group

        users = sorted(
            set(
                review.user_id
                for review in reviews
                if review.user_id is not None
            )
        )

        return [
            (
                (users[i], users[j]),
                (
                    product_id,
                    time_bucket
                )
            )
            for i in range(
                len(users)
            )
            for j in range(
                i + 1,
                len(users)
            )
        ]


    user_pairs = (
        candidate_groups
        .flatMap(
            generate_user_pairs
        )
    )


    print()
    print("Finding repeated user pairs...")

    repeated_pairs = (
        user_pairs
        .groupByKey()
        .mapValues(
            lambda groups:
                list(
                    set(groups)
                )
        )
        .filter(
            lambda x:
                len(x[1])
                >= MIN_REPEATED_GROUPS
        )
        .cache()
    )

    repeated_pair_count = (
        repeated_pairs.count()
    )

    print(
        "Repeated user pairs: {}".format(
            repeated_pair_count
        )
    )


    # ========================================================
    # STEP 5
    # ========================================================

    print()
    print("Building coordination graph...")

    graph_edges = (
        repeated_pairs
        .map(
            lambda x: (
                x[0][0],
                x[0][1],
                len(x[1])
            )
        )
        .cache()
    )

    vertices = (
        graph_edges
        .flatMap(
            lambda x: [
                x[0],
                x[1]
            ]
        )
        .distinct()
        .cache()
    )

    vertex_count = (
        vertices.count()
    )

    edge_count = (
        graph_edges.count()
    )

    print(
        "Graph vertices: {}".format(
            vertex_count
        )
    )

    print(
        "Graph edges: {}".format(
            edge_count
        )
    )


    flagged_pair_count = write_pair_audit(
        graph_edges.toLocalIterator(),
        PAIR_AUDIT_PATH
    )

    print(
        "Pairs with possible ID suffix artifacts: {}".format(
            flagged_pair_count
        )
    )


    # ========================================================
    # STEP 6
    # ========================================================

    print()
    print(
        "Finding connected components..."
    )

    simple_edges = (
        graph_edges
        .map(
            lambda x: (
                x[0],
                x[1]
            )
        )
    )

    labels = (
        connected_components(
            vertices,
            simple_edges,
            LABEL_PROPAGATION_ITERATIONS
        )
        .cache()
    )


    # ========================================================
    # STEP 7
    # ========================================================

    print()
    print(
        "Calculating coordination scores..."
    )

    component_sizes = (
        labels
        .map(
            lambda x: (
                x[1],
                1
            )
        )
        .reduceByKey(
            lambda a, b:
                a + b
        )
    )


    edges_with_source = (
        graph_edges
        .map(
            lambda x: (
                x[0],
                (
                    x[1],
                    x[2]
                )
            )
        )
    )

    edges_with_labels = (
        edges_with_source
        .join(
            labels
        )
    )

    component_edges = (
        edges_with_labels
        .map(
            lambda x: (
                x[1][1],
                (
                    x[0],
                    x[1][0][0],
                    x[1][0][1]
                )
            )
        )
    )


    component_statistics = (
        component_edges
        .groupByKey()
        .mapValues(
            aggregate_component
        )
    )

    component_data = (
        component_statistics
        .join(
            component_sizes
        )
    )


    scored_components = (
        component_data
        .map(
            calculate_initial_score
        )
        .cache()
    )


    top_components = (
        scored_components
        .sortBy(
            lambda x:
                x.coordination_score,
            ascending=False
        )
        .take(
            TOP_N
        )
    )


    print()
    print("=" * 70)
    print(
        "INITIAL TOP {} COORDINATION GROUPS"
        .format(TOP_N)
    )
    print("=" * 70)


    for index, component in enumerate(
        top_components,
        start=1
    ):

        print()
        print(
            "Group #{}".format(
                index
            )
        )

        print(
            "Component ID: {}"
            .format(
                component.component_id
            )
        )

        print(
            "Users: {}"
            .format(
                component.user_count
            )
        )

        print(
            "Edges: {}"
            .format(
                component.edge_count
            )
        )

        print(
            "Repeated Groups: {}"
            .format(
                component.total_repeated_groups
            )
        )

        print(
            "Density: {:.4f}"
            .format(
                component.density
            )
        )

        print(
            "Average Repetition: {:.4f}"
            .format(
                component.average_repetition
            )
        )

        print(
            "Coordination Score: {:.4f}"
            .format(
                component.coordination_score
            )
        )


    # ========================================================
    # STEP 8
    # ========================================================

    print()
    print(
        "Saving enriched reviews..."
    )

    enriched_schema = StructType(
        [
            StructField(
                "user_id",
                StringType(),
                False
            ),

            StructField(
                "product_id",
                StringType(),
                False
            ),

            StructField(
                "rating",
                DoubleType(),
                True
            ),

            StructField(
                "timestamp",
                LongType(),
                False
            ),

            StructField(
                "review_text",
                StringType(),
                False
            ),

            StructField(
                "verified_purchase",
                BooleanType(),
                False
            ),

            StructField(
                "helpful_votes",
                LongType(),
                False
            ),

            StructField(
                "category",
                StringType(),
                False
            ),

            StructField(
                "product_title",
                StringType(),
                False
            ),

            StructField(
                "catalog_average_rating",
                DoubleType(),
                True
            ),

            StructField(
                "year",
                IntegerType(),
                False
            ),

            StructField(
                "rating_deviation",
                DoubleType(),
                True
            ),

            StructField(
                "text_eligible",
                BooleanType(),
                False
            )
        ]
    )


    enriched_columns = (
        enriched_schema.fieldNames()
    )


    enriched_df = (
        sql_context
        .createDataFrame(
            enriched_rdd.map(
                lambda review:
                    tuple(
                        review[name]
                        for name
                        in enriched_columns
                    )
            ),
            enriched_schema
        )
    )


    write_parquet(
        enriched_df,
        OUTPUT_PATH
    )


    # ========================================================
    # STEP 9
    # ========================================================

    print()
    print(
        "Attaching component IDs to reviews..."
    )

    labels_df = (
        sql_context
        .createDataFrame(
            labels.map(
                lambda x:
                    Row(
                        user_id=str(
                            x[0]
                        ),
                        component_id=str(
                            x[1]
                        )
                    )
            )
        )
    )


    component_reviews_df = (
        enriched_df
        .join(
            labels_df,
            on="user_id",
            how="inner"
        )
        .cache()
    )


    component_review_count = (
        component_reviews_df.count()
    )


    print(
        "Reviews associated with graph components: {}".format(
            component_review_count
        )
    )


    # ========================================================
    # STEP 10
    # ========================================================

    print()
    print(
        "Calculating detailed component statistics..."
    )

    component_review_rdd = (
        component_reviews_df
        .rdd
        .map(
            lambda row: (
                row.component_id,
                row
            )
        )
    )


    detailed_component_statistics = (
        component_review_rdd
        .groupByKey()
        .mapValues(
            aggregate_review_statistics
        )
    )


    component_stats_rows = (
        detailed_component_statistics
        .map(
            lambda x:
                Row(
                    component_id=str(
                        x[0]
                    ),
                    review_count=int(
                        x[1][0]
                    ),
                    average_rating=float(
                        x[1][1]
                    ),
                    average_rating_deviation=float(
                        x[1][2]
                    ),
                    time_concentration=float(
                        x[1][3]
                    ),
                    rating_agreement=float(
                        x[1][4]
                    ),
                    verified_purchase_rate=float(
                        x[1][5]
                    ),
                    average_helpful_votes=float(
                        x[1][6]
                    )
                )
        )
    )


    component_stats_df = (
        sql_context
        .createDataFrame(
            component_stats_rows
        )
    )


    # ========================================================
    # STEP 11
    # ========================================================

    print()
    print(
        "Running distributed MinHash text similarity analysis..."
    )


    text_reviews_df = (
        component_reviews_df
        .filter(
            col("text_eligible") == True
        )
        .filter(
            col("review_text").isNotNull()
        )
        .filter(
            col("review_text") != ""
        )
        .select(
            "user_id",
            "component_id",
            "product_id",
            "timestamp",
            "review_text"
        )
    )


    # --------------------------------------------------------
    # Restrict text analysis to initial top components.
    # --------------------------------------------------------

    top_component_ids = [
        str(
            component.component_id
        )
        for component
        in top_components
    ]


    if top_component_ids:

        top_component_df = (
            sql_context
            .createDataFrame(
                [
                    Row(
                        component_id=value
                    )
                    for value
                    in top_component_ids
                ]
            )
        )

        text_reviews_df = (
            text_reviews_df
            .join(
                top_component_df,
                on="component_id",
                how="inner"
            )
        )

    else:

        text_reviews_df = (
            text_reviews_df
            .limit(0)
        )


    # --------------------------------------------------------
    # Enforce maximum reviews per component.
    # --------------------------------------------------------

    if top_component_ids:

        text_window = (
            Window
            .partitionBy(
                "component_id"
            )
            .orderBy(
                col(
                    "timestamp"
                ).asc(),
                col(
                    "user_id"
                ).asc(),
                col(
                    "product_id"
                ).asc()
            )
        )


        text_reviews_df = (
            text_reviews_df
            .withColumn(
                "text_rank",
                row_number().over(
                    text_window
                )
            )
            .filter(
                col("text_rank")
                <=
                MAX_TEXT_REVIEWS_PER_COMPONENT
            )
            .drop(
                "text_rank"
            )
        )


    # --------------------------------------------------------
    # Tokenize.
    # --------------------------------------------------------

    tokenize_udf = udf(
        tokenize_review_text,
        ArrayType(
            StringType()
        )
    )


    text_reviews_df = (
        text_reviews_df
        .withColumn(
            "tokens",
            tokenize_udf(
                col("review_text")
            )
        )
        .filter(
            col("tokens").isNotNull()
        )
    )


    # --------------------------------------------------------
    # HashingTF + MinHash.
    # --------------------------------------------------------

    text_review_count = text_reviews_df.count()

    if text_review_count == 0:

        print("No eligible text reviews found; skipping MinHash fitting.")

        similarity_pairs = (
            sql_context
            .createDataFrame(
                [],
                StructType([
                    StructField("component_id", StringType(), False),
                    StructField("user1", StringType(), False),
                    StructField("user2", StringType(), False),
                    StructField("product1", StringType(), False),
                    StructField("product2", StringType(), False),
                    StructField("timestamp1", LongType(), False),
                    StructField("timestamp2", LongType(), False),
                    StructField("jaccard_distance", DoubleType(), False),
                    StructField("text_similarity", DoubleType(), False)
                ])
            )
        )

    else:

        hashing_tf = HashingTF(
            inputCol="tokens",
            outputCol="features",
            numFeatures=TEXT_NUM_FEATURES
        )

        feature_df = hashing_tf.transform(
            text_reviews_df
        )

        minhash = MinHashLSH(
            inputCol="features",
            outputCol="hashes",
            numHashTables=MINHASH_NUM_HASH_TABLES
        )

        minhash_model = minhash.fit(
            feature_df
        )

        # approxSimilarityJoin expects Jaccard DISTANCE.
        # TEXT_JOIN_THRESHOLD = 1 - TEXT_SIMILARITY_THRESHOLD.
        similarity_pairs = (
            minhash_model
            .approxSimilarityJoin(
                feature_df,
                feature_df,
                TEXT_JOIN_THRESHOLD,
                distCol="jaccard_distance"
            )
        .filter(
            col("datasetA.user_id")
            !=
            col("datasetB.user_id")
        )
        .filter(
            col("datasetA.component_id")
            ==
            col("datasetB.component_id")
        )
        .select(
            col(
                "datasetA.component_id"
            ).alias(
                "component_id"
            ),

            col(
                "datasetA.user_id"
            ).alias(
                "user1"
            ),

            col(
                "datasetB.user_id"
            ).alias(
                "user2"
            ),

            col(
                "datasetA.product_id"
            ).alias(
                "product1"
            ),

            col(
                "datasetB.product_id"
            ).alias(
                "product2"
            ),

            col(
                "datasetA.timestamp"
            ).alias(
                "timestamp1"
            ),

            col(
                "datasetB.timestamp"
            ).alias(
                "timestamp2"
            ),

            col(
                "jaccard_distance"
            )
        )
    )


    # --------------------------------------------------------
    # Remove duplicate A/B and B/A pairs.
    #
    # Keep one row per component/user-pair/review-pair.
    # --------------------------------------------------------

    similarity_pairs = (
        similarity_pairs
        .withColumn(
            "user_low",
            least(
                col("user1"),
                col("user2")
            )
        )
        .withColumn(
            "user_high",
            greatest(
                col("user1"),
                col("user2")
            )
        )
        .withColumn(
            "product_low",
            least(
                col("product1"),
                col("product2")
            )
        )
        .withColumn(
            "product_high",
            greatest(
                col("product1"),
                col("product2")
            )
        )
        .dropDuplicates(
            [
                "component_id",
                "user_low",
                "user_high",
                "product_low",
                "product_high",
                "jaccard_distance"
            ]
        )
        .drop(
            "user_low",
            "user_high",
            "product_low",
            "product_high"
        )
    )


    similarity_pairs = (
        similarity_pairs
        .withColumn(
            "text_similarity",
            1.0 -
            col("jaccard_distance")
        )
    )


    similarity_pair_count = (
        similarity_pairs.count()
    )


    print(
        "Similar review pairs found: {}".format(
            similarity_pair_count
        )
    )


    # ========================================================
    # STEP 12
    # ========================================================

    print()
    print(
        "Aggregating text similarity by component..."
    )


    if similarity_pair_count > 0:

        text_similarity_rdd = (
            similarity_pairs
            .select(
                "component_id",
                "text_similarity"
            )
            .rdd
            .map(
                lambda row: (
                    row.component_id,
                    row.text_similarity
                )
            )
        )


        text_similarity_statistics = (
            text_similarity_rdd
            .groupByKey()
            .mapValues(
                aggregate_text_similarity
            )
        )


        text_similarity_rows = (
            text_similarity_statistics
            .map(
                lambda x:
                    Row(
                        component_id=str(
                            x[0]
                        ),
                        average_text_similarity=float(
                            x[1][0]
                        ),
                        similar_pair_count=int(
                            x[1][1]
                        )
                    )
            )
        )


        text_similarity_df = (
            sql_context
            .createDataFrame(
                text_similarity_rows
            )
        )

    else:

        text_similarity_schema = (
            StructType(
                [
                    StructField(
                        "component_id",
                        StringType(),
                        False
                    ),

                    StructField(
                        "average_text_similarity",
                        DoubleType(),
                        False
                    ),

                    StructField(
                        "similar_pair_count",
                        LongType(),
                        False
                    )
                ]
            )
        )


        text_similarity_df = (
            sql_context
            .createDataFrame(
                [],
                text_similarity_schema
            )
        )


    # ========================================================
    # STEP 13
    # ========================================================

    print()
    print(
        "Calculating extended coordination scores..."
    )


    scored_components_df = (
        sql_context
        .createDataFrame(
            scored_components
        )
    )


    final_component_df = (
        scored_components_df
        .join(
            component_stats_df,
            on="component_id",
            how="left"
        )
        .join(
            text_similarity_df,
            on="component_id",
            how="left"
        )
        .fillna(
            {
                "average_text_similarity": 0.0,
                "similar_pair_count": 0,
                "review_count": 0,
                "average_rating": 0.0,
                "average_rating_deviation": 0.0,
                "time_concentration": 0.0,
                "rating_agreement": 0.0,
                "verified_purchase_rate": 0.0,
                "average_helpful_votes": 0.0
            }
        )
    )


    combined_score_udf = udf(
        calculate_combined_coordination_score,
        DoubleType()
    )


    final_component_df = (
        final_component_df
        .withColumn(
            "final_coordination_score",
            combined_score_udf(
                col("size_signal"),
                col("density"),
                col("repetition_signal"),
                col("time_concentration"),
                col("rating_agreement"),
                col("average_text_similarity")
            )
        )
    )


    # ========================================================
    # STEP 14
    # ========================================================

    print()
    print(
        "Ranking final coordination groups..."
    )


    ranked_components_df = (
        final_component_df
        .orderBy(
            col(
                "final_coordination_score"
            ).desc(),
            col(
                "component_id"
            ).asc()
        )
    )


    final_top_components = (
        ranked_components_df
        .limit(
            TOP_N
        )
        .collect()
    )


    print()
    print("=" * 70)
    print(
        "FINAL TOP {} COORDINATION GROUPS"
        .format(TOP_N)
    )
    print("=" * 70)


    if not final_top_components:

        print(
            "No coordination groups were found."
        )

    else:

        for index, component in enumerate(
            final_top_components,
            start=1
        ):

            print()
            print(
                "Group #{}".format(
                    index
                )
            )

            print(
                "Component ID: {}"
                .format(
                    component.component_id
                )
            )

            print(
                "Users: {}"
                .format(
                    component.user_count
                )
            )

            print(
                "Edges: {}"
                .format(
                    component.edge_count
                )
            )

            print(
                "Repeated Groups: {}"
                .format(
                    component.total_repeated_groups
                )
            )

            print(
                "Reviews: {}"
                .format(
                    component.review_count
                )
            )

            print(
                "Density: {:.4f}"
                .format(
                    component.density
                )
            )

            print(
                "Average Repetition: {:.4f}"
                .format(
                    component.average_repetition
                )
            )

            print(
                "Time Concentration: {:.4f}"
                .format(
                    component.time_concentration
                )
            )

            print(
                "Rating Agreement: {:.4f}"
                .format(
                    component.rating_agreement
                )
            )

            print(
                "Average Rating Deviation: {:.4f}"
                .format(
                    component.average_rating_deviation
                )
            )

            print(
                "Verified Purchase Rate: {:.4f}"
                .format(
                    component.verified_purchase_rate
                )
            )

            print(
                "Average Helpful Votes: {:.4f}"
                .format(
                    component.average_helpful_votes
                )
            )

            print(
                "Average Text Similarity: {:.4f}"
                .format(
                    component.average_text_similarity
                )
            )

            print(
                "Similar Review Pairs: {}"
                .format(
                    component.similar_pair_count
                )
            )

            print(
                "Final Coordination Score: {:.4f}"
                .format(
                    component.final_coordination_score
                )
            )


    # ========================================================
    # STEP 15
    # ========================================================

    print()
    print(
        "Saving component analysis results..."
    )


    (
        ranked_components_df
        .write
        .mode(OUTPUT_WRITE_MODE)
        .parquet(
            COMPONENT_OUTPUT_PATH
        )
    )


    print(
        "Component results saved to: {}"
        .format(
            COMPONENT_OUTPUT_PATH
        )
    )


    # --------------------------------------------------------
    # Save text similarity results.
    #
    # This is written even if there are zero matches.
    # --------------------------------------------------------

    (
        similarity_pairs
        .write
        .mode(OUTPUT_WRITE_MODE)
        .parquet(
            TEXT_OUTPUT_PATH
        )
    )


    print(
        "Text similarity results saved to: {}"
        .format(
            TEXT_OUTPUT_PATH
        )
    )


    # --------------------------------------------------------
    # Candidate reviews from final selected components.
    # --------------------------------------------------------

    final_component_ids = [
        str(
            row.component_id
        )
        for row
        in final_top_components
    ]


    if final_component_ids:

        final_component_id_df = (
            sql_context
            .createDataFrame(
                [
                    Row(
                        component_id=value
                    )
                    for value
                    in final_component_ids
                ]
            )
        )


        candidate_reviews_df = (
            component_reviews_df
            .join(
                final_component_id_df,
                on="component_id",
                how="inner"
            )
        )


        (
            candidate_reviews_df
            .write
            .mode(OUTPUT_WRITE_MODE)
            .partitionBy(
                "category",
                "year"
            )
            .parquet(
                SUSPICIOUS_REVIEW_OUTPUT_PATH
            )
        )


        print(
            "Candidate group reviews saved to: {}"
            .format(
                SUSPICIOUS_REVIEW_OUTPUT_PATH
            )
        )

    else:

        print(
            "No final coordination groups were found."
        )


    # ========================================================
    # STEP 16
    # ========================================================

    print()
    print(
        "Generating performance summary..."
    )


    pipeline_end = time.time()

    total_runtime_seconds = (
        pipeline_end -
        pipeline_start
    )


    performance_row = Row(
        review_records_loaded=int(
            review_count
        ),

        metadata_records_loaded=int(
            metadata_count
        ),

        valid_reviews=int(
            valid_review_count
        ),

        valid_metadata=int(
            valid_metadata_count
        ),

        enriched_reviews=int(
            enriched_count
        ),

        candidate_groups=int(
            candidate_group_count
        ),

        repeated_user_pairs=int(
            repeated_pair_count
        ),

        graph_vertices=int(
            vertex_count
        ),

        graph_edges=int(
            edge_count
        ),

        flagged_id_pairs=int(
            flagged_pair_count
        ),

        component_reviews=int(
            component_review_count
        ),

        similar_text_pairs=int(
            similarity_pair_count
        ),

        total_runtime_seconds=float(
            total_runtime_seconds
        )
    )


    performance_df = (
        sql_context
        .createDataFrame(
            [performance_row]
        )
    )


    (
        performance_df
        .write
        .mode(OUTPUT_WRITE_MODE)
        .parquet(
            PERFORMANCE_OUTPUT_PATH
        )
    )


    print(
        "Performance summary saved to: {}"
        .format(
            PERFORMANCE_OUTPUT_PATH
        )
    )


    # ========================================================
    # Finish
    # ========================================================

    print()
    print("=" * 70)
    print(
        "PIPELINE COMPLETED SUCCESSFULLY"
    )
    print("=" * 70)

    print()
    print(
        "Enriched review output:"
    )
    print(
        OUTPUT_PATH
    )

    print()
    print(
        "Component output:"
    )
    print(
        COMPONENT_OUTPUT_PATH
    )

    print()
    print(
        "Text similarity output:"
    )
    print(
        TEXT_OUTPUT_PATH
    )

    print()
    print(
        "Candidate group review output:"
    )
    print(
        SUSPICIOUS_REVIEW_OUTPUT_PATH
    )

    print()
    print(
        "Performance output:"
    )
    print(
        PERFORMANCE_OUTPUT_PATH
    )

    print()
    print(
        "Total runtime: {:.2f} seconds"
        .format(
            total_runtime_seconds
        )
    )


    sc.stop()


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    main()
