# ============================================================
# AmazonReviewPipeline.py
# ============================================================
#
# Identifying Coordinated Amazon Review Groups
# with Distributed Text and Graph Analysis
#
# This program:
#   1. Loads Amazon reviews and product metadata
#   2. Cleans and enriches review data
#   3. Groups reviews into time buckets
#   4. Identifies repeated user pairs
#   5. Builds a user coordination graph
#   6. Finds connected components using label propagation
#   7. Calculates coordination scores
#   8. Saves enriched data as Parquet
#
# Configuration is stored separately in config.py
# ============================================================

import re
import sys
import math

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row

from config import (
    REVIEWS_PATH,
    METADATA_PATH,
    OUTPUT_PATH,

    APP_NAME,
    MASTER,
    DRIVER_MEMORY,

    TIME_BUCKET_HOURS,
    LARGE_GROUP_THRESHOLD,
    LARGE_GROUP_BUCKET_HOURS,

    MIN_REPEATED_GROUPS,
    LABEL_PROPAGATION_ITERATIONS,
    TOP_N,

    SIZE_WEIGHT,
    DENSITY_WEIGHT,
    REPETITION_WEIGHT,

    SIZE_NORMALIZATION,
    REPETITION_NORMALIZATION,

    MIN_TEXT_LENGTH
)


# ============================================================
# Helper Functions
# ============================================================

def get_value(row, *keys):
    """
    Return the first non-null value found for the supplied keys.
    """

    for key in keys:
        try:
            value = row[key]

            if value is not None:
                return value

        except Exception:
            pass

    return None


def get_float(row, *keys):
    """
    Safely convert a value to float.
    """

    value = get_value(row, *keys)

    if value is None:
        return None

    try:
        return float(value)
    except Exception:
        return None


def get_int(row, *keys):
    """
    Safely convert a value to integer.
    """

    value = get_value(row, *keys)

    if value is None:
        return 0

    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return 0


def get_long(row, *keys):
    """
    Safely convert a value to integer timestamp.
    """

    value = get_value(row, *keys)

    if value is None:
        return None

    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return None


def get_boolean(row, *keys):
    """
    Safely convert a value to boolean.
    """

    value = get_value(row, *keys)

    if value is None:
        return False

    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        value = value.strip().lower()

        if value in ("true", "1", "yes"):
            return True

        if value in ("false", "0", "no"):
            return False

    try:
        return bool(value)
    except Exception:
        return False


def clean_text(text):
    """
    Clean review text.
    """

    if text is None:
        return ""

    text = str(text)

    # Remove HTML tags
    text = re.sub(r"<[^>]+>", " ", text)

    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def get_time_bucket(timestamp, bucket_hours):
    """
    Convert timestamp to a time bucket.

    timestamp is expected to be in seconds.
    """

    if timestamp is None:
        return None

    bucket_seconds = bucket_hours * 60 * 60

    return (timestamp // bucket_seconds) * bucket_seconds


# ============================================================
# Review Parsing
# ============================================================

def parse_review(row):
    """
    Parse one raw review record.
    """

    user_id = get_value(
        row,
        "user_id",
        "user"
    )

    product_id = get_value(
        row,
        "parent_asin",
        "asin",
        "product_id"
    )

    review_text = get_value(
        row,
        "text",
        "reviewText"
    )

    rating = get_float(
        row,
        "rating",
        "overall"
    )

    timestamp = get_long(
        row,
        "timestamp",
        "unixReviewTime",
        "reviewTime"
    )

    verified_purchase = get_boolean(
        row,
        "verified_purchase",
        "verifiedPurchase"
    )

    helpful_votes = get_int(
        row,
        "helpful_votes",
        "helpfulVote",
        "helpful"
    )

    # --------------------------------------------------------
    # Convert milliseconds to seconds if necessary
    # --------------------------------------------------------

    if timestamp is not None and timestamp > 100000000000:
        timestamp = timestamp // 1000

    # --------------------------------------------------------
    # Clean text
    # --------------------------------------------------------

    review_text = clean_text(review_text)

    # --------------------------------------------------------
    # Validate essential fields
    # --------------------------------------------------------

    if user_id is None:
        return None

    if product_id is None:
        return None

    if timestamp is None:
        return None

    return Row(
        user_id=str(user_id),
        product_id=str(product_id),
        rating=rating,
        timestamp=timestamp,
        review_text=review_text,
        verified_purchase=verified_purchase,
        helpful_votes=helpful_votes
    )


# ============================================================
# Metadata Parsing
# ============================================================

def parse_metadata(row):
    """
    Parse one product metadata record.
    """

    product_id = get_value(
        row,
        "parent_asin",
        "asin",
        "product_id"
    )

    category = get_value(
        row,
        "main_category",
        "category"
    )

    title = get_value(
        row,
        "title",
        "product_title"
    )

    average_rating = get_float(
        row,
        "average_rating"
    )

    if product_id is None:
        return None

    return Row(
        product_id=str(product_id),
        category=str(category) if category is not None else "",
        product_title=str(title) if title is not None else "",
        catalog_average_rating=average_rating
    )


# ============================================================
# Enrichment
# ============================================================

def make_enriched(review, metadata):
    """
    Combine review information with product metadata.
    """

    return Row(
        user_id=review.user_id,
        product_id=review.product_id,
        rating=review.rating,
        timestamp=review.timestamp,
        review_text=review.review_text,
        verified_purchase=review.verified_purchase,
        helpful_votes=review.helpful_votes,

        category=metadata.category if metadata else "",
        product_title=metadata.product_title if metadata else "",
        catalog_average_rating=(
            metadata.catalog_average_rating
            if metadata
            else None
        )
    )


# ============================================================
# Connected Components
# ============================================================

def connected_components(vertices, edges, iterations):
    """
    Find connected components using iterative minimum-label
    propagation.

    This mirrors the approach used in the original Spark 1.3
    Java implementation and does not require GraphFrames.
    """

    # --------------------------------------------------------
    # vertices:
    #   RDD containing user IDs
    #
    # edges:
    #   RDD containing tuples:
    #       (user1, user2)
    # --------------------------------------------------------

    labels = vertices.map(
        lambda v: (v, v)
    )

    # Send edges in both directions
    directed_edges = edges.flatMap(
        lambda edge: [
            (edge[0], edge[1]),
            (edge[1], edge[0])
        ]
    )

    for iteration in range(iterations):

        # Join source vertex with its current label
        joined = directed_edges.join(labels)

        # joined:
        #   source -> (neighbor, source_label)

        proposals = joined.map(
            lambda x: (
                x[1][0],
                x[1][1]
            )
        )

        # Each vertex also proposes its own label
        own_labels = labels.map(
            lambda x: (
                x[0],
                x[1]
            )
        )

        # Find minimum label received by each vertex
        proposed_labels = (
            proposals
            .union(own_labels)
            .reduceByKey(min)
        )

        labels = proposed_labels

        print(
            "Connected components iteration {}/{}".format(
                iteration + 1,
                iterations
            )
        )

    return labels


# ============================================================
# Save DataFrame as Parquet
# ============================================================

def write_parquet(df, output_path):
    """
    Save a DataFrame as Parquet.
    """

    print("Writing Parquet output:")
    print(output_path)

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print("Parquet output successfully written.")


# ============================================================
# Main Pipeline
# ============================================================

def main():

    print("=" * 70)
    print("Amazon Review Coordination Analysis")
    print("=" * 70)

    print()
    print("Configuration")
    print("-" * 70)

    print("Reviews:       {}".format(REVIEWS_PATH))
    print("Metadata:      {}".format(METADATA_PATH))
    print("Output:        {}".format(OUTPUT_PATH))
    print("Spark master:  {}".format(MASTER))
    print("Driver memory: {}".format(DRIVER_MEMORY))

    print()
    print("=" * 70)

    # ========================================================
    # 1. Create Spark Context
    # ========================================================

    print("Starting Spark...")

    conf = (
        SparkConf()
        .setAppName(APP_NAME)
        .setMaster(MASTER)
    )

    sc = SparkContext(conf=conf)

    sql_context = SQLContext(sc)

    print("Spark started.")
    print("Spark version: {}".format(sc.version))

    # ========================================================
    # 2. Load Reviews
    # ========================================================

    print()
    print("Step 1: Loading reviews...")

    reviews_df = sql_context.read.json(
        REVIEWS_PATH
    )

    print(
        "Review records loaded: {}".format(
            reviews_df.count()
        )
    )

    # ========================================================
    # 3. Load Metadata
    # ========================================================

    print()
    print("Step 2: Loading product metadata...")

    metadata_df = sql_context.read.json(
        METADATA_PATH
    )

    print(
        "Metadata records loaded: {}".format(
            metadata_df.count()
        )
    )

    # ========================================================
    # 4. Parse Reviews
    # ========================================================

    print()
    print("Step 3: Parsing reviews...")

    parsed_reviews = (
        reviews_df.rdd
        .map(parse_review)
        .filter(lambda x: x is not None)
        .filter(
            lambda x:
                x.review_text is not None
                and len(x.review_text) >= MIN_TEXT_LENGTH
        )
    )

    print(
        "Valid reviews: {}".format(
            parsed_reviews.count()
        )
    )

    # ========================================================
    # 5. Parse Metadata
    # ========================================================

    print()
    print("Step 4: Parsing metadata...")

    parsed_metadata = (
        metadata_df.rdd
        .map(parse_metadata)
        .filter(lambda x: x is not None)
    )

    print(
        "Valid metadata records: {}".format(
            parsed_metadata.count()
        )
    )

    # ========================================================
    # 6. Prepare Metadata for Join
    # ========================================================

    metadata_by_product = parsed_metadata.map(
        lambda x: (
            x.product_id,
            x
        )
    )

    reviews_by_product = parsed_reviews.map(
        lambda x: (
            x.product_id,
            x
        )
    )

    # ========================================================
    # 7. Join Reviews with Metadata
    # ========================================================

    print()
    print("Step 5: Joining reviews with metadata...")

    enriched_rdd = (
        reviews_by_product
        .join(metadata_by_product)
        .map(
            lambda x: make_enriched(
                x[1][0],
                x[1][1]
            )
        )
    )

    enriched_count = enriched_rdd.count()

    print(
        "Enriched review records: {}".format(
            enriched_count
        )
    )

    # ========================================================
    # 8. Create Time Buckets
    # ========================================================

    print()
    print("Step 6: Creating 24-hour time buckets...")

    bucketed_reviews = enriched_rdd.map(
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

    # ========================================================
    # 9. Group Reviews by Product + Time
    # ========================================================

    grouped_reviews = (
        bucketed_reviews
        .groupByKey()
        .mapValues(list)
    )

    # ========================================================
    # 10. Split Large Groups
    # ========================================================

    print()
    print(
        "Step 7: Splitting groups larger than {} reviews...".format(
            LARGE_GROUP_THRESHOLD
        )
    )

    def split_large_group(group):

        key, reviews = group

        product_id, time_bucket = key

        reviews = list(reviews)

        # Normal-sized group
        if len(reviews) <= LARGE_GROUP_THRESHOLD:
            return [
                (
                    product_id,
                    time_bucket,
                    reviews
                )
            ]

        # ----------------------------------------------------
        # Large group:
        # split into smaller time buckets
        # ----------------------------------------------------

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

            if subgroup_key not in subgroups:
                subgroups[subgroup_key] = []

            subgroups[subgroup_key].append(review)

        return [
            (
                product,
                bucket,
                group_reviews
            )
            for (product, bucket), group_reviews
            in subgroups.items()
        ]

    candidate_groups = (
        grouped_reviews
        .flatMap(split_large_group)
    )

    print(
        "Candidate groups: {}".format(
            candidate_groups.count()
        )
    )

    # ========================================================
    # 11. Generate User Pairs
    # ========================================================

    print()
    print("Step 8: Generating user pairs...")

    def generate_user_pairs(group):

        product_id, time_bucket, reviews = group

        users = sorted(
            set(
                review.user_id
                for review in reviews
                if review.user_id is not None
            )
        )

        pairs = []

        for i in range(len(users)):

            for j in range(i + 1, len(users)):

                user1 = users[i]
                user2 = users[j]

                pairs.append(
                    (
                        (user1, user2),
                        (
                            product_id,
                            time_bucket
                        )
                    )
                )

        return pairs

    user_pairs = candidate_groups.flatMap(
        generate_user_pairs
    )

    # ========================================================
    # 12. Count Repeated Product-Time Groups
    # ========================================================

    print()
    print("Step 9: Finding repeated user pairs...")

    repeated_pairs = (
        user_pairs
        .mapValues(lambda x: x)
        .groupByKey()
        .mapValues(
            lambda groups:
                list(set(groups))
        )
        .filter(
            lambda x:
                len(x[1]) >= MIN_REPEATED_GROUPS
        )
    )

    print(
        "Repeated user pairs: {}".format(
            repeated_pairs.count()
        )
    )

    # ========================================================
    # 13. Build Graph Edges
    # ========================================================

    print()
    print("Step 10: Building coordination graph...")

    graph_edges = (
        repeated_pairs
        .map(
            lambda x: (
                x[0][0],
                x[0][1],
                len(x[1])
            )
        )
    )

    # ========================================================
    # 14. Build Vertices
    # ========================================================

    vertices = (
        graph_edges
        .flatMap(
            lambda x: [
                x[0],
                x[1]
            ]
        )
        .distinct()
    )

    vertex_count = vertices.count()

    edge_count = graph_edges.count()

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

    # ========================================================
    # 15. Connected Components
    # ========================================================

    print()
    print(
        "Step 11: Finding connected components..."
    )

    simple_edges = graph_edges.map(
        lambda x: (
            x[0],
            x[1]
        )
    )

    labels = connected_components(
        vertices,
        simple_edges,
        LABEL_PROPAGATION_ITERATIONS
    )

    # labels:
    # user_id -> component_id

    # ========================================================
    # 16. Count Users in Each Component
    # ========================================================

    print()
    print("Step 12: Counting users per component...")

    component_sizes = (
        labels
        .map(
            lambda x: (
                x[1],
                1
            )
        )
        .reduceByKey(
            lambda a, b: a + b
        )
    )

    # ========================================================
    # 17. Assign Edges to Components
    # ========================================================

    print()
    print(
        "Step 13: Aggregating coordination groups..."
    )

    edges_with_source = graph_edges.map(
        lambda x: (
            x[0],
            (
                x[1],
                x[2]
            )
        )
    )

    edges_with_labels = (
        edges_with_source
        .join(labels)
    )

    # Result:
    #
    # user1 -> ((user2, repetition_count), component)

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

    # ========================================================
    # 18. Aggregate Component Statistics
    # ========================================================

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

    component_statistics = (
        component_edges
        .groupByKey()
        .mapValues(
            aggregate_component
        )
    )

    # ========================================================
    # 19. Join Component Sizes
    # ========================================================

    component_data = (
        component_statistics
        .join(component_sizes)
    )

    # ========================================================
    # 20. Calculate Coordination Score
    # ========================================================

    print()
    print(
        "Step 14: Calculating coordination scores..."
    )

    def calculate_score(record):

        component_id, data = record

        statistics, user_count = data

        edge_count = statistics[0]

        total_repeated_groups = statistics[1]

        # ----------------------------------------------------
        # Possible edges
        # ----------------------------------------------------

        if user_count <= 1:

            possible_edges = 1.0

        else:

            possible_edges = (
                user_count *
                (user_count - 1)
                / 2.0
            )

        # ----------------------------------------------------
        # Density
        # ----------------------------------------------------

        density = (
            edge_count /
            possible_edges
        )

        # ----------------------------------------------------
        # Average repetition
        # ----------------------------------------------------

        if edge_count > 0:

            average_repetition = (
                total_repeated_groups /
                float(edge_count)
            )

        else:

            average_repetition = 0.0

        # ----------------------------------------------------
        # Size signal
        # ----------------------------------------------------

        size_signal = min(
            user_count /
            SIZE_NORMALIZATION,
            1.0
        )

        # ----------------------------------------------------
        # Repetition signal
        # ----------------------------------------------------

        repetition_signal = min(
            average_repetition /
            REPETITION_NORMALIZATION,
            1.0
        )

        # ----------------------------------------------------
        # Coordination score
        # ----------------------------------------------------

        coordination_score = (
            SIZE_WEIGHT * size_signal
            +
            DENSITY_WEIGHT * density
            +
            REPETITION_WEIGHT * repetition_signal
        )

        return Row(
            component_id=str(component_id),
            user_count=int(user_count),
            edge_count=int(edge_count),
            total_repeated_groups=int(
                total_repeated_groups
            ),
            density=float(density),
            average_repetition=float(
                average_repetition
            ),
            size_signal=float(size_signal),
            repetition_signal=float(
                repetition_signal
            ),
            coordination_score=float(
                coordination_score
            )
        )

    scored_components = (
        component_data
        .map(calculate_score)
    )

    # ========================================================
    # 21. Sort Components
    # ========================================================

    print()
    print(
        "Step 15: Sorting coordination groups..."
    )

    top_components = (
        scored_components
        .sortBy(
            lambda x:
                x.coordination_score,
            ascending=False
        )
        .take(TOP_N)
    )

    # ========================================================
    # 22. Display Results
    # ========================================================

    print()
    print("=" * 70)
    print(
        "TOP {} COORDINATION GROUPS".format(
            TOP_N
        )
    )
    print("=" * 70)

    if len(top_components) == 0:

        print(
            "No coordination groups were found."
        )

    else:

        for index, component in enumerate(
            top_components,
            start=1
        ):

            print()
            print(
                "Group #{}".format(index)
            )

            print(
                "Component ID: {}"
                .format(component.component_id)
            )

            print(
                "Users: {}"
                .format(component.user_count)
            )

            print(
                "Edges: {}"
                .format(component.edge_count)
            )

            print(
                "Repeated Groups: {}"
                .format(
                    component.total_repeated_groups
                )
            )

            print(
                "Density: {:.4f}"
                .format(component.density)
            )

            print(
                "Average Repetition: {:.4f}"
                .format(
                    component.average_repetition
                )
            )

            print(
                "Size Signal: {:.4f}"
                .format(component.size_signal)
            )

            print(
                "Repetition Signal: {:.4f}"
                .format(
                    component.repetition_signal
                )
            )

            print(
                "Coordination Score: {:.4f}"
                .format(
                    component.coordination_score
                )
            )

    # ========================================================
    # 23. Save Enriched Reviews
    # ========================================================

    print()
    print(
        "Step 16: Saving enriched reviews..."
    )

    enriched_df = sql_context.createDataFrame(
        enriched_rdd
    )

    write_parquet(
        enriched_df,
        OUTPUT_PATH
    )

    # ========================================================
    # Finish
    # ========================================================

    print()
    print("=" * 70)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)

    print()
    print(
        "Output directory:"
    )

    print(OUTPUT_PATH)

    # ========================================================
    # Stop Spark
    # ========================================================

    sc.stop()


# ============================================================
# Program Entry Point
# ============================================================

if __name__ == "__main__":
    main()
