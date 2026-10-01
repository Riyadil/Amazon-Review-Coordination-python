# ============================================================
# AmazonReviewPipeline.py
# ============================================================
#
# Identifying Coordinated Amazon Review Groups
# with Distributed Text and Graph Analysis
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
#
# Configuration and helper functions are included in separate files.
# ============================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, BooleanType, IntegerType
)

from Config import (
    APP_NAME,
    MASTER,
    DRIVER_MEMORY,
    REVIEWS_PATH,
    METADATA_PATH,
    OUTPUT_PATH,
    PAIR_AUDIT_PATH,
    TIME_BUCKET_HOURS,
    LARGE_GROUP_THRESHOLD,
    LARGE_GROUP_BUCKET_HOURS,
    MIN_REPEATED_GROUPS,
    LABEL_PROPAGATION_ITERATIONS,
    TOP_N
)

from Utils import (
    parse_review,
    parse_metadata,
    make_enriched,
    get_time_bucket,
    write_pair_audit,
    connected_components,
    write_parquet
)


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
    # STEP 1: Loads Amazon reviews and metadata
    # ========================================================

    # --------------------------------------------------------
    # Create Spark Context
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Load Reviews
    # --------------------------------------------------------

    print()
    print("Loading reviews...")

    reviews_df = sql_context.read.json(
        REVIEWS_PATH
    )

    print(
        "Review records loaded: {}".format(
            reviews_df.count()
        )
    )

    # --------------------------------------------------------
    # Load Product Metadata
    # --------------------------------------------------------

    print()
    print("Loading product metadata...")

    metadata_df = sql_context.read.json(
        METADATA_PATH
    )

    print(
        "Metadata records loaded: {}".format(
            metadata_df.count()
        )
    )


    # ========================================================
    # STEP 2: Cleans and enriches review data
    # ========================================================

    # --------------------------------------------------------
    # Parse Reviews
    # --------------------------------------------------------

    print()
    print("Parsing reviews...")

    parsed_reviews = (
        reviews_df.rdd
        .map(parse_review)
        .filter(lambda x: x is not None)
    )

    print(
        "Valid reviews: {}".format(
            parsed_reviews.count()
        )
    )

    # --------------------------------------------------------
    # Parse Product Metadata
    # --------------------------------------------------------

    print()
    print("Parsing metadata...")

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

    # --------------------------------------------------------
    # Prepare Metadata for Join
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Join Reviews with Metadata
    # --------------------------------------------------------

    print()
    print("Joining reviews with metadata...")

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
    # STEP 3: Groups reviews into time buckets
    # ========================================================

    # --------------------------------------------------------
    # Create 24-hour Time Buckets
    # --------------------------------------------------------

    print()
    print("Creating 24-hour time buckets...")

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

    # --------------------------------------------------------
    # Group Reviews by Product + Time
    # --------------------------------------------------------

    grouped_reviews = (
        bucketed_reviews
        .groupByKey()
        .mapValues(list)
    )

    # --------------------------------------------------------
    # Split Oversized Groups
    # --------------------------------------------------------

    print()
    print(
        "Splitting groups larger than {} reviews...".format(
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
    # STEP 4: Identifies repeated user pairs
    # ========================================================

    # --------------------------------------------------------
    # Generate User Pairs
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Count Repeated Product-Time Groups
    # --------------------------------------------------------

    print()
    print("Finding repeated user pairs...")

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
    # STEP 5: Builds a user coordination graph
    # ========================================================

    # --------------------------------------------------------
    # Build Graph Edges
    # --------------------------------------------------------

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
    )

    # --------------------------------------------------------
    # Build Graph Vertices
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Audit Repeated Pairs for Possible Reviewer-ID Artifacts
    # --------------------------------------------------------

    flagged_pair_count = write_pair_audit(
        graph_edges.toLocalIterator(),
        PAIR_AUDIT_PATH
    )

    print(
        "Pairs with possible ID suffix artifacts: {}".format(
            flagged_pair_count
        )
    )

    print(
        "Pair audit: {}".format(
            PAIR_AUDIT_PATH
        )
    )


    # ========================================================
    # STEP 6: Finds connected components using label propagation
    # ========================================================

    print()
    print(
        "Finding connected components..."
    )

    # --------------------------------------------------------
    # Create Simple Graph Edges
    # --------------------------------------------------------

    simple_edges = graph_edges.map(
        lambda x: (
            x[0],
            x[1]
        )
    )

    # --------------------------------------------------------
    # Run Connected Components
    # --------------------------------------------------------

    labels = connected_components(
        vertices,
        simple_edges,
        LABEL_PROPAGATION_ITERATIONS
    )

    # labels:
    # user_id -> component_id

    # --------------------------------------------------------
    # Count Users in Each Component
    # --------------------------------------------------------

    print()
    print("Counting users per component...")

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

    # --------------------------------------------------------
    # Assign Edges to Components
    # --------------------------------------------------------

    print()
    print(
        "Aggregating coordination groups..."
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
    # STEP 7: Calculates coordination scores
    # ========================================================

    # --------------------------------------------------------
    # Aggregate Component Statistics
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Join Component Sizes
    # --------------------------------------------------------

    component_data = (
        component_statistics
        .join(component_sizes)
    )

    # --------------------------------------------------------
    # Calculate Coordination Score
    # --------------------------------------------------------

    print()
    print(
        "Calculating coordination scores..."
    )

    def calculate_score(record):

        component_id, data = record

        statistics, user_count = data

        edge_count = statistics[0]

        total_repeated_groups = statistics[1]

        # ----------------------------------------------------
        # Possible Edges
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
        # Average Repetition
        # ----------------------------------------------------

        if edge_count > 0:

            average_repetition = (
                total_repeated_groups /
                float(edge_count)
            )

        else:

            average_repetition = 0.0

        # ----------------------------------------------------
        # Size Signal
        # ----------------------------------------------------

        size_signal = min(
            user_count /
            SIZE_NORMALIZATION,
            1.0
        )

        # ----------------------------------------------------
        # Repetition Signal
        # ----------------------------------------------------

        repetition_signal = min(
            average_repetition /
            REPETITION_NORMALIZATION,
            1.0
        )

        # ----------------------------------------------------
        # Coordination Score
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

    # --------------------------------------------------------
    # Sort Components
    # --------------------------------------------------------

    print()
    print(
        "Sorting coordination groups..."
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

    # --------------------------------------------------------
    # Display Results
    # --------------------------------------------------------

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
    # STEP 8: Saves enriched data as Parquet by category and year
    # ========================================================

    print()
    print(
        "Saving enriched reviews..."
    )

    # --------------------------------------------------------
    # Define Enriched Review Schema
    # --------------------------------------------------------

    enriched_schema = StructType(
        [
            StructField("user_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("rating", DoubleType(), True),
            StructField("timestamp", LongType(), False),
            StructField("review_text", StringType(), False),
            StructField("verified_purchase", BooleanType(), False),
            StructField("helpful_votes", LongType(), False),
            StructField("category", StringType(), False),
            StructField("product_title", StringType(), False),
            StructField("catalog_average_rating", DoubleType(), True),
            StructField("year", IntegerType(), False),
            StructField("rating_deviation", DoubleType(), True),
            StructField("text_eligible", BooleanType(), False)
        ]
    )

    # Match values to schema names explicitly,
    # regardless of Row order.
    enriched_columns = enriched_schema.fieldNames()

    enriched_df = sql_context.createDataFrame(
        enriched_rdd.map(
            lambda review:
                tuple(
                    review[name]
                    for name in enriched_columns
                )
        ),
        enriched_schema
    )

    # --------------------------------------------------------
    # Write Partitioned Parquet Output
    # --------------------------------------------------------

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
