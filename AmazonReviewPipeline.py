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
    TOP_N,

    MIN_TEXT_LENGTH,
    TEXT_NUM_FEATURES,
    MIN_TEXT_SIMILARITY,
    MAX_TEXT_REVIEWS_PER_COMPONENT,
    TEXT_JOIN_THRESHOLD,

    TEXT_OUTPUT_PATH,
    COMPONENT_OUTPUT_PATH,
    SUSPICIOUS_REVIEW_OUTPUT_PATH,
    PERFORMANCE_OUTPUT_PATH
)

from Utils import (
    parse_review,
    parse_metadata,
    make_enriched,
    get_time_bucket,

    write_pair_audit,
    connected_components,
    write_parquet,

    calculate_time_concentration,
    calculate_rating_agreement,
    calculate_verified_purchase_rate,
    calculate_helpful_vote_average,
    calculate_average_similarity,
    calculate_combined_coordination_score,
    tokenize_review_text
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
        user_count / 20.0,
        1.0
    )

    repetition_signal = min(
        average_repetition / 10.0,
        1.0
    )

    coordination_score = (
        0.35 * size_signal
        +
        0.35 * density
        +
        0.30 * repetition_signal
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
        .filter(
            col("tokens").isNotNull()
        )
    )


    # --------------------------------------------------------
    # HashingTF.
    # --------------------------------------------------------

    hashing_tf = HashingTF(
        inputCol="tokens",
        outputCol="features",
        numFeatures=TEXT_NUM_FEATURES
    )


    feature_df = hashing_tf.transform(
        text_reviews_df
    )


    # --------------------------------------------------------
    # MinHash.
    # --------------------------------------------------------

    minhash = MinHashLSH(
        inputCol="features",
        outputCol="hashes",
        numHashTables=3
    )


    minhash_model = minhash.fit(
        feature_df
    )


    # --------------------------------------------------------
    # Similarity join.
    #
    # The component comparison MUST happen before the nested
    # dataset columns are flattened by select().
    # --------------------------------------------------------

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
        .mode("errorifexists")
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
        .mode("errorifexists")
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
            .mode("errorifexists")
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
        .mode("errorifexists")
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
