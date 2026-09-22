"""
CSC 7740 Project: Identifying Coordinated Amazon Review Groups

Python/PySpark equivalent of the supplied Java Spark pipeline.

Default files:
    /home/training/workspace/All_Beauty.jsonl
    /home/training/workspace/meta_All_Beauty.jsonl

Output:
    /home/training/workspace/amazon_output
"""

import sys
import re
from itertools import combinations

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row


# ============================================================
# DEFAULT PATHS
# ============================================================

DEFAULT_REVIEWS_PATH = (
    "/home/training/workspace/All_Beauty.jsonl"
)

DEFAULT_METADATA_PATH = (
    "/home/training/workspace/meta_All_Beauty.jsonl"
)

DEFAULT_OUTPUT_PATH = (
    "/home/training/workspace/amazon_output"
)


# ============================================================
# PARAMETERS
# ============================================================

INITIAL_BUCKET_HOURS = 24
SMALL_BUCKET_HOURS = 6
OVERSIZED_GROUP_LIMIT = 100
MIN_REPEATED_GROUPS = 3
MIN_TEXT_LENGTH = 30
COMPONENT_ITERATIONS = 20


# ============================================================
# ROW HELPERS
# ============================================================

def get_value(row, *fields):

    for field in fields:

        try:

            if isinstance(row, dict):
                value = row.get(field)

            else:
                value = row[field]

        except Exception:
            value = None

        if value is not None:
            return value

    return None


def get_float(row, fields, default_value=0.0):

    value = get_value(row, *fields)

    if value is None:
        return default_value

    try:
        return float(value)

    except Exception:
        return default_value


def get_int(row, fields, default_value=0):

    value = get_value(row, *fields)

    if value is None:
        return default_value

    try:
        return int(value)

    except Exception:

        try:
            return int(float(value))

        except Exception:
            return default_value


def get_long(row, fields, default_value=0):

    value = get_value(row, *fields)

    if value is None:
        return default_value

    try:
        return int(value)

    except Exception:

        try:
            return int(float(value))

        except Exception:
            return default_value


def get_boolean(row, fields, default_value=False):

    value = get_value(row, *fields)

    if value is None:
        return default_value

    if isinstance(value, bool):
        return value

    return str(value).lower() == "true"


# ============================================================
# CLEAN REVIEW TEXT
# ============================================================

def clean_text(text):

    if text is None:
        return ""

    cleaned = str(text).lower()

    cleaned = re.sub(
        r"[^a-z0-9\s]",
        " ",
        cleaned
    )

    cleaned = re.sub(
        r"\s+",
        " ",
        cleaned
    )

    return cleaned.strip()


# ============================================================
# TIME BUCKET
# ============================================================

def get_time_bucket(timestamp, hours):

    seconds_per_bucket = (
        hours * 60 * 60
    )

    return int(timestamp) // seconds_per_bucket


# ============================================================
# PARSE REVIEW
# ============================================================

def parse_review(row):

    try:

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
            ("rating",),
            0.0
        )

        timestamp = get_long(
            row,
            ("timestamp",),
            0
        )

        verified_purchase = get_boolean(
            row,
            (
                "verified_purchase",
                "verifiedPurchase"
            ),
            False
        )

        helpful_votes = get_int(
            row,
            (
                "helpful_vote",
                "helpful_votes"
            ),
            0
        )

        # Convert milliseconds to seconds.
        if timestamp > 100000000000:

            timestamp = timestamp // 1000

        if user_id is None:
            return None

        if product_id is None:
            return None

        if timestamp <= 0:
            return None

        return {
            "user_id": str(user_id),
            "product_id": str(product_id),
            "rating": rating,
            "timestamp": timestamp,
            "review_text": clean_text(
                review_text
            ),
            "verified_purchase": verified_purchase,
            "helpful_votes": helpful_votes
        }

    except Exception:

        return None


# ============================================================
# PARSE PRODUCT METADATA
# ============================================================

def parse_metadata(row):

    product_id = get_value(
        row,
        "parent_asin",
        "asin",
        "product_id"
    )

    if product_id is None:
        return None

    category = get_value(
        row,
        "main_category",
        "category"
    )

    title = get_value(
        row,
        "title"
    )

    average_rating = get_float(
        row,
        ("average_rating",),
        0.0
    )

    return {
        "product_id": str(product_id),
        "category": (
            None
            if category is None
            else str(category)
        ),
        "product_title": (
            None
            if title is None
            else str(title)
        ),
        "catalog_average_rating": (
            average_rating
        )
    }


# ============================================================
# ENRICH REVIEW
# ============================================================

def make_enriched(review, metadata):

    result = dict(review)

    result["category"] = (
        metadata["category"]
    )

    result["product_title"] = (
        metadata["product_title"]
    )

    result["catalog_average_rating"] = (
        metadata["catalog_average_rating"]
    )

    return result


# ============================================================
# CONNECTED COMPONENTS
# ============================================================

def connected_components(
        labels,
        edges,
        iterations=COMPONENT_ITERATIONS):

    current = labels

    for iteration in range(iterations):

        print(
            "Connected component iteration %d"
            % (iteration + 1)
        )

        # ----------------------------------------------------
        # Send every edge in both directions.
        # ----------------------------------------------------

        neighbor_requests = (
            edges
            .flatMap(
                lambda item: [
                    (
                        item[1][0],
                        item[1][1]
                    ),
                    (
                        item[1][1],
                        item[1][0]
                    )
                ]
            )
        )

        # ----------------------------------------------------
        # Obtain current label of every neighbor.
        # ----------------------------------------------------

        neighbor_labels = (
            neighbor_requests.join(current)
        )

        # ----------------------------------------------------
        # user -> neighbor component label
        # ----------------------------------------------------

        proposals = neighbor_labels.map(
            lambda item: (
                item[0],
                item[1][1]
            )
        )

        # ----------------------------------------------------
        # Select smallest component label.
        # ----------------------------------------------------

        minimum_labels = (
            proposals
            .reduceByKey(
                lambda a, b:
                a if a <= b else b
            )
        )

        # ----------------------------------------------------
        # Compare current and proposed labels.
        # ----------------------------------------------------

        combined = (
            current.join(minimum_labels)
        )

        updated = combined.map(
            lambda item: (
                item[0],
                (
                    item[1][0]
                    if item[1][0] <= item[1][1]
                    else item[1][1]
                )
            )
        )

        # ----------------------------------------------------
        # Preserve vertices without neighbors.
        # ----------------------------------------------------

        unchanged = (
            current.subtractByKey(updated)
        )

        current = (
            updated
            .union(unchanged)
            .reduceByKey(
                lambda a, b:
                a if a <= b else b
            )
        )

    return current


# ============================================================
# WRITE PARQUET
# ============================================================

def write_parquet(
        sql_context,
        enriched_reviews,
        output_path):

    rows = enriched_reviews.map(
        lambda value: Row(
            user_id=value["user_id"],
            product_id=value["product_id"],
            rating=float(value["rating"]),
            timestamp=int(value["timestamp"]),
            review_text=value["review_text"],
            verified_purchase=bool(
                value["verified_purchase"]
            ),
            helpful_votes=int(
                value["helpful_votes"]
            ),
            category=value["category"],
            product_title=value["product_title"],
            catalog_average_rating=float(
                value[
                    "catalog_average_rating"
                ]
            )
        )
    )

    output = (
        sql_context
        .createDataFrame(rows)
    )

    # Spark 1.3 API.
    output.saveAsParquetFile(
        output_path
    )


# ============================================================
# MAIN
# ============================================================

def main():

    reviews_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else DEFAULT_REVIEWS_PATH
    )

    metadata_path = (
        sys.argv[2]
        if len(sys.argv) > 2
        else DEFAULT_METADATA_PATH
    )

    output_path = (
        sys.argv[3]
        if len(sys.argv) > 3
        else DEFAULT_OUTPUT_PATH
    )

    # ========================================================
    # SPARK CONFIGURATION
    # ========================================================

    conf = (
        SparkConf()
        .setAppName(
            "Amazon Review Coordination Analysis"
        )
        .setMaster("local[*]")
    )

    sc = SparkContext(
        conf=conf
    )

    sql_context = SQLContext(sc)

    try:

        print()
        print(
            "Starting Amazon Review Analysis..."
        )

        print(
            "Reviews: %s"
            % reviews_path
        )

        print(
            "Metadata: %s"
            % metadata_path
        )

        print(
            "Output: %s"
            % output_path
        )

        # ====================================================
        # 1. LOAD REVIEWS
        # ====================================================

        print()
        print(
            "1. Loading reviews..."
        )

        review_df = (
            sql_context
            .jsonFile(reviews_path)
        )

        review_rows = (
            review_df.rdd
        )

        # ====================================================
        # 2. LOAD METADATA
        # ====================================================

        print(
            "2. Loading product metadata..."
        )

        metadata_df = (
            sql_context
            .jsonFile(metadata_path)
        )

        metadata_rows = (
            metadata_df.rdd
        )

        # ====================================================
        # 3. PARSE AND CLEAN REVIEWS
        # ====================================================

        print(
            "3. Parsing and cleaning reviews..."
        )

        reviews = (
            review_rows
            .map(parse_review)
            .filter(
                lambda review:
                review is not None
            )
        )

        # ====================================================
        # 4. PARSE PRODUCT METADATA
        # ====================================================

        print(
            "4. Parsing product metadata..."
        )

        metadata_by_product = (
            metadata_rows
            .map(parse_metadata)
            .filter(
                lambda metadata:
                metadata is not None
            )
            .map(
                lambda metadata: (
                    metadata["product_id"],
                    metadata
                )
            )
        )

        # ====================================================
        # 5. JOIN REVIEWS + METADATA
        # ====================================================

        print(
            "5. Joining reviews with metadata..."
        )

        reviews_by_product = (
            reviews
            .map(
                lambda review: (
                    review["product_id"],
                    review
                )
            )
        )

        joined = (
            reviews_by_product
            .join(metadata_by_product)
        )

        enriched_reviews = (
            joined
            .map(
                lambda item:
                make_enriched(
                    item[1][0],
                    item[1][1]
                )
            )
        )

        # ====================================================
        # 6. SAVE ENRICHED DATA
        # ====================================================

        print(
            "6. Writing enriched data..."
        )

        write_parquet(
            sql_context,
            enriched_reviews,
            output_path
        )

        # ====================================================
        # 7. CREATE PRODUCT-TIME GROUPS
        # ====================================================

        print(
            "7. Creating 24-hour product-time groups..."
        )

        product_time = (
            enriched_reviews
            .map(
                lambda review: (
                    (
                        review["product_id"],
                        get_time_bucket(
                            review["timestamp"],
                            INITIAL_BUCKET_HOURS
                        )
                    ),
                    review
                )
            )
        )

        grouped = (
            product_time
            .groupByKey()
        )

        # ====================================================
        # 8. SPLIT OVERSIZED GROUPS
        # ====================================================

        print(
            "8. Splitting oversized groups..."
        )

        def split_group(item):

            group_key = item[0]

            reviews_list = list(
                item[1]
            )

            # Normal 24-hour group.
            if len(reviews_list) <= (
                    OVERSIZED_GROUP_LIMIT):

                return [
                    (
                        group_key,
                        review
                    )
                    for review in reviews_list
                ]

            # Oversized group:
            # use 6-hour buckets.
            output = []

            for review in reviews_list:

                bucket = (
                    get_time_bucket(
                        review["timestamp"],
                        SMALL_BUCKET_HOURS
                    )
                )

                new_key = (
                    review["product_id"],
                    bucket
                )

                output.append(
                    (
                        new_key,
                        review
                    )
                )

            return output

        candidate_reviews = (
            grouped
            .flatMap(split_group)
        )

        candidate_groups = (
            candidate_reviews
            .groupByKey()
        )

        # ====================================================
        # 9. GENERATE ACCOUNT PAIRS
        # ====================================================

        print(
            "9. Generating account pairs..."
        )

        def generate_pairs(item):

            group_key = item[0]

            unique_users = set()

            for review in item[1]:

                user_id = (
                    review.get("user_id")
                )

                if user_id is not None:
                    unique_users.add(
                        user_id
                    )

            users = sorted(
                unique_users
            )

            result = []

            for user_a, user_b in combinations(
                    users,
                    2):

                pair_key = (
                    user_a,
                    user_b
                )

                pair_record = (
                    user_a,
                    user_b,
                    group_key
                )

                result.append(
                    (
                        pair_key,
                        pair_record
                    )
                )

            return result

        pair_records = (
            candidate_groups
            .flatMap(generate_pairs)
        )

        # ====================================================
        # 10. FIND REPEATED ACCOUNT PAIRS
        # ====================================================

        print(
            "10. Finding repeated account pairs..."
        )

        pair_groups = (
            pair_records
            .groupByKey()
        )

        def create_edge(item):

            pair_key = item[0]

            groups = set()

            user_a = None
            user_b = None

            for record in item[1]:

                user_a = record[0]
                user_b = record[1]

                groups.add(
                    record[2]
                )

            edge = (
                user_a,
                user_b,
                len(groups)
            )

            return (
                pair_key,
                edge
            )

        edges = (
            pair_groups
            .map(create_edge)
            .filter(
                lambda item:
                item[1][2]
                >= MIN_REPEATED_GROUPS
            )
        )

        # ====================================================
        # 11. CREATE GRAPH VERTICES
        # ====================================================

        print(
            "11. Creating graph vertices..."
        )

        vertices = (
            edges
            .flatMap(
                lambda item: [
                    item[1][0],
                    item[1][1]
                ]
            )
            .distinct()
        )

        initial_labels = (
            vertices
            .map(
                lambda user: (
                    user,
                    user
                )
            )
        )

        # ====================================================
        # 12. CONNECTED COMPONENTS
        # ====================================================

        print(
            "12. Finding connected components..."
        )

        components = (
            connected_components(
                initial_labels,
                edges
            )
        )

        # ====================================================
        # 13. COUNT USERS PER COMPONENT
        # ====================================================

        print(
            "13. Counting users per component..."
        )

        component_sizes = (
            components
            .map(
                lambda item: (
                    item[1],
                    1
                )
            )
            .reduceByKey(
                lambda a, b:
                a + b
            )
        )

        # ====================================================
        # 14. ASSIGN EDGES TO COMPONENTS
        # ====================================================

        print(
            "14. Assigning edges to components..."
        )

        edges_by_user = (
            edges
            .map(
                lambda item: (
                    item[1][0],
                    item[1]
                )
            )
        )

        edge_components = (
            edges_by_user
            .join(components)
        )

        # ====================================================
        # 15. AGGREGATE COMPONENT METRICS
        # ====================================================

        print(
            "15. Aggregating component metrics..."
        )

        def edge_metric(item):

            edge = item[1][0]

            component = item[1][1]

            return (
                component,
                (
                    1,
                    edge[2]
                )
            )

        metrics = (
            edge_components
            .map(edge_metric)
            .reduceByKey(
                lambda a, b: (
                    a[0] + b[0],
                    a[1] + b[1]
                )
            )
        )

        # ====================================================
        # 16. JOIN COMPONENT SIZE + METRICS
        # ====================================================

        print(
            "16. Joining component metrics..."
        )

        final_metrics = (
            metrics
            .join(component_sizes)
        )

        # ====================================================
        # 17. CALCULATE COORDINATION SCORE
        # ====================================================

        print(
            "17. Calculating coordination scores..."
        )

        def calculate_result(item):

            component_id = item[0]

            metric = item[1][0]

            user_count = item[1][1]

            edge_count = metric[0]

            total_repeated_groups = (
                metric[1]
            )

            if user_count <= 1:

                possible_edges = 1.0

            else:

                possible_edges = (
                    float(user_count)
                    * float(user_count - 1)
                ) / 2.0

            density = (
                float(edge_count)
                / possible_edges
            )

            if edge_count == 0:

                average_repetition = 0.0

            else:

                average_repetition = (
                    float(
                        total_repeated_groups
                    )
                    / float(edge_count)
                )

            size_signal = min(
                float(user_count) / 20.0,
                1.0
            )

            repetition_signal = min(
                average_repetition / 10.0,
                1.0
            )

            score = (
                0.35 * size_signal
                + 0.35 * density
                + 0.30 * repetition_signal
            )

            return {
                "component_id":
                    component_id,

                "user_count":
                    user_count,

                "edge_count":
                    edge_count,

                "total_repeated_groups":
                    total_repeated_groups,

                "edge_density":
                    density,

                "average_repetition":
                    average_repetition,

                "coordination_score":
                    score
            }

        results = (
            final_metrics
            .map(calculate_result)
        )

        # ====================================================
        # 18. SORT AND DISPLAY RESULTS
        # ====================================================

        print(
            "18. Collecting and sorting results..."
        )

        result_list = (
            results.collect()
        )

        result_list.sort(
            key=lambda result:
            result["coordination_score"],
            reverse=True
        )

        print()
        print(
            "=========================================="
        )

        print(
            "Amazon Review Coordination Analysis"
        )

        print(
            "=========================================="
        )

        print(
            "Candidate components: %d"
            % len(result_list)
        )

        limit = min(
            20,
            len(result_list)
        )

        for i in range(limit):

            result = result_list[i]

            print(
                "Component=%s | Users=%d | "
                "Edges=%d | RepeatedGroups=%d | "
                "Density=%s | AvgRepetition=%s | "
                "CoordinationScore=%s"
                % (
                    result["component_id"],
                    result["user_count"],
                    result["edge_count"],
                    result[
                        "total_repeated_groups"
                    ],
                    result["edge_density"],
                    result[
                        "average_repetition"
                    ],
                    result[
                        "coordination_score"
                    ]
                )
            )

        print()

        print(
            "Analysis completed successfully."
        )

    finally:

        sc.stop()


# ============================================================
# PROGRAM ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()
