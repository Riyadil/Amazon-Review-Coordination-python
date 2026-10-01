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

        product_id = (
            record.get("product_id")
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
    """

    try:

        product_id = (
            record.get("product_id")
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
        0.20 * size_signal
        +
        0.20 * density
        +
        0.20 * repetition_signal
        +
        0.15 * time_concentration
        +
        0.10 * rating_agreement
        +
        0.15 * average_text_similarity
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
        if len(token) >= 2
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
        .mode("errorifexists")
        .partitionBy(
            "category",
            "year"
        )
        .parquet(
            output_path
        )
    )
