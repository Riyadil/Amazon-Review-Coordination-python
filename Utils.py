# ============================================================
# Utils.py
# ============================================================
# Helper functions for Amazon Review Coordination Analysis
# ============================================================

import csv
import math
import re

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Tuple

from pyspark.sql import Row


# ============================================================
# Review Year
# ============================================================

def get_review_year(timestamp: int) -> int:
    """
    Extract the UTC year from a timestamp already in seconds.
    """

    if isinstance(timestamp, bool) or not isinstance(timestamp, int):
        raise ValueError(
            "Timestamp must be an integer in seconds."
        )

    return datetime.fromtimestamp(
        timestamp,
        timezone.utc
    ).year


# ============================================================
# Rating Deviation
# ============================================================

def get_rating_deviation(
    rating: Optional[float],
    catalog_average_rating: Optional[float]
) -> Optional[float]:
    """
    Return absolute rating deviation.

    Returns None if either rating is missing.
    """

    for value in (
        rating,
        catalog_average_rating
    ):

        if value is not None and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not 1.0 <= value <= 5.0
        ):
            raise ValueError(
                "Ratings must be finite numbers from 1 to 5."
            )

    if (
        rating is None
        or catalog_average_rating is None
    ):
        return None

    return abs(
        float(rating)
        -
        float(catalog_average_rating)
    )


# ============================================================
# Text Eligibility
# ============================================================

def is_text_eligible(
    text: str,
    minimum_length: int
) -> bool:
    """
    Check whether cleaned review text is long enough
    for text similarity analysis.
    """

    if not isinstance(text, str):
        raise ValueError(
            "Review text must be a cleaned string."
        )

    if (
        isinstance(minimum_length, bool)
        or not isinstance(minimum_length, int)
        or minimum_length < 1
    ):
        raise ValueError(
            "Minimum text length must be a positive integer."
        )

    return len(text) >= minimum_length


# ============================================================
# Reviewer ID Audit
# ============================================================

def get_audit_base(user_id: str) -> str:
    """
    Remove trailing numeric suffixes only for audit comparison.
    """

    if (
        not isinstance(user_id, str)
        or not user_id.strip()
    ):
        raise ValueError(
            "Reviewer ID must be a non-empty string."
        )

    base_id = re.sub(
        r"(?:_[0-9]+)+$",
        "",
        user_id
    )

    if not base_id.strip():
        raise ValueError(
            "Reviewer ID has no base before its suffix."
        )

    return base_id


# ============================================================
# Save Pair Audit
# ============================================================

def write_pair_audit(
    edges: Iterable[Tuple[str, str, int]],
    output_path: str
) -> int:
    """
    Write repeated graph edges to CSV.

    Returns the number of pairs that may contain
    reviewer-ID suffix artifacts.
    """

    path = Path(output_path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    flagged_pair_count = 0

    with path.open(
        "x",
        newline="",
        encoding="utf-8"
    ) as output_file:

        writer = csv.writer(output_file)

        writer.writerow(
            [
                "user1",
                "user2",
                "repeated_group_count",
                "audit_base1",
                "audit_base2",
                "possible_suffix_artifact"
            ]
        )

        for (
            user1,
            user2,
            repetition_count
        ) in edges:

            base1 = get_audit_base(user1)
            base2 = get_audit_base(user2)

            if user1 == user2:
                raise ValueError(
                    "A graph edge must contain different users."
                )

            if (
                isinstance(repetition_count, bool)
                or not isinstance(repetition_count, int)
                or repetition_count < 1
            ):
                raise ValueError(
                    "Repeated group count must be positive."
                )

            possible_artifact = (
                base1 == base2
            )

            if possible_artifact:
                flagged_pair_count += 1

            writer.writerow(
                [
                    user1,
                    user2,
                    repetition_count,
                    base1,
                    base2,
                    possible_artifact
                ]
            )

    return flagged_pair_count


# ============================================================
# Generic Row Access
# ============================================================

def get_value(row, *keys):
    """
    Return the first non-null value found.
    """

    for key in keys:

        try:

            value = row[key]

            if value is not None:
                return value

        except Exception:
            pass

    return None


# ============================================================
# Safe Float
# ============================================================

def get_float(row, *keys):
    """
    Safely convert a value to float.
    """

    value = get_value(
        row,
        *keys
    )

    if value is None:
        return None

    try:
        return float(value)

    except Exception:
        return None


# ============================================================
# Safe Integer
# ============================================================

def get_int(row, *keys):
    """
    Safely convert a value to integer.
    """

    value = get_value(
        row,
        *keys
    )

    if value is None:
        return 0

    try:

        return int(value)

    except Exception:

        try:
            return int(float(value))

        except Exception:
            return 0


# ============================================================
# Safe Long
# ============================================================

def get_long(row, *keys):
    """
    Safely convert a timestamp to integer.
    """

    value = get_value(
        row,
        *keys
    )

    if value is None:
        return None

    try:

        return int(value)

    except Exception:

        try:
            return int(float(value))

        except Exception:
            return None


# ============================================================
# Safe Boolean
# ============================================================

def get_boolean(row, *keys):
    """
    Safely convert a value to boolean.
    """

    value = get_value(
        row,
        *keys
    )

    if value is None:
        return False

    if isinstance(value, bool):
        return value

    if isinstance(value, str):

        value = value.strip().lower()

        if value in (
            "true",
            "1",
            "yes"
        ):
            return True

        if value in (
            "false",
            "0",
            "no"
        ):
            return False

    try:
        return bool(value)

    except Exception:
        return False


# ============================================================
# Clean Text
# ============================================================

def clean_text(text):
    """
    Clean review text.
    """

    if text is None:
        return ""

    text = str(text)

    # Remove HTML tags
    text = re.sub(
        r"<[^>]+>",
        " ",
        text
    )

    # Normalize whitespace
    text = re.sub(
        r"\s+",
        " ",
        text
    )

    return text.strip()


# ============================================================
# Time Bucket
# ============================================================

def get_time_bucket(
    timestamp,
    bucket_hours
):
    """
    Convert timestamp to a time bucket.

    Timestamp is expected to be in seconds.
    """

    if timestamp is None:
        return None

    bucket_seconds = (
        bucket_hours
        * 60
        * 60
    )

    return (
        timestamp // bucket_seconds
    ) * bucket_seconds


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

    # Convert milliseconds to seconds.
    if (
        timestamp is not None
        and timestamp > 100000000000
    ):
        timestamp = timestamp // 1000

    review_text = clean_text(
        review_text
    )

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
        category=(
            str(category)
            if category is not None
            else ""
        ),
        product_title=(
            str(title)
            if title is not None
            else ""
        ),
        catalog_average_rating=average_rating
    )


# ============================================================
# Enrichment
# ============================================================

def make_enriched(
    review: Row,
    metadata: Row
) -> Row:
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

        category=(
            metadata.category
            if metadata
            else ""
        ),

        product_title=(
            metadata.product_title
            if metadata
            else ""
        ),

        catalog_average_rating=(
            metadata.catalog_average_rating
            if metadata
            else None
        ),

        year=get_review_year(
            review.timestamp
        ),

        rating_deviation=get_rating_deviation(
            review.rating,
            (
                metadata.catalog_average_rating
                if metadata
                else None
            )
        ),

        text_eligible=is_text_eligible(
            review.review_text,
            30
        )
    )


# ============================================================
# Connected Components
# ============================================================

def connected_components(
    vertices,
    edges,
    iterations
):
    """
    Find connected components using iterative
    minimum-label propagation.
    """

    labels = vertices.map(
        lambda v: (v, v)
    )

    directed_edges = edges.flatMap(
        lambda edge: [
            (edge[0], edge[1]),
            (edge[1], edge[0])
        ]
    )

    for iteration in range(iterations):

        joined = directed_edges.join(
            labels
        )

        proposals = joined.map(
            lambda x: (
                x[1][0],
                x[1][1]
            )
        )

        own_labels = labels.map(
            lambda x: (
                x[0],
                x[1]
            )
        )

        proposed_labels = (
            proposals
            .union(own_labels)
            .reduceByKey(min)
        )

        labels = proposed_labels

        print(
            "Connected components "
            "iteration {}/{}".format(
                iteration + 1,
                iterations
            )
        )

    return labels


# ============================================================
# Component Score
# ============================================================

def calculate_coordination_score(
    component_id,
    statistics,
    user_count,
    size_weight,
    density_weight,
    repetition_weight,
    size_normalization,
    repetition_normalization
):
    """
    Calculate coordination score for one component.
    """

    edge_count = statistics[0]

    total_repeated_groups = statistics[1]

    if user_count <= 1:

        possible_edges = 1.0

    else:

        possible_edges = (
            user_count
            * (user_count - 1)
            / 2.0
        )

    density = (
        edge_count
        / possible_edges
    )

    if edge_count > 0:

        average_repetition = (
            total_repeated_groups
            / float(edge_count)
        )

    else:

        average_repetition = 0.0

    size_signal = min(
        user_count
        / size_normalization,
        1.0
    )

    repetition_signal = min(
        average_repetition
        / repetition_normalization,
        1.0
    )

    coordination_score = (
        size_weight * size_signal
        +
        density_weight * density
        +
        repetition_weight * repetition_signal
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


# ============================================================
# Print Component
# ============================================================

def print_component(
    component,
    index
):
    """
    Print one coordination group.
    """

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


# ============================================================
# Save DataFrame as Parquet
# ============================================================

def write_parquet(
    df,
    output_path
):
    """
    Save DataFrame as partitioned Parquet.
    """

    print(
        "Writing Parquet output:"
    )

    print(output_path)

    (
        df.write
        .mode("errorifexists")
        .partitionBy(
            "category",
            "year"
        )
        .parquet(
            output_path
        )
    )

    print(
        "Parquet output successfully written."
    )
