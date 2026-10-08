"""Create a small, joinable sample for the final submission ZIP."""

import glob
import gzip
import json
import os
from pathlib import Path

REVIEWS_GLOB = os.environ.get(
    "REVIEWS_PATH", "data/reviews/Video_Games.jsonl.gz")
METADATA_GLOB = os.environ.get(
    "METADATA_PATH", "data/meta/meta_Video_Games.jsonl.gz")
OUTPUT_DIR = Path(os.environ.get("SAMPLE_OUTPUT_DIR", "sample_data"))
MAX_REVIEWS = int(os.environ.get("MAX_SAMPLE_REVIEWS", "5000"))
MAX_TOTAL_BYTES = 10 * 1024 * 1024
SEED_USERS = {user for user in os.environ.get("SEED_USERS", "").split(",") if user}


def read_review_sample():
    lines = []
    parent_asins = set()
    seen_seed_users = set()
    base_reviews = 0
    paths = sorted(glob.glob(REVIEWS_GLOB))
    if not paths:
        raise FileNotFoundError("No review files match %s" % REVIEWS_GLOB)
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            for line in source:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                user_id = row.get("user_id")
                parent_asin = row.get("parent_asin")
                if not user_id or not parent_asin or row.get("timestamp") is None:
                    continue
                is_seed = user_id in SEED_USERS
                if base_reviews >= MAX_REVIEWS and not is_seed:
                    continue
                lines.append(line)
                parent_asins.add(parent_asin)
                if is_seed:
                    seen_seed_users.add(user_id)
                else:
                    base_reviews += 1
    missing = SEED_USERS - seen_seed_users
    if missing:
        raise RuntimeError("Seed users not found: %s" % sorted(missing))
    return lines, parent_asins, seen_seed_users


def read_matching_metadata(parent_asins):
    lines = []
    found = set()
    paths = sorted(glob.glob(METADATA_GLOB))
    if not paths:
        raise FileNotFoundError("No metadata files match %s" % METADATA_GLOB)
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            for line in source:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                parent_asin = row.get("parent_asin")
                if parent_asin in parent_asins and parent_asin not in found:
                    lines.append(line)
                    found.add(parent_asin)
                    if found == parent_asins:
                        return lines, found
    return lines, found


def write_gzip(path, lines):
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=9) as target:
        target.writelines(lines)


def main():
    review_lines, parent_asins, seen_seed_users = read_review_sample()
    metadata_lines, found = read_matching_metadata(parent_asins)
    missing_metadata = parent_asins - found
    if not review_lines or not metadata_lines or missing_metadata:
        raise RuntimeError(
            "Sample is not fully joinable; missing metadata for %d products"
            % len(missing_metadata))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    review_out = OUTPUT_DIR / "reviews_sample.jsonl.gz"
    metadata_out = OUTPUT_DIR / "metadata_sample.jsonl.gz"
    write_gzip(review_out, review_lines)
    write_gzip(metadata_out, metadata_lines)

    total_bytes = review_out.stat().st_size + metadata_out.stat().st_size
    if total_bytes > MAX_TOTAL_BYTES:
        raise RuntimeError(
            "Sample is %.2f MiB; reduce MAX_SAMPLE_REVIEWS"
            % (total_bytes / 1024 / 1024))

    print("reviews: %d" % len(review_lines))
    print("review products: %d" % len(parent_asins))
    print("matching metadata products: %d" % len(found))
    print("seed users included: %d" % len(seen_seed_users))
    print("total compressed size: %.2f MiB" % (total_bytes / 1024 / 1024))
    print("output: %s" % OUTPUT_DIR)


if __name__ == "__main__":
    main()
