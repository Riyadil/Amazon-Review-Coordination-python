# ============================================================
# Config.py
# ============================================================
# Configuration for Amazon Review Coordination Analysis
# ============================================================

# ==============================
# File Paths
# ==============================

REVIEWS_PATH = "data/All_Beauty.jsonl"
METADATA_PATH = "data/meta_All_Beauty.jsonl"

# Use a new directory for each run.
OUTPUT_PATH = "output/amazon_output"

# Local CSV for manual review of repeated user pairs.
PAIR_AUDIT_PATH = "output/repeated_pair_audit.csv"


# ==============================
# Spark Configuration
# ==============================

APP_NAME = "Amazon Review Coordination Analysis"
MASTER = "local[*]"
DRIVER_MEMORY = "4g"


# ==============================
# Time-Bucket Configuration
# ==============================

TIME_BUCKET_HOURS = 24

LARGE_GROUP_THRESHOLD = 100

LARGE_GROUP_BUCKET_HOURS = 6


# ==============================
# Graph Configuration
# ==============================

MIN_REPEATED_GROUPS = 3

LABEL_PROPAGATION_ITERATIONS = 20


# ==============================
# Result Configuration
# ==============================

TOP_N = 20


# ==============================
# Coordination Score
# ==============================

SIZE_WEIGHT = 0.35
DENSITY_WEIGHT = 0.35
REPETITION_WEIGHT = 0.30

SIZE_NORMALIZATION = 20.0
REPETITION_NORMALIZATION = 10.0


# ==============================
# Text Analysis
# ==============================

# Reviews shorter than this are excluded
# from the planned text-similarity stage,
# but remain in the co-review graph.

MIN_TEXT_LENGTH = 30


# ==============================
# Text Similarity Configuration
# ==============================

# Number of MinHash hash tables.
MINHASH_NUM_HASH_TABLES = 3

# Jaccard similarity threshold for candidate
# review-text similarity.
TEXT_SIMILARITY_THRESHOLD = 0.5

# Minimum number of text-similar review pairs
# required before increasing a group's score.
MIN_TEXT_SIMILAR_REVIEWS = 1


# ==============================
# Analysis Configuration
# ==============================

# Maximum number of reviews sampled/displayed
# for detailed analysis of one coordination group.
MAX_REVIEWS_PER_GROUP = 500


# ==============================
# Cloud / Performance Configuration
# ==============================

# Set to True when running on a multi-node Spark cluster.
# Keep False for local development.
CLOUD_MODE = False

# Number of Spark partitions for large operations.
DEFAULT_PARTITIONS = 200


# ==============================
# Runtime Benchmarking
# ==============================

ENABLE_RUNTIME_BENCHMARK = True