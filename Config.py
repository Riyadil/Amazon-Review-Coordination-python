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
