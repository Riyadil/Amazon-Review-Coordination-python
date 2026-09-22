# ==============================
# File Paths
# ==============================

REVIEWS_PATH = "data/All_Beauty.jsonl"
METADATA_PATH = "data/meta_All_Beauty.jsonl"
OUTPUT_PATH = "output/amazon_output"


# ==============================
# Spark Configuration
# ==============================

APP_NAME = "Amazon Review Coordination Analysis"
MASTER = "local[*]"

DRIVER_MEMORY = "4g"


# ==============================
# Pipeline Configuration
# ==============================

# Initial time bucket
TIME_BUCKET_HOURS = 24

# If a 24-hour group has more than this many reviews,
# split it into smaller buckets.
LARGE_GROUP_THRESHOLD = 100

# Size of smaller time buckets
LARGE_GROUP_BUCKET_HOURS = 6

# Minimum number of distinct product-time groups
# in which a user pair must appear.
MIN_REPEATED_GROUPS = 3

# Number of iterations for connected components
LABEL_PROPAGATION_ITERATIONS = 20

# Number of top coordination groups to display
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
# Review Processing
# ==============================

MIN_TEXT_LENGTH = 30
