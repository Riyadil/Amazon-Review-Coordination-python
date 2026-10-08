"""Count Spark execution call sites for Final Report Appendix 2."""

import ast
import os
from pathlib import Path

SOURCES = [
    Path("AmazonReviewCoordinationDF.py"),
    Path("analysis/sensitivity.py"),
    Path("analysis/injection.py"),
]

# DataFrame/RDD/ML transformations, actions, persistence/checkpointing, and I/O.
# Spark SQL expression helpers such as F.col(), F.avg(), and F.count() are
# deliberately excluded because they do not independently execute work.
SPARK_CALLS = {
    "agg", "approxSimilarityJoin", "cache", "coalesce", "collect",
    "connectedComponents", "count", "distinct", "drop", "dropDuplicates",
    "filter", "fit", "groupBy", "join", "json", "limit",
    "localCheckpoint", "orderBy", "parquet", "persist", "repartition",
    "select", "selectExpr", "show", "sort", "take", "transform",
    "union", "unionByName", "unpersist", "where", "withColumn",
}


def root_name(node):
    while isinstance(node, (ast.Attribute, ast.Call, ast.Subscript)):
        if isinstance(node, ast.Attribute):
            node = node.value
        elif isinstance(node, ast.Call):
            node = node.func
        else:
            node = node.value
    return node.id if isinstance(node, ast.Name) else ""


def count_file(path):
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        name = node.func.attr
        if name not in SPARK_CALLS:
            continue
        # Exclude SQL expression constructors such as F.count(...) and
        # Python container methods that happen to share a Spark method name.
        root = root_name(node.func.value)
        if not root or root in {"F", "os", "rows", "parent", "sizes", "members", "parent_asins", "found"}:
            continue
        found.append((node.lineno, name))
    return sorted(found)


def main():
    output = Path(os.environ.get(
        "OPERATIONS_REPORT", "analysis/DISTRIBUTED_OPERATIONS.md"))
    results = [(path, count_file(path)) for path in SOURCES]
    total = sum(len(calls) for _, calls in results)

    lines = [
        "# Distributed Operations Count",
        "",
        "Static count of Spark call sites in the submitted Python source. "
        "Repeated runtime execution inside loops is counted once per source call site. "
        "Local Python operations and Spark SQL expression helpers are excluded.",
        "",
        "| File | Distributed operation call sites |",
        "|---|---:|",
    ]
    for path, calls in results:
        lines.append("| `%s` | %d |" % (path, len(calls)))
    lines.extend(["| **Total** | **%d** |" % total, "", "## Audit detail", ""])
    for path, calls in results:
        details = ", ".join("L%d `%s`" % item for item in calls)
        lines.extend(["### `%s`" % path, "", details or "None", ""])

    output.write_text("\n".join(lines), encoding="utf-8")
    print("wrote %s (%d call sites)" % (output, total))


if __name__ == "__main__":
    main()
