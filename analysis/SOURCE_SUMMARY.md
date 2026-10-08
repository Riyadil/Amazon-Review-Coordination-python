# Source Code Summary and AI Assistance

This table is formatted for Final Report Appendix 1. The line counts are physical source lines and include comments and blank lines.

| File | Lines | Primary functionality / data logic | AI coding-assistant usage scope |
|---|---:|---|---|
| `AmazonReviewCoordinationDF.py` | 595 | End-to-end PySpark ingestion, metadata join, skew-aware product-time grouping, repeated-pair graph, GraphFrames components, MinHash text comparison, scoring, partitioned Parquet, and audit exports | ChatGPT/Codex assisted with design review, debugging, DataFrame refactoring, performance improvements, comments, and validation; the team ran and verified the pipeline |
| `analysis/sensitivity.py` | 132 | Measures how window size and repetition threshold change pairs and graph components | ChatGPT/Codex assisted with experiment design, implementation review, and correction of result labels; the team executed and interpreted the experiment |
| `analysis/injection.py` | 144 | Injects known synthetic coordination and measures recovery/negative controls | ChatGPT/Codex assisted with experiment design, implementation, and correction of the outside-window control; the team executed and verified the result |
| `analysis/feasibility.py` | 44 | Measures short-text prevalence and grouping skew | ChatGPT/Codex assisted with code review and interpretation |
| `analysis/nearDup.py` | 40 | Early near-duplicate text feasibility test | ChatGPT/Codex assisted with code review and interpretation |
| `analysis/lockstep.py` | 31 | Early repeated co-review feasibility test | ChatGPT/Codex assisted with code review and interpretation |
| `analysis/pairest.py` | 47 | Estimates candidate-pair scale and oversized-group risk | ChatGPT/Codex assisted with code review and interpretation |
| `analysis/create_sample_dataset.py` | 110 | Builds a joinable compressed sample under the 10 MB submission limit | ChatGPT/Codex assisted with implementation; the team verified counts, matches, size, and an end-to-end smoke test |
| `analysis/count_spark_operations.py` | 84 | Produces an auditable static count for Appendix 2 | ChatGPT/Codex assisted with implementation and false-positive correction |
| `run_local.sh` | 17 | Reproducible local pipeline launcher | ChatGPT/Codex assisted with review and documentation |
| `run_all_categories.sh` | 48 | Optional sequential category diagnostic runner | ChatGPT/Codex assisted with portability and log-parser corrections |
| `analysis/download_data.sh` | 19 | Downloads the selected Amazon Reviews'23 files | ChatGPT/Codex assisted with review |
| `emr/bootstrap.sh` | 6 | Installs Python dependencies on all EMR nodes | ChatGPT/Codex assisted with implementation |
| `emr/run_emr.sh` | 26 | Submits the same pipeline to YARN with S3 input/output | ChatGPT/Codex assisted with implementation; the team must verify it during the cluster run |

Total physical lines across these Python and shell files: 1,343.

## Disclosure text

> OpenAI ChatGPT/Codex was used to brainstorm and refine the project design, review and debug PySpark code, help design validation experiments, and improve documentation and comments. The team reviewed the generated suggestions, ran the experiments, verified the outputs, and is responsible for the final code, interpretation, and presentation.

Before submission, each team member should confirm that the per-file descriptions accurately reflect how the tools were used.
