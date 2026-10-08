# Analysis scripts

Standalone PySpark/Python measurements used to size the problem and to support
claims in the report. They do not import the pipeline and can be run on their
own. Each expects the datasets under `../data/`.

| script | what it measures | result / purpose |
|---|---|---|
| `pairest.py` | candidate account pairs the graph step must build, after the oversized-group split | 8,195,949 pairs from 21,390,754 groups; largest group 556 reviews |
| `nearDup.py` | near-duplicate review text between different accounts on the same product and week | 10 pairs above 0.5 Jaccard out of 700,679 compared; no cluster of 3+ accounts |
| `lockstep.py` | accounts that repeatedly review the same products in the same weeks | 690 pairs share 3+ product-weeks, 59 share 5+, top pair 41 (Video Games) |
| `feasibility.py` | generic review-text noise and group-size skew | 17.5% of reviews are under 30 characters |
| `download_data.sh` | fetches the review files from the McAuley Lab server | - |
| `sensitivity.py` | effect of the time window and repetition threshold | supports the selected 24-hour / 3-group parameters on Video Games |
| `injection.py` | recovery of known planted groups and negative controls | 100% recall for complete valid rings; both negative controls correctly missed |
| `create_sample_dataset.py` | joinable sample for the submission ZIP | 5,037 reviews plus matching metadata in 4.77 MiB; full smoke test succeeds |
| `count_spark_operations.py` | auditable Appendix 2 static count | 201 Spark call sites across the core and validation code |
| `VALIDATION_RESULTS.md` | report-ready result tables and caveats | sensitivity and corrected injection results |

These produced the numbers behind two findings worth reporting: copy-paste
review text is essentially absent in Amazon Reviews'23, and co-review
behaviour is sparse (groups are almost always pairs).
