# Analysis scripts

Standalone PySpark/Python measurements used to size the problem and to support
claims in the report. They do not import the pipeline and can be run on their
own. Each expects the datasets under `../data/`.

| script | what it measures | result on the four categories |
|---|---|---|
| `pairest.py` | candidate account pairs the graph step must build, after the oversized-group split | 8,195,949 pairs from 21,390,754 groups; largest group 556 reviews |
| `nearDup.py` | near-duplicate review text between different accounts on the same product and week | 10 pairs above 0.5 Jaccard out of 700,679 compared; no cluster of 3+ accounts |
| `lockstep.py` | accounts that repeatedly review the same products in the same weeks | 690 pairs share 3+ product-weeks, 59 share 5+, top pair 41 (Video Games) |
| `feasibility.py` | generic review-text noise and group-size skew | 17.5% of reviews are under 30 characters |
| `download_data.sh` | fetches the review files from the McAuley Lab server | - |

These produced the numbers behind two findings worth reporting: copy-paste
review text is essentially absent in Amazon Reviews'23, and co-review
behaviour is sparse (groups are almost always pairs).
