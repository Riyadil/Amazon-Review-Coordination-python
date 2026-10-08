# Validation Results

These results were produced on the 4,624,615-review Video Games category with the same product-time grouping, oversized-group splitting, suffixed-ID filtering, and repeated-pair rules used by the main pipeline.

## Sensitivity analysis

With the repetition threshold fixed at 3:

| Window | Usable pairs | Users | Components | Largest component |
|---:|---:|---:|---:|---:|
| 6 hours | 58 | 95 | 44 | 4 |
| 12 hours | 110 | 184 | 82 | 7 |
| 24 hours | 224 | 338 | 129 | 34 |
| 72 hours | 637 | 873 | 310 | 177 |
| 168 hours | 1,302 | 1,603 | 515 | 328 |

With the window fixed at 24 hours:

| Minimum shared groups | Usable pairs | Users | Components | Largest component |
|---:|---:|---:|---:|---:|
| 2 | 4,989 | 7,037 | 2,784 | 675 |
| 3 | 224 | 338 | 129 | 34 |
| 5 | 10 | 16 | 7 | 4 |
| 10 | 1 | 2 | 1 | 2 |

The selected 24-hour window and three-group threshold are a defensible middle point. Shorter windows or a threshold of five remove most of the signal. A threshold of two or windows of three to seven days create very large connected components that are more likely to contain weak chain connections. Sensitivity demonstrates that the result counts depend strongly on the definition of coordination, so groups must be interpreted as candidates rather than confirmed fraud.

Full measurements are saved in `output/sensitivity_v2.csv`; the run log is `logs/sensitivity-v2.log`.

## Synthetic injection test

| Planted pattern | Accounts | Products | Accounts recovered | Recall | Expected result |
|---|---:|---:|---:|---:|---|
| Tight group | 3 | 5 | 3 | 100% | Detect |
| Tight group | 5 | 3 | 5 | 100% | Detect |
| Larger tight group | 10 | 3 | 10 | 100% | Detect |
| Only two shared products | 5 | 2 | 0 | 0% | Miss below threshold |
| Same products, accounts staggered across 96 hours | 5 | 5 | 0 | 0% | Miss outside 24-hour window |
| 60% random participation | 8 | 6 | 7 | 87.5% | Partial recovery |

The corrected test shows that the detector finds planted coordination that satisfies its rules and rejects both negative controls. It also shows the expected loss of recall when group participation is incomplete. No planted group absorbed an unrelated real account.

Full measurements are saved in `output/injection_results_v2.csv`; the run log is `logs/injection-v2.log`.

## Reporting limits

- The sensitivity and injection experiments use one category, not all four.
- Synthetic recall validates rule implementation, not the truth of real-world fraud labels.
- The Amazon dataset has no ground-truth coordination labels, so real groups require qualitative inspection and cautious wording.
