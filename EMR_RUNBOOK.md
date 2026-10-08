# AWS EMR Runbook

This runbook covers only the remaining real multi-node experiment. It has not been executed from this workstation because the AWS CLI and AWS credentials are not installed here.

## Target experiment

Use one EMR primary node and compare:

- Run A: one core/worker node.
- Run B: two core/worker nodes.

Both runs must use the same code, all four categories, parameters, instance type, and S3 inputs. Only the worker count and output prefix should differ. This is a real cluster in both cases; the second run supplies the required one-worker versus multi-worker timing comparison.

## 1. Prepare S3

1. Create one private S3 bucket in the same AWS Region as the EMR cluster.
2. Upload the four review files under `s3://BUCKET/data/reviews/`.
3. Upload the four metadata files under `s3://BUCKET/data/meta/`.
4. Upload `AmazonReviewCoordinationDF.py`, `emr/bootstrap.sh`, and `emr/run_emr.sh` under `s3://BUCKET/project/`.
5. Reserve separate result prefixes: `results/one-worker/` and `results/two-workers/`.

Do not place AWS keys in the repository, scripts, screenshots, or report.

## 2. Create the first cluster

In the Amazon EMR console, choose EMR on EC2 and create a cluster with:

- Release: `emr-7.14.0` if available in the selected Region. It includes Spark 3.5.8 and Python 3.11, which match this Spark 3.5 project.
- Applications: Spark and Hadoop.
- Primary: 1 `m5.xlarge`.
- Core: 1 `m5.xlarge`.
- Storage: at least 64 GB gp3 per node to leave room for shuffle and temporary data.
- Logging: an S3 log URI in the project bucket.
- Bootstrap action: `s3://BUCKET/project/bootstrap.sh`.
- Auto-termination: enable an idle timeout, but leave enough time to resize and perform the second run.

Create or select the required EMR service role and EC2 instance profile through the console. The roles need read access to the input/code prefixes and write access to the result/log prefixes.

## 3. Run with one worker

Connect to the primary node and download the runner:

```bash
aws s3 cp s3://BUCKET/project/run_emr.sh /tmp/run_emr.sh
chmod +x /tmp/run_emr.sh
```

Set the paths and run:

```bash
export S3_CODE_URI=s3://BUCKET/project/AmazonReviewCoordinationDF.py
export REVIEWS_PATH='s3://BUCKET/data/reviews/*.jsonl.gz'
export METADATA_PATH='s3://BUCKET/data/meta/*.jsonl.gz'
export OUTPUT_ROOT=s3://BUCKET/results/one-worker
export RUN_LABEL=one-worker
/tmp/run_emr.sh 2>&1 | tee /tmp/one-worker.log
aws s3 cp /tmp/one-worker.log s3://BUCKET/logs/one-worker.log
```

Confirm that the log ends with `PIPELINE COMPLETED SUCCESSFULLY` and that all output folders exist in S3.

## 4. Resize and run with two workers

In the EMR console, resize the core instance group from 1 to 2. Wait until both core nodes are running and YARN shows both workers as healthy. Then repeat the command with:

```bash
export OUTPUT_ROOT=s3://BUCKET/results/two-workers
export RUN_LABEL=two-workers
/tmp/run_emr.sh 2>&1 | tee /tmp/two-workers.log
aws s3 cp /tmp/two-workers.log s3://BUCKET/logs/two-workers.log
```

Do not overwrite the first run.

## 5. Evidence to save

- Cluster summary showing the EMR release, Region, and applications.
- Hardware page showing one primary and the worker count for each run.
- Spark/YARN application page showing completed stages and executors.
- Complete logs for both runs.
- S3 output folders and `_SUCCESS` markers.
- The final count block from each log.
- Stage timings and total runtime from each log.
- The exact code commit and parameter values used.

Use a small comparison table in the report: workers, instance type, reviews, pairs, components, total time, speedup, and any failed/retried tasks.

## 6. Cost and safety controls

- Create an AWS Budget alert before launching the cluster.
- Keep the S3 bucket and EMR cluster in the same Region.
- Use On-Demand instances for the primary; Spot workers are optional but make timing comparisons less controlled.
- Watch the first run. If it fails early, stop and diagnose instead of repeatedly relaunching.
- Terminate the cluster immediately after both runs and screenshots are complete.
- Keep the small logs/results needed for the report; remove unnecessary large duplicate outputs after submission.

## Official references

- EMR getting started: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html
- Add a Spark step: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-submit-step.html
- EMR Spark release history: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/Spark-release-history.html

