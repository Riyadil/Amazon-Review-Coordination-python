# Temporary AWS EMR Setup Guide

> Temporary working guide for the Amazon Review Coordination project. Replace every value written as `YOUR_...` before running commands. Do not put AWS passwords, access keys, or the downloaded `.pem` key in Git.

## Goal

Complete two full runs of the same 24,447,530-review pipeline in a real AWS EMR cluster:

1. Run A: 1 primary node + 1 core worker.
2. Run B: the same cluster resized to 1 primary node + 2 core workers.

Use identical data, code, and parameters. Save the two runtimes and AWS screenshots. This is the remaining work needed to remove the on-campus team's 90-point cluster cap.

## Values to decide before starting

| Placeholder | Example | Your value |
|---|---|---|
| `YOUR_REGION` | `us-east-1` | |
| `YOUR_BUCKET` | `csc7740-amazon-review-yourname` | |
| `YOUR_KEY_NAME` | `csc7740-emr-key` | |
| `YOUR_KEY_FILE` | `~/Downloads/csc7740-emr-key.pem` | |
| `YOUR_PRIMARY_DNS` | Shown after the cluster starts | |

Keep the S3 bucket and EMR cluster in the same Region. S3 bucket names must be globally unique.

## Step 1 — Set a cost warning first

1. Open **AWS Console → Billing and Cost Management → Budgets**.
2. Choose **Create budget**.
3. Choose the simplified **Monthly cost budget** template, or create a custom cost budget.
4. Set the budget to **$30**.
5. Enter an email address and create alerts around **$10, $20, and $25** if the advanced workflow is available.

A budget sends warnings; it is not a hard spending limit, and AWS cost data can be delayed. You must still terminate the cluster manually.

## Step 2 — Create an EC2 key pair

1. Open **EC2 → Network & Security → Key Pairs**.
2. Choose **Create key pair**.
3. Name it `YOUR_KEY_NAME`.
4. Choose **RSA** and **`.pem`**.
5. Download the file and keep it private. AWS will not let you download the private key again.
6. On Linux/macOS, run locally:

```bash
chmod 400 YOUR_KEY_FILE
```

Do not upload the `.pem` file to S3 or GitHub.

## Step 3 — Create the S3 bucket

1. Open **S3 → Create bucket**.
2. Enter `YOUR_BUCKET` and select `YOUR_REGION`.
3. Leave **Block all public access** enabled.
4. Leave default encryption enabled.
5. Create the bucket.

Create or upload into these prefixes:

```text
s3://YOUR_BUCKET/data/reviews/
s3://YOUR_BUCKET/data/meta/
s3://YOUR_BUCKET/project/
s3://YOUR_BUCKET/logs/
s3://YOUR_BUCKET/results/
```

## Step 4 — Upload the project inputs

From the S3 console, open the bucket and upload the following local files.

### Upload to `data/reviews/`

```text
data/reviews/Arts_Crafts_and_Sewing.jsonl.gz
data/reviews/Baby_Products.jsonl.gz
data/reviews/CDs_and_Vinyl.jsonl.gz
data/reviews/Video_Games.jsonl.gz
```

### Upload to `data/meta/`

```text
data/meta/meta_Arts_Crafts_and_Sewing.jsonl.gz
data/meta/meta_Baby_Products.jsonl.gz
data/meta/meta_CDs_and_Vinyl.jsonl.gz
data/meta/meta_Video_Games.jsonl.gz
```

### Upload to `project/`

```text
AmazonReviewCoordinationDF.py
emr/bootstrap.sh
emr/run_emr.sh
```

The eight dataset files total approximately 4.5 GB compressed. Wait until every upload reports success before creating the cluster.

## Step 5 — Prepare the EMR roles

EMR needs two roles:

- EMR service role: use or create `EMR_DefaultRole_V2`.
- EC2 instance profile: select the instance-profile role offered by the EMR console, commonly `EMR_EC2_DefaultRole`, or a custom EMR EC2 role.

If this is the first EMR cluster in the account, the console may offer to create default roles. Choose the v2/default-permissions option. The EC2 instance-profile role must be able to list, read, write, and delete objects in `YOUR_BUCKET`.

If the cluster later reports `AccessDenied` for S3, attach a least-privilege policy like this to the chosen EC2 instance-profile role after replacing `YOUR_BUCKET`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListProjectBucket",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::YOUR_BUCKET"
    },
    {
      "Sid": "ReadWriteProjectObjects",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::YOUR_BUCKET/*"
    }
  ]
}
```

## Step 6 — Create the EMR cluster

Open **Amazon EMR → EMR on EC2 → Clusters → Create cluster**.

Use these settings:

| Setting | Value |
|---|---|
| Cluster name | `CSC7740-Amazon-Review` |
| EMR release | `emr-7.14.0` if available |
| Applications | Spark and Hadoop |
| Provisioning | Instance groups, not instance fleets |
| Scaling | Fixed/manual; do not enable managed scaling |
| Primary group | 1 × `m5.xlarge`, On-Demand |
| Core group | 1 × `m5.xlarge`, On-Demand |
| EBS storage | At least 64 GB gp3 on each node |
| Network | Public subnet with outbound internet and public IPv4 for the primary node |
| EC2 key pair | `YOUR_KEY_NAME` |
| Service role | `EMR_DefaultRole_V2` |
| EC2 instance profile | The S3-enabled EMR EC2 role from Step 5 |
| Log URI | `s3://YOUR_BUCKET/logs/emr/` |
| Auto-termination | About 60 idle minutes |

`emr-7.14.0` uses Spark 3.5.8, which matches this Spark 3.5 project. If it is unavailable in the selected Region, use another EMR 7.x release that contains Spark 3.5, and record the exact version.

### Add the bootstrap action

Under **Bootstrap actions**, add:

- Name: `Install project Python packages`
- Script location: `s3://YOUR_BUCKET/project/bootstrap.sh`

The cluster needs outbound internet access because the bootstrap installs Python packages and Spark downloads the GraphFrames package. A normal public subnet with outbound internet is the simplest option.

Create the cluster and wait until its status is **Waiting**.

## Step 7 — Allow SSH from only your computer

1. On the cluster page, open the **Instances** or **Security** information.
2. Open the primary node's EC2 security group.
3. Add an inbound rule:
   - Type: SSH
   - Protocol/port: TCP 22
   - Source: **My IP**, not `0.0.0.0/0`
4. Save the rule.

Copy the primary node's public DNS name from the EMR cluster page.

Connect from your local terminal:

```bash
ssh -i YOUR_KEY_FILE hadoop@YOUR_PRIMARY_DNS
```

The EMR login username is `hadoop`.

## Step 8 — Verify the cluster before spending time on the full run

Run these commands on the EMR primary node:

```bash
python3 --version
spark-submit --version
aws s3 ls s3://YOUR_BUCKET/project/
aws s3 ls s3://YOUR_BUCKET/data/reviews/
aws s3 ls s3://YOUR_BUCKET/data/meta/
```

Stop here and fix the setup if:

- Any of the eight input files is missing.
- S3 returns `AccessDenied`.
- Spark is unavailable.
- The bootstrap action failed.

## Step 9 — Run A with one worker

Still on the EMR primary node:

```bash
aws s3 cp s3://YOUR_BUCKET/project/run_emr.sh /tmp/run_emr.sh
chmod +x /tmp/run_emr.sh

export S3_CODE_URI=s3://YOUR_BUCKET/project/AmazonReviewCoordinationDF.py
export REVIEWS_PATH='s3://YOUR_BUCKET/data/reviews/*.jsonl.gz'
export METADATA_PATH='s3://YOUR_BUCKET/data/meta/*.jsonl.gz'
export OUTPUT_ROOT=s3://YOUR_BUCKET/results/one-worker
export RUN_LABEL=one-worker

nohup /tmp/run_emr.sh > /tmp/one-worker.log 2>&1 &
echo $! > /tmp/one-worker.pid
tail -f /tmp/one-worker.log
```

Using `nohup` allows the Spark submission to continue if SSH disconnects. Watch
the log until it shows either `PIPELINE COMPLETED SUCCESSFULLY` or an error,
then press **Ctrl+C** to stop only the `tail` display. Upload the completed log:

```bash
aws s3 cp /tmp/one-worker.log s3://YOUR_BUCKET/logs/one-worker.log
```

### Confirm Run A

The bottom of the log should show these expected counts:

```text
PIPELINE COMPLETED SUCCESSFULLY
Reviews processed : 24447530
Candidate groups  : 20640171
Repeated pairs    : 1038
Graph vertices    : 1294
Components        : 443
```

Record `Total runtime`. In S3, confirm that `results/one-worker/` contains `_SUCCESS` markers.

Before resizing, save screenshots of:

- Cluster Summary showing the release and cluster ID.
- Instances tab showing 1 primary and 1 core node.
- YARN/Spark application showing executors and completed stages.
- Successful log ending and S3 output.

## Step 10 — Resize to two workers

Only resize after Run A has completely finished.

1. Open the running EMR cluster.
2. Open the **Instances** tab.
3. In **Instance groups**, select the core group.
4. Choose **Resize instance group**.
5. Change the core instance count from **1 to 2**.
6. Wait until the core group is **Running** and both core nodes are healthy.

Take a screenshot showing 1 primary and 2 core nodes.

## Step 11 — Run B with two workers

Return to the same SSH terminal, or reconnect and repeat the earlier exports. Then run:

```bash
export S3_CODE_URI=s3://YOUR_BUCKET/project/AmazonReviewCoordinationDF.py
export REVIEWS_PATH='s3://YOUR_BUCKET/data/reviews/*.jsonl.gz'
export METADATA_PATH='s3://YOUR_BUCKET/data/meta/*.jsonl.gz'
export OUTPUT_ROOT=s3://YOUR_BUCKET/results/two-workers
export RUN_LABEL=two-workers

nohup /tmp/run_emr.sh > /tmp/two-workers.log 2>&1 &
echo $! > /tmp/two-workers.pid
tail -f /tmp/two-workers.log
```

After success, press **Ctrl+C** to stop `tail`, then upload the log:

```bash
aws s3 cp /tmp/two-workers.log s3://YOUR_BUCKET/logs/two-workers.log
```

Confirm the same five result counts as Run A. Record `Total runtime` and confirm the `_SUCCESS` markers under `results/two-workers/`.

Save screenshots of the two-worker configuration, Spark/YARN execution, successful log ending, and S3 output.

## Step 12 — Calculate and report the comparison

Use:

```text
speedup = one-worker runtime / two-worker runtime
percentage reduction = (one-worker runtime - two-worker runtime) / one-worker runtime × 100
```

Report this table:

| Run | Primary nodes | Worker nodes | Instance type | Reviews | Pairs | Components | Runtime |
|---|---:|---:|---|---:|---:|---:|---:|
| One worker | 1 | 1 | m5.xlarge | 24,447,530 | 1,038 | 443 | Fill in |
| Two workers | 1 | 2 | m5.xlarge | 24,447,530 | 1,038 | 443 | Fill in |

State that the second run could receive some operating-system/S3 caching benefit because the same cluster was resized. The code itself creates a new Spark application and does not intentionally reuse cached DataFrames between runs.

## Step 13 — Download evidence and terminate immediately

1. Confirm both log files are in `s3://YOUR_BUCKET/logs/`.
2. Save all required screenshots locally.
3. Download the two small logs if desired.
4. In EMR, choose **Terminate** for the cluster.
5. Wait until the cluster status is **Terminated**.
6. Check EC2 Instances and EBS Volumes to ensure no unexpected running resources remain.
7. Keep the S3 data until the report is complete; S3 remains billable after cluster termination.

Do not merely close the browser. Closing the console does not stop AWS charges.

## Troubleshooting

| Problem | Most likely fix |
|---|---|
| SSH times out | Confirm a public DNS/IP exists and TCP 22 is allowed from **My IP** |
| `Permission denied (publickey)` | Use the correct `.pem`, run `chmod 400`, and connect as `hadoop` |
| S3 `AccessDenied` | Fix the EC2 instance-profile role's permissions for `YOUR_BUCKET` |
| Bootstrap failed | Inspect the bootstrap logs in the configured S3 log prefix; verify internet access |
| GraphFrames package cannot download | Verify outbound internet access and access to `repos.spark-packages.org` |
| Output already exists or is mixed | Use the exact separate `one-worker` and `two-workers` output prefixes |
| Counts differ between runs | Verify the same eight files, code, environment variables, and thresholds were used |
| Job runs out of memory | Check failed executor logs before increasing instance size; changing hardware affects the comparison |

## Final completion checklist

- [ ] $30 AWS budget and email alerts created.
- [ ] Eight dataset files and three project files uploaded.
- [ ] EMR roles can access the private project bucket.
- [ ] Run A completed with 1 core worker.
- [ ] Run B completed with 2 core workers.
- [ ] Counts match between runs.
- [ ] Both runtimes recorded.
- [ ] Cluster, executor, log, and S3 screenshots saved.
- [ ] Logs copied to S3.
- [ ] Cluster terminated and EC2 checked.
- [ ] Report and poster updated with the comparison.

## Official AWS references

- Create and use EMR clusters: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html
- EMR IAM roles: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-roles.html
- Connect to the primary node using SSH: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-ssh.html
- Resize a running cluster: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-manage-resize.html
- EMR Spark versions: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/Spark-release-history.html
- Create an AWS budget: https://docs.aws.amazon.com/cost-management/latest/userguide/create-cost-budget.html

