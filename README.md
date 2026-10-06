# Amazon Review Coordination — GitHub Codespaces + PySpark

This repository is configured to run the CSC 7740 Amazon Review Coordination project in **GitHub Codespaces**.

The development container provides:

- Python 3.11
- Java 17
- PySpark 3.5.7
- Spark in `local[*]` mode
- VS Code Python/Pylance extensions

This is the **Phase 1 development environment**. It is a single Codespace machine, not a multi-node Spark cluster.

## 1. Open the repository in Codespaces

Push the `.devcontainer/`, `Dockerfile`, `requirements.txt`, and project files to GitHub.

Then in GitHub:

1. Open the repository.
2. Select **Code → Codespaces → Create codespace on main**.
3. Wait for the container to build.
4. The terminal should open at `/workspace`.

Codespaces reads `.devcontainer/devcontainer.json` and builds the Dockerfile automatically.

## 2. Verify the environment

Run:

```bash
python --version
java -version
python -c "import pyspark; print(pyspark.__version__)"
```

Expected versions are approximately:

```text
Python 3.11.x
openjdk version "17..."
3.5.7
```

Then test Spark itself:

```bash
python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[2]').getOrCreate(); print('Spark count:', s.range(10).count()); s.stop()"
```

You should see:

```text
Spark count: 10
```

## 3. Project layout

Recommended repository structure:

```text
Amazon-Review-Coordination-python/
├── .devcontainer/
│   └── devcontainer.json
├── data/
│   ├── All_Beauty.jsonl
│   └── meta_All_Beauty.jsonl
├── output/
├── checkpoints/
├── AmazonReviewCoordination_single.py
├── AmazonReviewPipeline_phase1_fixed.py
├── phase1_smoke_test.py
├── Dockerfile
├── requirements.txt
└── README.md
```

### Important: dataset files

The Amazon Reviews'23 dataset is large. The repository `.gitignore` excludes files under `data/` by default so that multi-GB JSONL files are not accidentally committed to Git.

For Codespaces, place the required dataset files in `data/` after creating the Codespace, or obtain them using the project's approved dataset-download procedure.

## 4. Run the Phase 1 smoke test

Before processing the real dataset, run:

```bash
python phase1_smoke_test.py
```

This checks the main pipeline behavior using a small synthetic dataset.

## 5. Run the pipeline

If your working implementation is named:

```text
AmazonReviewCoordination_single.py
```

run:

```bash
python AmazonReviewCoordination_single.py
```

If you are testing the Phase 1 fixed implementation supplied with this environment, run:

```bash
python AmazonReviewPipeline_phase1_fixed.py
```

## 6. Spark configuration

The current Codespaces environment is intentionally configured for local development:

```text
Spark master = local[*]
```

This means Spark can use the CPU resources available to the Codespace, but there are no separate Spark worker machines.

Do **not** describe this as a multi-node Spark experiment in the final CSC 7740 report.

The eventual distributed experiment should use a separate multi-node Spark deployment and compare at least:

- one-worker execution
- multi-worker execution
- runtime
- relevant resource/configuration information

## 7. Why Spark 3.5.7?

The current implementation uses modern PySpark APIs, including `pyspark.ml.feature.MinHashLSH`. Spark 3.5.7 provides a stable environment for this Phase 1 implementation.

The older Spark 1.3.0 environment used in some earlier coursework/setup should not be used for this implementation.

## 8. Git workflow

From the Codespace terminal:

```bash
git status
git add .devcontainer/devcontainer.json Dockerfile requirements.txt README.md
git commit -m "Add GitHub Codespaces PySpark environment"
git push
```

If you also changed the pipeline code:

```bash
git add AmazonReviewCoordination_single.py
git commit -m "Fix Amazon review coordination pipeline"
git push
```

Remove the accidental leading space before `git commit` if copying the second command exactly.

## 9. Rebuilding after dependency changes

If you change `Dockerfile` or `requirements.txt`, rebuild the Codespace container:

**VS Code Command Palette → Codespaces: Rebuild Container**

A normal Python source-code change does not require rebuilding the container.

## 10. Current project limitation

The Phase 1 implementation uses custom connected-components/minimum-label propagation logic. It does **not** yet provide the planned GraphFrames implementation.

Likewise, the Codespace is not the final cloud multi-node experiment environment.

Those should be handled as later project phases rather than mixing cluster configuration into the basic development environment.
