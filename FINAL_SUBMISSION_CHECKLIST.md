# Final Project Submission Checklist

Status snapshot: 2026-10-06. This file turns the attached final-project guidelines into project-specific tasks. It does not replace the instructor's instructions.

## Deliverables

| Deliverable | Requirement | Current status |
|---|---|---|
| Final poster | Use the required template and submit the final version | Midterm poster exists; final results and cluster comparison still need to be added |
| Video | 5–10 minutes; presenter face visible throughout | Not recorded |
| Final report | Separate PDF; at least 5 pages excluding cover, contents, and appendices | Outline and verified evidence are available; report not written |
| Project ZIP | All raw source/executable scripts plus a representative sample dataset no larger than 10 MB; exclude full data and generated output | Not assembled |
| Online text field | Paste only the core processing code, with a short AI-use disclosure at the top; do not use attachments or links | Core file is `AmazonReviewCoordinationDF.py`; paste it only after the final cluster run |
| Team submission | Only one member submits for the entire team | Choose the submitting member before the deadline |

## Verified evidence already available

- Full local run: 24,447,530 reviews, 20,640,171 candidate product-time groups, 1,051 initial repeated pairs, 13 artifacts removed, 1,038 usable pairs, 1,294 vertices, and 443 components.
- Full local runtime: 6,050.5 seconds (about 1 hour 41 minutes). Evidence: `logs/full-run.log` and `output/df_full/`.
- Component sizes: 367 two-account groups, 76 groups with at least 3 accounts, 4 groups with at least 10 accounts, and a largest component of 223 accounts.
- Text evidence is sparse: 6 components contain cross-account similar text, with 34 similar review pairs in total.
- Corrected synthetic validation: 100% recovery for three fully coordinated planted groups; 0% for the below-threshold and outside-window negative controls; 87.5% for the partial-participation group.
- Video Games validation: 4,624,615 reviews; 232 initial pairs; 8 artifacts removed; 224 usable pairs; 338 vertices; 129 components; 617 seconds.

## Work still required for the strongest submission

1. Run the same code on a real multi-node cluster. For an on-campus team, this is the work that avoids the 90-point cap.
2. Record two comparable timings: one worker versus at least two workers. Keep data, parameters, Spark version, instance type, and output the same.
3. Save cluster proof: EMR cluster summary, node list, Spark history/application page, command, logs, S3 output, and successful completion counts.
4. Add the sensitivity results and corrected synthetic-validation table to the report.
5. Investigate representative top groups, especially the 223-account sparse component. Call them candidate coordination groups, not confirmed fraud.
6. Write the report, update the final poster, record the video, make the sample dataset, and assemble the final ZIP.

## Required report structure

1. Cover page: project title, course, semester, team members, LSU emails, and project URL if requested.
2. Table of contents.
3. Introduction and motivation.
4. Division of work. Give each member concrete engineering, experiment, analysis, and presentation responsibilities.
5. Architecture, big-data framework, and datasets. Include a pipeline diagram and the multi-node cluster layout.
6. Detailed component explanation: ingestion, metadata join, partitioned Parquet, skew control, repeated co-review edges, GraphFrames components, MinHash, scoring, and exports.
7. Evaluation: environment, reproducible run instructions, correctness tests, sensitivity analysis, local and cluster timings, findings, limitations, and screenshots.
8. Conclusion.
9. Appendix 1: source-code summary and an honest AI-assistance table.
10. Appendix 2: count and describe the distributed operations used by the application.

## Claims and caveats to use

- The method detects repeated co-review behavior; it does not prove fraud.
- The metadata join is used through catalog-average rating deviation and product/category enrichment.
- Oversized product-time groups are split into 6-hour buckets to limit skew and pair explosion.
- Processed reviews are written as Parquet partitioned by category and year.
- The 223-account component is sparse and may be a chain connected by bridging accounts; analyze it separately instead of presenting all 223 accounts as one coordinated ring.
- Do not describe a local `local[4]` run as a multi-node distributed experiment.

## Suggested AI-use disclosure

> OpenAI ChatGPT/Codex was used to brainstorm and refine the project design, review and debug PySpark code, help design validation experiments, and improve documentation and comments. The team reviewed the generated suggestions, ran the experiments, verified the outputs, and is responsible for the final code, interpretation, and presentation.

Use the same truthful disclosure in the report appendix and at the top of the code pasted into the online text field.

## Team information to verify

- Riyadil Zannat — Riyadil.Zannat@lsu.edu
- Asif Faisal Chowdhury — achowd6@lsu.edu
- Souhardya Saha Dip — add LSU email
- No team member has previously taken CSC 4740.

## Final pre-submission checks

- Every reported number matches a saved log, CSV, Parquet result, or screenshot.
- The cluster comparison uses the same data and parameters in both configurations.
- Full data and the 5.2 GB output directory are not placed in the ZIP.
- The sample dataset is at most 10 MB.
- All code in the ZIP runs from documented commands and contains no machine-specific home path.
- The PDF report meets the five-page minimum before appendices.
- The video is 5–10 minutes and the presenter face remains visible.
- Exactly one team member submits all team deliverables.
