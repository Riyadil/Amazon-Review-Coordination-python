# Distributed Operations Count

Static count of Spark call sites in the submitted Python source. Repeated runtime execution inside loops is counted once per source call site. Local Python operations and Spark SQL expression helpers are excluded.

| File | Distributed operation call sites |
|---|---:|
| `AmazonReviewCoordinationDF.py` | 151 |
| `analysis/sensitivity.py` | 28 |
| `analysis/injection.py` | 22 |
| **Total** | **201** |

## Audit detail

### `AmazonReviewCoordinationDF.py`

L112 `json`, L112 `select`, L112 `where`, L112 `withColumn`, L112 `withColumn`, L112 `withColumn`, L112 `withColumn`, L112 `withColumn`, L112 `withColumn`, L129 `dropDuplicates`, L129 `json`, L129 `select`, L129 `where`, L141 `join`, L141 `withColumn`, L161 `withColumn`, L162 `agg`, L162 `groupBy`, L166 `drop`, L166 `join`, L166 `withColumn`, L166 `withColumn`, L166 `withColumn`, L184 `distinct`, L184 `select`, L188 `agg`, L188 `groupBy`, L188 `join`, L188 `where`, L188 `where`, L213 `distinct`, L213 `select`, L213 `unionByName`, L214 `select`, L217 `select`, L222 `connectedComponents`, L225 `select`, L243 `distinct`, L243 `select`, L243 `unionByName`, L244 `select`, L248 `distinct`, L248 `select`, L248 `withColumn`, L254 `join`, L254 `select`, L257 `localCheckpoint`, L258 `agg`, L258 `groupBy`, L258 `unionByName`, L262 `count`, L262 `join`, L262 `limit`, L262 `where`, L276 `join`, L279 `agg`, L279 `groupBy`, L279 `withColumn`, L295 `agg`, L295 `agg`, L295 `groupBy`, L295 `groupBy`, L295 `withColumn`, L303 `agg`, L303 `groupBy`, L303 `join`, L303 `select`, L303 `select`, L303 `withColumn`, L312 `agg`, L312 `groupBy`, L312 `join`, L318 `drop`, L318 `join`, L318 `join`, L318 `join`, L318 `withColumn`, L318 `withColumn`, L333 `join`, L333 `withColumn`, L333 `withColumn`, L333 `withColumn`, L333 `withColumn`, L333 `withColumn`, L333 `withColumn`, L360 `join`, L360 `select`, L360 `where`, L360 `where`, L360 `withColumn`, L360 `withColumn`, L377 `count`, L377 `limit`, L382 `transform`, L385 `fit`, L389 `cache`, L390 `approxSimilarityJoin`, L390 `select`, L390 `where`, L390 `where`, L390 `where`, L404 `agg`, L404 `groupBy`, L427 `select`, L427 `where`, L427 `withColumn`, L427 `withColumn`, L427 `withColumn`, L427 `withColumn`, L449 `where`, L460 `orderBy`, L462 `drop`, L462 `join`, L462 `select`, L462 `where`, L462 `withColumn`, L513 `count`, L517 `parquet`, L520 `cache`, L520 `distinct`, L520 `select`, L521 `count`, L521 `distinct`, L521 `select`, L524 `cache`, L525 `count`, L528 `cache`, L529 `count`, L530 `cache`, L531 `count`, L536 `orderBy`, L536 `select`, L536 `show`, L544 `cache`, L545 `count`, L546 `count`, L546 `distinct`, L546 `select`, L551 `cache`, L554 `cache`, L554 `orderBy`, L555 `count`, L558 `parquet`, L559 `parquet`, L560 `parquet`, L561 `parquet`, L564 `coalesce`, L568 `parquet`, L572 `select`, L572 `show`, L578 `parquet`

### `analysis/sensitivity.py`

L63 `cache`, L63 `json`, L63 `select`, L63 `where`, L63 `withColumn`, L69 `count`, L82 `withColumn`, L83 `agg`, L83 `groupBy`, L84 `distinct`, L84 `join`, L84 `select`, L84 `withColumn`, L84 `withColumn`, L84 `withColumn`, L94 `count`, L94 `distinct`, L94 `select`, L98 `agg`, L98 `cache`, L98 `groupBy`, L98 `join`, L98 `where`, L98 `where`, L106 `count`, L109 `where`, L110 `collect`, L120 `unpersist`

### `analysis/injection.py`

L69 `json`, L69 `where`, L75 `count`, L77 `select`, L77 `unionByName`, L77 `withColumn`, L82 `withColumn`, L83 `agg`, L83 `groupBy`, L84 `distinct`, L84 `join`, L84 `select`, L84 `withColumn`, L84 `withColumn`, L84 `withColumn`, L96 `agg`, L96 `groupBy`, L96 `join`, L96 `where`, L96 `where`, L96 `where`, L100 `collect`
