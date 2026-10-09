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

L119 `json`, L119 `select`, L119 `where`, L119 `withColumn`, L119 `withColumn`, L119 `withColumn`, L119 `withColumn`, L119 `withColumn`, L119 `withColumn`, L136 `dropDuplicates`, L136 `json`, L136 `select`, L136 `where`, L148 `join`, L148 `withColumn`, L168 `withColumn`, L169 `agg`, L169 `groupBy`, L173 `drop`, L173 `join`, L173 `withColumn`, L173 `withColumn`, L173 `withColumn`, L191 `distinct`, L191 `select`, L195 `agg`, L195 `groupBy`, L195 `join`, L195 `where`, L195 `where`, L220 `distinct`, L220 `select`, L220 `unionByName`, L221 `select`, L224 `select`, L229 `connectedComponents`, L232 `select`, L250 `distinct`, L250 `select`, L250 `unionByName`, L251 `select`, L255 `distinct`, L255 `select`, L255 `withColumn`, L261 `join`, L261 `select`, L264 `localCheckpoint`, L265 `agg`, L265 `groupBy`, L265 `unionByName`, L269 `count`, L269 `join`, L269 `limit`, L269 `where`, L283 `join`, L286 `agg`, L286 `groupBy`, L286 `withColumn`, L302 `agg`, L302 `agg`, L302 `groupBy`, L302 `groupBy`, L302 `withColumn`, L310 `agg`, L310 `groupBy`, L310 `join`, L310 `select`, L310 `select`, L310 `withColumn`, L319 `agg`, L319 `groupBy`, L319 `join`, L325 `drop`, L325 `join`, L325 `join`, L325 `join`, L325 `withColumn`, L325 `withColumn`, L340 `join`, L340 `withColumn`, L340 `withColumn`, L340 `withColumn`, L340 `withColumn`, L340 `withColumn`, L340 `withColumn`, L367 `join`, L367 `select`, L367 `where`, L367 `where`, L367 `withColumn`, L367 `withColumn`, L384 `count`, L384 `limit`, L389 `transform`, L394 `fit`, L399 `cache`, L400 `approxSimilarityJoin`, L400 `select`, L400 `where`, L400 `where`, L400 `where`, L414 `agg`, L414 `groupBy`, L437 `select`, L437 `where`, L437 `withColumn`, L437 `withColumn`, L437 `withColumn`, L437 `withColumn`, L459 `where`, L470 `orderBy`, L472 `drop`, L472 `join`, L472 `select`, L472 `where`, L472 `withColumn`, L527 `count`, L532 `parquet`, L537 `cache`, L537 `distinct`, L537 `select`, L538 `count`, L538 `distinct`, L538 `select`, L541 `cache`, L542 `count`, L545 `cache`, L546 `count`, L547 `cache`, L548 `count`, L553 `orderBy`, L553 `select`, L553 `show`, L561 `cache`, L562 `count`, L563 `count`, L563 `distinct`, L563 `select`, L568 `cache`, L571 `cache`, L571 `orderBy`, L572 `count`, L575 `parquet`, L576 `parquet`, L577 `parquet`, L578 `parquet`, L581 `coalesce`, L585 `parquet`, L589 `select`, L589 `show`, L595 `parquet`

### `analysis/sensitivity.py`

L63 `cache`, L63 `json`, L63 `select`, L63 `where`, L63 `withColumn`, L69 `count`, L82 `withColumn`, L83 `agg`, L83 `groupBy`, L84 `distinct`, L84 `join`, L84 `select`, L84 `withColumn`, L84 `withColumn`, L84 `withColumn`, L94 `count`, L94 `distinct`, L94 `select`, L98 `agg`, L98 `cache`, L98 `groupBy`, L98 `join`, L98 `where`, L98 `where`, L106 `count`, L109 `where`, L110 `collect`, L120 `unpersist`

### `analysis/injection.py`

L69 `json`, L69 `where`, L75 `count`, L77 `select`, L77 `unionByName`, L77 `withColumn`, L82 `withColumn`, L83 `agg`, L83 `groupBy`, L84 `distinct`, L84 `join`, L84 `select`, L84 `withColumn`, L84 `withColumn`, L84 `withColumn`, L96 `agg`, L96 `groupBy`, L96 `join`, L96 `where`, L96 `where`, L96 `where`, L100 `collect`
