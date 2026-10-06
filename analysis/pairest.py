# Estimate the candidate-pair workload of the project's graph step on the FULL data.
import time, glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

t0 = time.time()
spark = (SparkSession.builder.master("local[6]")
         .appName("pair-estimate")
         .config("spark.driver.memory", "8g")
         .config("spark.sql.shuffle.partitions", "200")
         .config("spark.local.dir", "/home/researchuser2/sparktmp")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

files = sorted(glob.glob("/home/researchuser2/workstation/CSC7740/project/*.jsonl.gz"))
print("input files:", [f.split('/')[-1] for f in files])

df = (spark.read.json(files)
      .select(F.col("asin").alias("product_id"),
              F.col("user_id"),
              F.col("timestamp").alias("ts")))
n = df.count()
print("reviews parsed: %d   (%.0fs)" % (n, time.time()-t0))

# Config: 24h buckets; groups with >100 reviews are re-bucketed to 6h
day = (F.col("ts")/1000/86400).cast("long")
six = (F.col("ts")/1000/21600).cast("long")
g = df.withColumn("d", day).withColumn("h", six)

day_groups = g.groupBy("product_id", "d").agg(F.count("*").alias("c"))
big = day_groups.filter("c > 100").select("product_id", "d")

small_pairs = (day_groups.filter("c <= 100")
               .select((F.col("c")*(F.col("c")-1)/2).alias("p"), F.col("c")))
# oversized day-groups: recompute at 6h granularity
big_sub = (g.join(big, ["product_id", "d"], "inner")
           .groupBy("product_id", "h").agg(F.count("*").alias("c"))
           .select((F.col("c")*(F.col("c")-1)/2).alias("p"), F.col("c")))

allp = small_pairs.unionAll(big_sub)
r = allp.agg(F.sum("p").alias("pairs"), F.count("*").alias("groups"), F.max("c").alias("maxgrp")).collect()[0]
print("\n--- candidate-pair workload (full 24.4M reviews) ---")
print("groups after splitting : %d" % r["groups"])
print("largest group          : %d reviews" % r["maxgrp"])
print("TOTAL CANDIDATE PAIRS  : %.0f" % (r["pairs"] or 0))
print("elapsed: %.0fs" % (time.time()-t0))
spark.stop()
