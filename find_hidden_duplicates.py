"""
Find hidden duplicate member records with PySpark (Databricks or local Spark).

Hidden duplicates are records for the same person that no exact match will find:
"Maria Gonzalez" / "Maria Gonzales" / "M. Gonzalez" at the same address.

Five steps: standardize -> block -> score -> group -> merge.

Input:  a table with columns id, name, dob, addr.
Output: one row per person, with the original ids kept in old_ids so any
        merge can be undone, and needs_review flagging groups to eyeball.

Run locally:  pip install pyspark && python find_hidden_duplicates.py
On Databricks: paste into a notebook and delete the SparkSession line
               (`spark` already exists there).

Tested on PySpark 4.3.0.
"""

# ----------------------------------------------------------------- setup ---
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()   # Databricks: `spark` already exists

# Tune these two on records you have checked by hand. They are not universal.
MERGE_AT = 0.75    # pairs at or above this score are treated as the same person
REVIEW_AT = 0.85   # ...but anything below this is worth a human look

# Sample data. Replace with: members = spark.table("your_catalog.your_schema.members")
members = spark.createDataFrame([
    (1001, "Maria Gonzalez", "04/12/1961", "12 Oak St Apt 4"),
    (2087, "Maria Gonzales", "04/12/1961", "12 Oak Street #4"),
    (3342, "M. Gonzalez",    "1961-04-12", "12 Oak St Unit 4"),
    (4410, "Maria Gomez",    "07/30/1975", "98 Elm Ave"),
], ["id", "name", "dob", "addr"])


# --------------------------------------------- 1. standardize the fields ---
# Exact matching fails on formatting, so make the formatting consistent first.
# Add your own rules here: suite/ste, apartment/apt, road/rd, drive/dr.
for pattern, replacement in [("(?i)street", "st"),
                             ("(?i)unit|#", "apt "),
                             (r"\s+", " ")]:
    members = members.withColumn("addr", F.regexp_replace("addr", pattern, replacement))

members = (members
    .withColumn("addr", F.trim(F.lower("addr")))
    .withColumn("name", F.trim(F.lower(F.regexp_replace("name", r"[.,]", ""))))
    .withColumn("dob", F.coalesce(F.expr("try_to_date(dob, 'MM/dd/yyyy')"),
                                  F.expr("try_to_date(dob, 'yyyy-MM-dd')"))))

# 12 Oak Street #4  ->  12 oak st apt 4
# M. Gonzalez       ->  m gonzalez


# ------------------------------------------------- 2. block the comparisons ---
# Comparing every row to every other row is n*(n-1)/2 pairs: about 500 billion
# at a million rows. Instead, only compare records that share a blocking key.
# Cost of blocking: a typo inside the key hides a true duplicate. Mitigate by
# running a second pass on a different key (phone + zip) and unioning the pairs.
last_name = F.element_at(F.split("name", " "), -1)
keyed = members.withColumn(
    "block", F.concat(F.substring(last_name, 1, 4), F.year("dob").cast("string")))

# Guard: a huge block (common surname + common year) would blow up the join.
oversized = keyed.groupBy("block").count().filter("count > 1000")
if oversized.count():
    print("WARNING: oversized blocks, add a field to the key:")
    oversized.show(5, truncate=False)

a, b = keyed.alias("a"), keyed.alias("b")
pairs = a.join(b, (F.col("a.block") == F.col("b.block"))
                & (F.col("a.id") < F.col("b.id")))     # a.id < b.id = each pair once

# gonz1961 -> 1001-2087, 1001-3342, 2087-3342 ... gome1975 -> no pairs


# ----------------------------------------------------- 3. score each pair ---
# levenshtein = how many single-character edits turn one string into the other.
# Divide by the longer string's length to get 0..1, then average the fields.
# Weight fields differently if some are more trustworthy in your data.
def similarity(column):
    x, y = f"a.{column}", f"b.{column}"
    return 1 - F.levenshtein(x, y) / F.greatest(F.length(x), F.length(y))

scored = pairs.select(
    F.col("a.id").alias("id_x"),
    F.col("b.id").alias("id_y"),
    ((similarity("name")
      + (F.col("a.dob") == F.col("b.dob")).cast("int")
      + similarity("addr")) / 3).alias("score"))

print("pair scores:")
scored.select("id_x", "id_y", F.round("score", 2).alias("score")).show()
# 1001-2087 0.98 | 1001-3342 0.90 | 2087-3342 0.88


# ------------------------------------------------- 4. group the matches ---
# Pairs are not enough: A-B and B-C means A, B and C are one person, even if
# A and C score lower than each other. This is connected components on a graph.
#
# GraphFrames does this in one call if it is installed on your cluster:
#     from graphframes import GraphFrame
#     spark.sparkContext.setCheckpointDir("/tmp/cc")   # required
#     groups = GraphFrame(vertices, edges).connectedComponents()
#
# The loop below needs no extra library: every id repeatedly takes the smallest
# id it can reach, until nothing changes. Fine for the chain lengths that member
# data produces. Checkpoint inside the loop if you hit lineage/memory problems.
linked = scored.filter(F.col("score") >= MERGE_AT)
edges = (linked.select(F.col("id_x").alias("src"), F.col("id_y").alias("dst"))
         .union(linked.select(F.col("id_y").alias("src"), F.col("id_x").alias("dst"))))

groups = edges.select(F.col("src").alias("id")).distinct().withColumn("grp", F.col("id"))
while True:
    neighbours = (edges.join(groups, edges.src == groups.id)
                       .select(F.col("dst").alias("id"), F.col("grp")))
    spread = groups.union(neighbours).groupBy("id").agg(F.min("grp").alias("grp")).cache()
    if spread.join(groups, ["id", "grp"]).count() == spread.count():
        break
    groups = spread
groups = spread

# 1001, 2087, 3342 -> group 1001.   4410 is in no group at all.


# ------------------------------------------------------------- 5. merge ---
# Keep every record, grouped or not: a left join, so singletons survive.
# Keep the original ids, so a wrong merge can be reversed.
# Field rule: longest non-null value wins ("Maria Gonzalez" over "M. Gonzalez").
longest = lambda c: F.element_at(F.sort_array(
    F.collect_list(F.struct(F.length(c).alias("len"), F.col(c).alias("val"))), False), 1)["val"]

# Groups held together by a weak link are the ones worth a human look.
weak = (linked.filter(F.col("score") < REVIEW_AT)
        .select(F.explode(F.array("id_x", "id_y")).alias("id"))
        .join(groups, "id").select("grp").distinct()
        .withColumn("weak_link", F.lit(True)))

merged = (members
    .join(groups, "id", "left")
    .withColumn("grp", F.coalesce("grp", "id"))      # ungrouped record = its own group
    .groupBy("grp").agg(
        longest("name").alias("name"),
        F.max("dob").alias("dob"),
        longest("addr").alias("addr"),
        F.sort_array(F.collect_list("id")).alias("old_ids"),
        F.count("*").alias("n"))
    .join(weak, "grp", "left")
    # eyeball a group if it is 3+ records, or if a weak pair holds it together
    .withColumn("needs_review", (F.col("n") >= 3) | F.coalesce("weak_link", F.lit(False)))
    .drop("n", "weak_link"))

print("merged records:")
merged.show(truncate=False)
# 1001 | maria gonzalez | 1961-04-12 | 12 oak st apt 4 | [1001, 2087, 3342] | true
# 4410 | maria gomez    | 1975-07-30 | 98 elm ave      | [4410]             | false
