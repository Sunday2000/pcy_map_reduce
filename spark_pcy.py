#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import sys
import csv
import os
from pyspark.sql import SparkSession
from pcy_common import SUPPORT, NUM_BUCKETS, pair_bucket

# --- positions des colonnes de sales.csv ---
BASKET_IDX = int(os.environ.get("BASKET_IDX", "0"))  # invoice_id
ITEM_IDX = int(os.environ.get("ITEM_IDX", "3"))      # product_id
# -------------------------------------------

sales_path = sys.argv[1] if len(sys.argv) > 1 else "data/sales_sample.csv"
items_path = sys.argv[2] if len(sys.argv) > 2 else "data/items_sample.csv"
output_path = sys.argv[3] if len(sys.argv) > 3 else None

spark = SparkSession.builder.appName("PCY_frequent_pairs").getOrCreate()
sc = spark.sparkContext


def parse_sale(line):
    """Ligne CSV -> (invoice_id, product_id). None si en-tete/ligne sale."""
    try:
        row = next(csv.reader([line]))
        basket = row[BASKET_IDX].strip()
        item = row[ITEM_IDX].strip()
        int(item)  # product_id doit etre numerique -> ecarte l'en-tete
        return (basket, item)
    except (IndexError, ValueError, StopIteration):
        return None


# ---------- ETAPE 0 : former les paniers ----------
# (invoice, product) -> regrouper par invoice -> liste d'articles distincts
baskets = (
    sc.textFile(sales_path)
      .map(parse_sale)
      .filter(lambda x: x is not None)
      .groupByKey()
      .map(lambda kv: sorted(set(kv[1])))
      .filter(lambda items: len(items) >= 1)
)
baskets.cache()

# ---------- PASSE 1 ----------
# (a) compte des articles seuls
item_counts = (
    baskets.flatMap(lambda items: [(it, 1) for it in items])
           .reduceByKey(lambda a, b: a + b)
)
frequent_items = set(
    item_counts.filter(lambda kv: kv[1] >= SUPPORT).keys().collect()
)


def pairs(items):
    out = []
    n = len(items)
    for a in range(n):
        for b in range(a + 1, n):
            out.append((items[a], items[b]))
    return out


# (b) compte des buckets (l'astuce PCY)
bucket_counts = (
    baskets.flatMap(lambda items: [(pair_bucket(a, b), 1) for a, b in pairs(items)])
           .reduceByKey(lambda a, b: a + b)
)
frequent_buckets = set(
    bucket_counts.filter(lambda kv: kv[1] >= SUPPORT).keys().collect()
)

# ---------- entre-passes : broadcast vers tous les executors ----------
bc_items = sc.broadcast(frequent_items)
bc_buckets = sc.broadcast(frequent_buckets)

print("[PCY] SUPPORT={0} BUCKETS={1} | articles frequents={2} | "
      "buckets frequents={3}".format(SUPPORT, NUM_BUCKETS,
                                     len(frequent_items),
                                     len(frequent_buckets)))


# ---------- PASSE 2 : paires candidates (double condition PCY) ----------
def candidate_pairs(items):
    fi = bc_items.value
    fb = bc_buckets.value
    items = [it for it in items if it in fi]          # (1) articles frequents
    out = []
    for a, b in pairs(items):
        if pair_bucket(a, b) in fb:                   # (2) bucket frequent
            out.append(((a, b), 1))
    return out


frequent_pairs = (
    baskets.flatMap(candidate_pairs)
           .reduceByKey(lambda a, b: a + b)
           .filter(lambda kv: kv[1] >= SUPPORT)
           .sortBy(lambda kv: kv[1], ascending=False)
)

# ---------- traduction product_id -> product_name ----------
id2name = {}
try:
    for line in sc.textFile(items_path).collect():
        row = next(csv.reader([line]))
        try:
            int(row[0])          # ignorer l'en-tete
            id2name[row[0].strip()] = row[1].strip()
        except (ValueError, IndexError):
            continue
except Exception:
    pass


def fmt(kv):
    (a, b), c = kv
    na = id2name.get(a, a)
    nb = id2name.get(b, b)
    return "{0} || {1}\t{2}".format(na, nb, c)


resultat = frequent_pairs.map(fmt)

if output_path:
    resultat.saveAsTextFile(output_path)
    print(">>> Resultat ecrit dans {0}".format(output_path))
else:
    print("=== Paires frequentes (PCY) ===")
    for line in resultat.collect():
        print(line)

spark.stop()
