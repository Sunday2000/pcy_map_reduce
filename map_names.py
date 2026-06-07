#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
map_names.py — Traduit les product_id en noms lisibles via items.csv.

Lit le resultat des paires frequentes (passe 2) sur stdin, et remplace chaque
product_id par son product_name grace a items.csv.

Usage : cat pass2_out.txt | python3 map_names.py items.csv
Sortie : "Laptop || Mouse   5"
"""

import sys
import csv

items_path = sys.argv[1] if len(sys.argv) > 1 else "data/items_sample.csv"

# Charger la table product_id -> product_name
id2name = {}
with open(items_path, newline="") as f:
    reader = csv.reader(f)
    header = next(reader, None)  # ignorer l'en-tete
    for row in reader:
        if len(row) >= 2:
            id2name[row[0].strip()] = row[1].strip()


def name(pid):
    return id2name.get(pid, pid)  # si inconnu, garder l'id


for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue
    pair, count = line.split("\t", 1)
    a, b = [p.strip() for p in pair.split("||")]
    print(f"{name(a)} || {name(b)}\t{count}")
