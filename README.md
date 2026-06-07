# TP4 — Frequent Itemsets (PCY) en MapReduce — Hadoop + Spark

Recherche des **produits souvent achetés ensemble** (paires fréquentes) dans
`sales.csv` du dataset ERP Kaggle, avec l'algorithme **PCY (Park–Chen–Yu)**
implémenté en MapReduce (Hadoop Streaming) et en Spark.

- **Panier** = `invoice_id` · **Article** = `product_id`
- Noms lisibles via `items.csv` (`product_id` → `product_name`)

## Fichiers

| Fichier | Rôle |
|---|---|
| `pcy_common.py` | Config (support, buckets) + hash déterministe partagé |
| `spark_pcy.py` | PCY complet en Spark (un seul script) |
| `run_spark.sh` | Lancement sur le cluster Docker |
| `GUIDE_TP4_PCY.md` | Guide pas à pas complet |
| `data/sales_sample.csv` / `data/items_sample.csv` | Échantillon (réponse connue) |

## Démarrage rapide

```bash
#Demarrer les conteurs docker
docker-compose up -d

# Sur le cluster, avec ton vrai sales.csv
PCY_SUPPORT=100 PCY_BUCKETS=100000 bash run_spark.sh  data/sales.csv data/items.csv
```

## Télécharger seulement sales.csv

```bash
pip install kaggle   # + token kaggle.json dans ~/.kaggle/ (chmod 600)
kaggle datasets download -d milootis/10-years-of-synthesized-erp-sales-data -f sales.csv -p ./data
kaggle datasets download -d milootis/10-years-of-synthesized-erp-sales-data -f items.csv -p ./data
cd data && unzip -o sales.csv.zip && unzip -o items.csv.zip && cd ..
```

## Paramètres (variables d'environnement)
- `PCY_SUPPORT` : seuil de support (défaut 3 pour l'échantillon ; ~100+ sur le vrai dataset)
- `PCY_BUCKETS` : taille de la table de hachage (défaut 1000 ; ~100000 sur le vrai dataset)
