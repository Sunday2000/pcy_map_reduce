#!/usr/bin/env bash
set -e
SALES=${1:-data/sales.csv}
ITEMS=${2:-data/items.csv}
export PCY_SUPPORT=${PCY_SUPPORT:-100}
export PCY_BUCKETS=${PCY_BUCKETS:-100000}

echo ">>> 1. Envoi des donnees dans HDFS"
sudo docker cp "$SALES" namenode:/data/sales.csv
sudo docker cp "$ITEMS" namenode:/data/items.csv
sudo docker exec namenode bash -lc '
  export HADOOP_HOME=${HADOOP_HOME:-$(ls -d /opt/hadoop-* 2>/dev/null | head -1)}
  export PATH="$HADOOP_HOME/bin:$PATH"
  hdfs dfsadmin -safemode leave || true
  hdfs dfs -mkdir -p /input
  hdfs dfs -put -f /data/sales.csv /input/sales.csv
  hdfs dfs -put -f /data/items.csv /input/items.csv
  hdfs dfs -rm -r -f /output_pcy
'

echo ">>> 2. Envoi des scripts Spark dans spark (conteneur)"
sudo docker cp spark_pcy.py  spark:/spark_pcy.py
sudo docker cp pcy_common.py spark:/pcy_common.py

echo ">>> 3. Lancement Spark (broadcast du bitmap inclus)"
sudo docker exec -e PCY_SUPPORT=$PCY_SUPPORT -e PCY_BUCKETS=$PCY_BUCKETS spark bash -lc '
  /spark/bin/spark-submit --py-files /pcy_common.py /spark_pcy.py \
    hdfs://namenode:8020/input/sales.csv \
    hdfs://namenode:8020/input/items.csv \
    hdfs://namenode:8020/output_pcy
'

echo ">>> 4. Resultat (top 30) :"
sudo docker exec namenode bash -lc 'export PATH="$(ls -d /opt/hadoop-* | head -1)/bin:$PATH"; hdfs dfs -cat /output_pcy/part-* | sort -t$'"'"'\t'"'"' -k2 -nr | head -30'