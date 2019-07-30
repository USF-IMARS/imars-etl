#!/bin/bash
# this convenience script uses backfill_airflow_db.py to fill in all
# currently DAGS with corresponding product_id and area considerations.

PY_SCRIPT='python3 ./backfill_airflow_db.py'  # cmd to exec prefill_airflow_db.py

# ==========================================================================
# === worldview classification images
# ==========================================================================
DAG_PRE=processing_wv_classification_
COMMON_SQL='product_id=40'
$PY_SCRIPT ${DAG_PRE}big_bend    "$COMMON_SQL AND area_id=6"
$PY_SCRIPT ${DAG_PRE}fl_se       "$COMMON_SQL AND area_id=7"
$PY_SCRIPT ${DAG_PRE}fl_ne       "$COMMON_SQL AND area_id=8"
$PY_SCRIPT ${DAG_PRE}monroe      "$COMMON_SQL AND area_id=9"
$PY_SCRIPT ${DAG_PRE}panhandle   "$COMMON_SQL AND area_id=10"
$PY_SCRIPT ${DAG_PRE}west_fl_pen "$COMMON_SQL AND area_id=11"
# TODO: + other regions & wv3 products
# ==========================================================================
