#!/usr/bin/env bash

if [ $# -lt 5 ]; then
  echo "usage: $0 <project-id> <dataset-name> <table-name> <manifest-file> <manifest-schema>"
  exit 1
fi

PROJECT_ID=$1
DATASET_NAME=$2
TABLE=$3
MANIFEST_FILE=$4
MANIFEST_SCHEMA=$5
TMP_SORTED="/tmp/manifest_ingest_sorted.csv"
TMP_SUB="/tmp/manifest_ingest_sub.csv"
TMP_PROC="/tmp/manifest_ingest_processed.csv"
TMP="/tmp/manifest_ingest.csv"

# put integers in front of the chromosomes that are not numbered so that they end up ordered by X, Y and MT
sed 's/,X,/,23X,/g; s/,Y,/,24Y,/g; s/,MT,/,25MT,/g' $MANIFEST_FILE > $TMP_SUB

# sort the probes by chrom, position and then name so there is a specific ordering when we assign integers
sort -t , -k23n,23 -k24n,24 -k2,2 $TMP_SUB > $TMP_SORTED

# checking for != "build37Flag" skips the header row (we don't want that numbered)
# only process rows with 29 fields - this skips some header info fields
# also skip entries that are flagged, not matched or have index conflict
awk -F ',' 'NF==29 && ($29!="ILLUMINA_FLAGGED" && $29!="INDEL_NOT_MATCHED" && $29!="INDEL_CONFLICT" && $29!="build37Flag") { flag=$29; if ($29=="PASS") flag=""; print id++","$2","$9","$23","$24","$25","$26","$27","flag }' $TMP_SORTED > $TMP_PROC

# remove the integer prefixes for chromosomes X, Y and MT
sed 's/,23X,/,X,/g; s/,24Y,/,Y,/g; s/,25MT,/,MT,/g' $TMP_PROC > $TMP

echo "created file for ingest $TMP"

# create a dataset
bq mk --project_id=$PROJECT_ID $DATASET_NAME

# create a site info table and load - schema and TSV header need to be the same order
bq --location=US mk --project_id=$PROJECT_ID $DATASET_NAME.$TABLE $MANIFEST_SCHEMA
bq load --location=US --project_id=$PROJECT_ID --source_format=CSV $DATASET_NAME.$TABLE $TMP $MANIFEST_SCHEMA

echo "ingested manifest data into table $TABLE"
bq show --format=prettyjson --project_id=$PROJECT_ID $DATASET_NAME.$TABLE

