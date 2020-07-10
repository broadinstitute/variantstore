#!/usr/bin/env bash
set -e
set -x

create_table_and_load_data () {
    TABLE=$1
    SCHEMA=$2
    UPLOAD_FILES=$3
    RANGE_PARTITIONING=""
    if [ $# -eq 4 ]; then
        RANGE_PARTITIONING=$4
    fi

    set +e
    bq ls --project_id $PROJECT_ID $DATASET_NAME > /dev/null
    if [ $? -ne 0 ]; then
        echo "making dataset $DATASET_NAME"
        bq mk --project_id=$PROJECT_ID $DATASET_NAME
    fi
    bq show --project_id $PROJECT_ID $TABLE > /dev/null
    echo "$?"
    if [ $? -ne 0 ]; then
        echo "making table $TABLE"
        bq --location=US mk $RANGE_PARTITIONING --project_id=$PROJECT_ID $TABLE $SCHEMA
    fi
    set -e
    bq load --location=US --project_id=$PROJECT_ID --skip_leading_rows=1 --null_marker="null" --source_format=CSV -F "\t" $TABLE $UPLOAD_FILES $SCHEMA
    echo "ingested all $UPLOAD_FILES into table $TABLE"
}

move_files () {
    FROM_DIR=$1
    TO_DIR=$2
    set +e
    gsutil -q -m mv ${FROM_DIR}${PET_WITH_ID}_* ${TO_DIR}
    gsutil -q -m mv ${FROM_DIR}${VET_WITH_ID}_* ${TO_DIR}
    gsutil -q -m mv ${FROM_DIR}${METADATA_WITH_ID}_* ${TO_DIR}
    set -e
}


if [ $# -lt 5 ]; then
  echo "usage: $0 <project-id> <dataset-name> <storage-location> <table-id-start> <table-id-end> [--reprocess]"
  exit 1
fi

PROJECT_ID=$1
DATASET_NAME=$2
STORAGE_LOCATION=$3
START_ID=$4
END_ID=$5
READY_DIR=$STORAGE_LOCATION/ready/
PROCESSING_DIR=$STORAGE_LOCATION/processing/
DONE_DIR=$STORAGE_LOCATION/done/

if [ $# -eq 6 -a "$6" = "--reprocess" ]; then
  REPROCESS=true
fi


PET_PREFIX="pet_"
VET_PREFIX="vet_"
METADATA_PREFIX="metadata_"

# schema and TSV header need to be the same order
PET_SCHEMA="pet_schema.json"
VET_SCHEMA="vet_schema.json"
SAMPLE_LIST_SCHEMA="sample_list_schema.json"
 
for ID in $(seq $START_ID $END_ID)
do
    echo $ID
    let PARTITION_START=($ID-1)*4000+1
    let PARTITION_END=$PARTITION_START+3999

    printf -v PADDED_TABLE_ID "%03d" $ID
    echo "processing $PADDED_TABLE_ID"

    PET_WITH_ID="pet_"$PADDED_TABLE_ID
    VET_WITH_ID="vet_"$PADDED_TABLE_ID
    METADATA_WITH_ID="metadata_"$PADDED_TABLE_ID

    if [ -z $REPROCESS ]; then
        move_files ${READY_DIR} ${PROCESSING_DIR}
    else
        echo "NOT moving files from ready! just reprocessing from the processing directory"
    fi

    NUM_PET_FILES=$(gsutil ls "${PROCESSING_DIR}${PET_WITH_ID}_*" | wc -l)
    NUM_VET_FILES=$(gsutil ls "${PROCESSING_DIR}${VET_WITH_ID}_*" | wc -l)
    NUM_METADATA_FILES=$(gsutil ls "$PROCESSING_DIR${METADATA_WITH_ID}_*" | wc -l)

   # create a metadata table and load
    if [ $NUM_METADATA_FILES -gt 0 ]; then
        create_table_and_load_data $DATASET_NAME.sample_list $SAMPLE_LIST_SCHEMA "${PROCESSING_DIR}${METADATA_WITH_ID}*"
    else
        echo "no metadata files to process"
    fi

    # create pet table
     if [ $NUM_PET_FILES -gt 0 ]; then
        create_table_and_load_data $DATASET_NAME.$PET_WITH_ID $PET_SCHEMA "${PROCESSING_DIR}${PET_WITH_ID}*" "--range_partitioning=sample_id,$PARTITION_START,$PARTITION_END,1"
    else
        echo "no pet data files to process"
    fi

    # create vet table
    if [ $NUM_VET_FILES -gt 0 ]; then
        create_table_and_load_data $DATASET_NAME.$VET_WITH_ID $VET_SCHEMA "${PROCESSING_DIR}${VET_WITH_ID}*" "--range_partitioning=sample_id,$PARTITION_START,$PARTITION_END,1"
    else
        echo "no vet data files to process"
    fi


    echo "moving files from processing to done"
    move_files $PROCESSING_DIR $DONE_DIR 

done

