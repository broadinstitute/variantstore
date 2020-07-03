PROJECT_ID=spec-ops-aou
DATASET_NAME=kris_aou_test
TABLE_NAME=probe_info

# cleanup
#gsutil rm gs://spec-ops-aou/kcibul/import/1/done/*
#bq rm $PROJECT_ID:$DATASET_NAME.probe_info
#bq rm $PROJECT_ID:$DATASET_NAME.sample_list
#bq rm $PROJECT_ID:$DATASET_NAME.arrays_001

# copy array extended manifest from cloud storage
gsutil cp gs://fc-b79a5ca7-28d7-48f4-8f92-d69e0c48a3ed/PDO-21032_85_ColorPGx_eMERGE_samples_03202020/GDA-8v1-0_A1.1.5.extended.csv .
./ingest/bq_ingest_arrays_manifest.sh $PROJECT_ID $DATASET_NAME $TABLE_NAME GDA-8v1-0_A1.1.5.extended.csv ingest/manifest_schema.json

# Make the sample manifest
echo "1,204126160095_R01C01" > sample_map.csv

# Stage probe_info from bucket
gsutil cp gs://spec-ops-aou/probe_info.csv .

#INGEST_PROBE_CLAUSE="--probe-info $PROJECT_ID.$DATASET_NAME.probe_info"
INGEST_PROBE_CLAUSE="--probe-info-csv probe_info.csv"

./gatk CreateArrayIngestFiles --sample-name-mapping sample_map.csv -V 204126160095_R01C01.vcf.gz $INGEST_PROBE_CLAUSE --ref-version 37

gsutil cp metadata_001_204126160095_R01C01.tsv gs://spec-ops-aou/kcibul/import/1/ready/
gsutil cp raw_001_204126160095_R01C01.tsv gs://spec-ops-aou/kcibul/import/1/ready/

cd ingest
./bq_ingest_arrays.sh $PROJECT_ID $DATASET_NAME gs://spec-ops-aou/kcibul/import 1 

OUTPUT_VCF="kris.aou.1.vcf"
REF="/Users/kcibul/seq/references//hg19/v0/Homo_sapiens_assembly19.fasta"

#EXTRACT_PROBE_CLAUSE="--probe-info-table $PROJECT_ID.$DATASET_NAME.probe_info"
EXTRACT_PROBE_CLAUSE="--probe-info-csv probe_info.csv"

./gatk ArrayExtractCohort -R "${REF}" \
   -O $OUTPUT_VCF \
   --use-compressed-data "true" \
   --cohort-extract-table $PROJECT_ID.$DATASET_NAME.arrays_001 \
   --local-sort-max-records-in-ram 10000000 \
   --sample-info-table $PROJECT_ID.$DATASET_NAME.sample_list \
   $EXTRACT_PROBE_CLAUSE \
   --project-id $PROJECT_ID
