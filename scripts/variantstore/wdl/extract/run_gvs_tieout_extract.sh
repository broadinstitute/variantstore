PROJECT="spec-ops-aou"
DATASET="gvs_tieout_acmg_v1"

python create_cohort_extract_data_table.py \
  --fq_petvet_dataset ${PROJECT}.${DATASET} \
  --fq_temp_table_dataset ${PROJECT}.temp_tables \
  --fq_destination_dataset ${PROJECT}.${DATASET} \
  --destination_table exported_cohort_all_samples \
  --fq_cohort_sample_names ${PROJECT}.${DATASET}.sample_info \
  --query_project ${PROJECT} \
  --fq_sample_mapping_table ${PROJECT}.${DATASET}.sample_info
