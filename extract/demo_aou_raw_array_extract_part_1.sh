python raw_array_cohort_extract.py \
  --dataset spec-ops-aou.aou_pmi_synthetic_100k \
  --max_tables 25 \
  --fq_destination_table spec-ops-aou.aou_pmi_synthetic_100k.demo_100_cohort_extract \
  --query_project spec-ops-aou \
  --fq_cohort_sample_mapping_table spec-ops-aou.aou_pmi_synthetic_100k.cohort_100_of_100k_sample_list 
