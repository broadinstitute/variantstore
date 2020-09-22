python raw_array_cohort_extract.py \
  --dataset spec-ops-aou.ah_aou_test \
  --max_tables 25 \
  --fq_destination_table spec-ops-aou.ah_aou_test.extract_1 \
  --query_project spec-ops-aou \
  --fq_cohort_sample_mapping_table spec-ops-aou.ah_aou_test.cohort_1 \
  --ttl 1000 \
  --number_of_partitions 2 \
  --probes_per_partition 1000000

ret=$?
echo "Exit Code $ret"
