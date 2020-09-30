python raw_array_cohort_extract.py \
  --dataset spec-ops-aou.ah_aou_synthetic \
  --max_tables 25 \
  --fq_destination_table spec-ops-aou.temp_tables.demo_10_cohort_extract \
  --query_project spec-ops-aou \
  --fq_cohort_sample_mapping_table spec-ops-aou.ah_aou_synthetic.cohort_10_of_500_sample_list \
  --ttl 12 \
  --number_of_partitions 2 \
  --probes_per_partition 1000000

ret=$?
echo "Exit Code $ret"
