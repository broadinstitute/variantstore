#!/usr/bin/env bash

GATK_HOME=~/projects/gatk

python raw_array_cohort_extract.py \
  --dataset spec-ops-aou.ms_test \
  --fq_destination_table spec-ops-aou.ms_test.metrics_extract \
  --query_project spec-ops-aou \
  --fq_cohort_sample_mapping_table spec-ops-aou.ms_test.sample_list \
  --extract_genotype_counts_only true


$GATK_HOME/gatk --java-options "-Xmx4g" ArrayCalculateMetrics \
  --genotype-counts-table spec-ops-aou.ms_test.metrics_extract \
  --output gs://broad-dsp-spec-ops/scratch/mshand/JointGenotyping/arrays/metrics.tsv


bq load --location=US --project_id=spec-ops-aou --skip_leading_rows=1 --source_format=CSV -F "\t" ms_test.metrics_table gs://broad-dsp-spec-ops/scratch/mshand/JointGenotyping/arrays/metrics.tsv metrics_schema.json