GATK_HOME=~/projects/gatk

project_id="spec-ops-aou"
probe_info_table="spec-ops-aou.ah_aou_test.probe_info"
sample_table="spec-ops-aou.ah_aou_test.cohort_1"
cohort_table="spec-ops-aou.ah_aou_test.extract_1"
qc_metrics_table="spec-ops-aou.ah_aou_test.metrics_table"
output_vcf="demo_1.aou.vcf.gz"

# local versions for faster testing iterations
# TODO: gs:// based reference is hundreds of times slower than local... doesn't feel right
reference="/Users/kcibul/seq/references//hg19/v0/Homo_sapiens_assembly19.fasta"
#reference="gs://broad-references/hg19/v0/Homo_sapiens_assembly19.fasta"

# local versions for faster testing iterations
#probe_info_csv="/Users/kcibul/projects/gatk/probe_info.csv"
#PROBE_INFO_CLAUSE="--probe-info-csv ${probe_info_csv} "

# cloud/bigquery versions
PROBE_INFO_CLAUSE="--probe-info-table ${probe_info_table} "

#PROBE_RANGE_CLAUSE="--min-probe-id 10 --max-probe-id 10"
PROBE_RANGE_CLAUSE=""

#DEBUG_CLAUSE="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
DEBUG_CLAUSE=""

local_sort_max_records_in_ram="10000000"

$GATK_HOME/gatk --java-options "-Xmx4g $DEBUG_CLAUSE" ArrayExtractCohort \
  -R "${reference}" \
  -O "${output_vcf}" \
    ${PROBE_INFO_CLAUSE} \
  --cohort-sample-table "${sample_table}" \
  --qc-metrics-table "${qc_metrics_table}" \
  --use-compressed-data "false" \
  --cohort-extract-table "${cohort_table}" \
  --local-sort-max-records-in-ram "${local_sort_max_records_in_ram}" \
  ${PROBE_RANGE_CLAUSE} \
  --project-id "${project_id}"