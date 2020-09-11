# -*- coding: utf-8 -*-
import sys
import uuid
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJobConfig

import argparse

JOB_IDS = set()

#
# CONSTANTS
#
RAW_ARRAY_TABLE_PREFIX = "arrays_"
SAMPLES_PER_PARTITION = 4000

MAX_PROBE_ID = 2000000
PROBES_PER_PARTITION = 100000 # 20 partitions

FINAL_TABLE_TTL = ""
#FINAL_TABLE_TTL = " OPTIONS( expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 72 HOUR)) "

RAW_ARRAY_TABLE_COUNT = -1
client = None

def utf8len(s):
    return len(s.encode('utf-8'))

def dump_job_stats():
  total = 0

  for jobid in JOB_IDS:
    job = client.get_job(jobid[1])

    bytes_billed = int(0 if job.total_bytes_billed is None else job.total_bytes_billed)
    total = total + bytes_billed

    print(jobid[0], " <====> Cache Hit:", job.cache_hit, bytes_billed/(1024 * 1024), " MBs")

  print(" Total GBs billed ", total/(1024 * 1024 * 1024), " GBs")

def execute_with_retry(label, sql):
  retry_delay = [30, 60, 90] # 3 retries with incremental backoff

  start = time.time()
  while len(retry_delay) > 0:
    try:
      query = client.query(sql)
      print(f"STARTING - {label}")
      JOB_IDS.add((label, query.job_id))
      results = query.result()
      print(f"COMPLETED ({time.time() - start} s, {3-len(retry_delay)} retries) - {label}")
      return results
    except Exception as err:

      # if there are no retries left... raise
      if (len(retry_delay) == 0):
        raise err
      else:
        t = retry_delay.pop(0)
        print(f"Error {err} running query {label}, sleeping for {t}")
        time.sleep(t)

def get_partition_range(i):
  if i < 1 or i > RAW_ARRAY_TABLE_COUNT:
    raise ValueError(f"out of partition range")

  return { 'start': (i-1)*SAMPLES_PER_PARTITION + 1, 'end': i*SAMPLES_PER_PARTITION }

def get_samples_for_partition(cohort, i):
  return [ s for s in cohort if s >= get_partition_range(i)['start'] and s <= get_partition_range(i)['end'] ]

def split_lists(samples, n):
  return [samples[i * n:(i + 1) * n] for i in range((len(samples) + n - 1) // n )]

def get_all_samples(fq_cohort_sample_mapping_table):
  sql = f"select sample_id from `{fq_cohort_sample_mapping_table}`"
      
  results = execute_with_retry("read cohort table", sql)    
  cohort = [row.sample_id for row in list(results)]
  cohort.sort()
  return cohort

def populate_extract_table(fq_dataset, cohort, fq_destination_table, extract_genotype_counts_only):
  def get_subselect(fq_array_table, samples, id, extract_genotype_counts_only):
    fields_to_extract = "sample_id, probe_id, GT_encoded" if extract_genotype_counts_only else "sample_id, probe_id, GT_encoded, NORMX, NORMY, BAF, LRR"
    sample_stanza = ','.join([str(s) for s in samples])
    sql = f"    q_{id} AS (SELECT {fields_to_extract} from `{fq_array_table}` WHERE sample_id IN ({sample_stanza})), "
    return sql
   
  subs = {}
  for i in range(1, RAW_ARRAY_TABLE_COUNT+1):
    partition_samples = get_samples_for_partition(cohort, i)

    fq_array_table = f"{fq_dataset}.{RAW_ARRAY_TABLE_PREFIX}{i:03}"
    if len(partition_samples) > 0:
      j = 1
      for samples in split_lists(partition_samples, 1000):
        id = f"{i}_{j}"
        subs[id] = get_subselect(fq_array_table, samples, id, extract_genotype_counts_only)
        j = j + 1

  # ref vs alt allele doesn't matter for HWE or call rate
  select_sql = (
                f" (SELECT probe_id, " +
                f"COUNT(IF(GT_encoded LIKE 'AA', Sample_id, null)) hom_ref, \n" +
                f"COUNT(IF(GT_encoded LIKE 'AB', Sample_id, null)) het, \n" +
                f"COUNT(IF(GT_encoded LIKE 'BB', Sample_id, null)) hom_var, \n" +
                f"COUNT(IF(GT_encoded LIKE '.', Sample_id, null)) no_call \n" +
                f"FROM q_all \n" +
                f"GROUP BY probe_id)"
  ) if extract_genotype_counts_only else f" (SELECT * FROM q_all)"

  sql = (
        f"CREATE OR REPLACE TABLE `{fq_destination_table}` \n"
        f"PARTITION BY RANGE_BUCKET(probe_id, GENERATE_ARRAY(0, {MAX_PROBE_ID}, {PROBES_PER_PARTITION})) \n"
        f"{FINAL_TABLE_TTL} "
        f"AS \n" +
        f"with\n" +
        ("\n".join(subs.values())) + "\n"
        "q_all AS (" + (" union all ".join([ f"(SELECT * FROM q_{id})" for id in subs.keys()])) + ")\n" +
        f"{select_sql}"
        )

  print(sql) 
  print(f"Extract Query is {utf8len(sql)/(1024*1024)} MB in length")  
  results = execute_with_retry("create extract table table", sql)    
  return results

def do_extract(fq_dataset,
               max_tables,
               query_project,
               fq_destination_table,
               fq_cohort_sample_mapping_table,
               extract_genotype_counts_only
              ):
  try:  
    global client
    client = bigquery.Client(project=query_project, 
                             default_query_job_config=QueryJobConfig(priority="INTERACTIVE", use_query_cache=False ))

    global RAW_ARRAY_TABLE_COUNT
    RAW_ARRAY_TABLE_COUNT = max_tables
    print(f"Using {RAW_ARRAY_TABLE_COUNT} tables in {fq_dataset}...")

    cohort = get_all_samples(fq_cohort_sample_mapping_table)
    print(f"Discovered {len(cohort)} samples in {fq_cohort_sample_mapping_table}...")

    populate_extract_table(fq_dataset, cohort, fq_destination_table, extract_genotype_counts_only)

  except Exception as err:
    print(err)

  dump_job_stats()
  print(f"\nFinal cohort extract written to {fq_destination_table}\n")

if __name__ == '__main__':
  parser = argparse.ArgumentParser(allow_abbrev=False, description='Extract a raw array cohort from BigQuery Variant Store ')
  
  parser.add_argument('--dataset',type=str, help='project.dataset location of raw array data', required=True)
  parser.add_argument('--fq_destination_table',type=str, help='fully qualified destination table', required=True)
  parser.add_argument('--query_project',type=str, help='Google project where query should be executed', required=True)
  parser.add_argument('--fq_cohort_sample_mapping_table',type=str, help='Mapping table from sample_id to sample_name for the extracted cohort', required=True)
  parser.add_argument('--max_tables',type=int, help='Maximum number of array_xxx tables to consider', required=False, default=250)
  parser.add_argument('--extract_genotype_counts_only', type=bool, help='Extract only genoype counts for QC metric calculations', required=False, default=False)

  # Execute the parse_args() method
  args = parser.parse_args()

  do_extract(args.dataset,
             args.max_tables,
             args.query_project,
             args.fq_destination_table,
             args.fq_cohort_sample_mapping_table,
             args.extract_genotype_counts_only)
