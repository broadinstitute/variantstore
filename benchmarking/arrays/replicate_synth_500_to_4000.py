import math
import sys

initial_samples = 500
replicates = 8
total_samples = initial_samples * replicates

SRC_DATASET="spec-ops-aou.ah_aou_synthetic"
DEST_DATASET="spec-ops-aou.aou_synthetic_100k"

print(f"requested {total_samples} from {initial_samples}, needs {replicates} replicates")

print(f"CREATE OR REPLACE TABLE `{DEST_DATASET}.sample_list` AS SELECT * FROM `{SRC_DATASET}.sample_list`;");
bits = []
for r in range(1,replicates):
    bits.append(f"SELECT * REPLACE (sample_name || \"_R{r:02}\" as sample_name, {initial_samples*r} + sample_id as sample_id) " \
          f"FROM `{SRC_DATASET}.sample_list`\n ")

print(f"INSERT INTO `{DEST_DATASET}.sample_list` ")

print(" UNION ALL\n ".join(bits))
print(";")


print(f"CREATE OR REPLACE TABLE `{DEST_DATASET}.arrays_001` PARTITION BY RANGE_BUCKET(sample_id, GENERATE_ARRAY(1, 4000, 1)) AS SELECT sample_id, probe_id, GT_encoded, NORMX, NORMY, BAF, LRR FROM `{SRC_DATASET}.arrays_001`;");
bits = []
for r in range(1,replicates):
    bits.append(f"SELECT {initial_samples*r} + sample_id as sample_id, probe_id, GT_encoded, NORMX, NORMY, BAF, LRR " \
          f"FROM `{SRC_DATASET}.arrays_001`\n ")

print(f"INSERT INTO `{DEST_DATASET}.arrays_001` ")

print(" UNION ALL\n ".join(bits))
print(";")
    
