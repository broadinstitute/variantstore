import math
import sys

initial_samples=117
replicates = 34
total_samples = initial_samples * replicates

print(f"requested {total_samples} from {initial_samples}, needs {replicates} replicates")

print("CREATE OR REPLACE TABLE `spec-ops-aou.aou_arrays_40k.sample_list` AS SELECT * FROM `spec-ops-aou.aou_arrays_test.sample_list`;");
bits = []
for r in range(1,replicates):
    bits.append(f"SELECT sample_name || \"_R{r:02}\", {initial_samples*r} + sample_id " \
          f"FROM `spec-ops-aou.aou_arrays_test.sample_list`\n ")

print(f"INSERT INTO `spec-ops-aou.aou_arrays_40k.sample_list` ")

print(" UNION ALL\n ".join(bits))
print(";")


print("CREATE OR REPLACE TABLE `spec-ops-aou.aou_arrays_40k.arrays_001` PARTITION BY RANGE_BUCKET(sample_id, GENERATE_ARRAY(1, 4000, 1)) AS SELECT * FROM `spec-ops-aou.aou_arrays_test.arrays_001`;");
bits = []
for r in range(1,replicates):
    bits.append(f"SELECT {initial_samples*r} + sample_id as sample_id, probe_id, filter, GT_encoded, NORMX, NORMY, BAF, LRR " \
          f"FROM `spec-ops-aou.aou_arrays_test.arrays_001`\n ")

print(f"INSERT INTO `spec-ops-aou.aou_arrays_40k.arrays_001` ")

print(" UNION ALL\n ".join(bits))
print(";")
    
