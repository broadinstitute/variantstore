# variantstore
A scalable and efficient solution to storing and accessing genomic variants

## Ingest
### Array manifest
First you need to load the extended manifest file for your array. This script assumes the manifest file is on your local filesystem. (TODO add a flag to process file from google bucket)
`./bq_ingest_array_manifest.sh <project-id> <dataset-name> <table-name> <ext-manifest-file> <manifest-schema>`

The dataset will be created if it doesn't exist. The table should not exist or duplicate data will be loaded. For manifest-schema, specify "manifest_schema.json". (TODO default the manifest file) For example: `./bq_ingest_array_manifest.sh spec-ops-aou aou_arrays_test probe_info GDA-8v1-0_A1.1.5.extended.csv manifest-schema.json`

### Array Data
There are several steps to ingest the vcf array data into BigQuery.


Assign a sequential integer id for each sample. If you want to process several samples, you can ceate a csv file where the first column is the integer id and the second column is the sample name. For example:

	1,204126160095_R01C01
	2,204126160095_R02C01
	3,204126160095_R03C01

Run the gatk ingest tool to convert the vcf file to 2 tsv files: one for the sample mapping and one for the array data. 

	./gatk CreateArrayIngestFiles -V <input-vcf> --probe-info <fully-qualified-probe-info-table> --ref-version 37

Copy the resulting tsv files to a google bucket for upload under a subdirectory number that represents the NNN table it will be loaded into `((sample_id-1)/4000)+1` and a subdirectory of that named "ready". For example, if you just ran the tool for sample_id 4001:

	gsutil cp <output-of-gatk-command>*.tsv gs://broad-dsp-spec-ops/scratch/import/2/ready

Both of these steps (the gatk tool and the copy of the files) can be accomplished by running the CreateArrayImportTsvs.wdl script.

Run the bq ingest script for array data. This script will move the files from the "ready" directory to the "processing" directory, then import from the processing directory, then move the files to the "done" directory. This way the tool can be run to import while data is still being generated and put into the ready directory. Use the `--reprocess` flag if importing fails and you want to re-run the tool to import only the data already in the "processing" directory (without moving new files from the "ready" directory). It is important not to reload the same file more than once!

	./bq_ingest_arrays.sh <project-id> <dataset-name> <storage-location> <start-directory-id> [--reprocess]
	
For example:

	./bq_ingest_arrays.sh spec-ops-aou aou_arrays_test gs://broad-dsp-spec-ops/scratch/import 2



