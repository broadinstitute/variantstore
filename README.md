# variantstore
A scalable and efficient solution to storing and accessing genomic variants

## Ingest
### Array manifest
First you need to load the extended manifest file for your array. This script assumes the manifest file is on your local filesystem. (TODO add a flag to process file from google bucket)
`./bq_ingest_array_manifest.sh <project-id> <dataset-name> <table-name> <ext-manifest-file> <manifest-schema>`

The dataset will be created if it doesn't exist. The table should not exist or duplicate data will be loaded. For manifest-schema, specify "manifest_schema.json". (TODO default the manifest file) For example: `./bq_ingest_array_manifest.sh spec-ops-aou aou_arrays_test probe_info GDA-8v1-0_A1.1.5.extended.csv manifest-schema.json`

### Array Data
There are several steps to ingest the vcf array data into BigQuery.

To get around BigQuery query limits, you should extract the probe_info table to a csv file in a google bucket. It will be used as input to the array ingest command.


Assign a sequential integer id for each sample. If you want to process several samples, you can ceate a csv file where the first column is the integer id and the second column is the sample name. For example:

	1,204126160095_R01C01
	2,204126160095_R02C01
	3,204126160095_R03C01

Run the gatk ingest tool to convert the vcf file to 2 tsv files: one for the sample mapping and one for the array data. 

	./gatk CreateArrayIngestFiles -V <input-vcf> --probe-info-table <gs-location-of-probe-info-export> --use-compressed-data true --ref-version 37

Copy the resulting tsv files to a google bucket for upload 

	gsutil cp <output-of-gatk-command>*.tsv gs://broad-dsp-spec-ops/scratch/import/

Both of these steps (the gatk tool and the copy of the files) can be accomplished by running the CreateArrayImportTsvs.wdl script.

Run the bq ingest script for array data. This script will import the metadata and sample files for the table specified and then move the files to a "done" subdirectory.

	./bq_ingest_arrays.sh <project-id> <dataset-name> <storage-location> <table-number>
	
For example:

	./bq_ingest_arrays.sh spec-ops-aou aou_arrays_test gs://broad-dsp-spec-ops/scratch/import 2

_**WARNING**_ 

It is important that new files are not being added to this directory during this process or they might be moved to the done directory without being processed. It is important not to reload the same file more than once or you will get duplicate entries in the database. 


