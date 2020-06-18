# variantstore
A scalable and efficient solution to storing and accessing genomic variants

## Ingest
### Array manifest
First you need to load the extended manifest file for your array. 
`./bq_ingest_array_manifest.sh <project-id> <dataset-name> <table-name> <ext-manifest-file> <manifest-schema>`

The dataset will be created if it doesn't exist. The table should not exist or duplicate data will be loaded. For manifest-schema, specify "manifest_schema.json". For example: `./bq_ingest_array_manifest.sh spec-ops-aou aou_arrays_test probe_info GDA-8v1-0_A1.1.5.extended.csv manifest-schema.json`

### Array Data
There are several steps to ingest the vcf array data into BigQuery.


Assign a sequential integer id for each sample. If you want to process several samples, you can ceate a csv file where the first column is the integer id and the second column is the sample name. 

	1,204126160095_R01C01
	2,204126160095_R02C01
	3,204126160095_R03C01

Run the gatk ingest tool to convert the vcf file to 2 tsv files: one for the sample mapping and one for the array data. 

	./gatk CreateArrayIngestFiles -V <input-vcf> --probe-info <fully-qualified-probe-info-table> --ref-version 37

Copy the resulting tsv files to a google bucket for upload.

These can both be accomplished by running the CreateArrayImportTsvs.wdl script.

Run the bq ingest script for array data.

	./bq_ingest_arrays.sh <project-id> <dataset-name> <storage-location> <start-directory-id> [--reprocess]



