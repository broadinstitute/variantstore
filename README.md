# variantstore
A scalable and efficient solution to storing and accessing genomic variants

## Ingest
### Array manifest
First you need to load the extended manifest file for your array. Both the manifest file and the schema file need to be on your local filesystem.

`./ingest/bq_ingest_arrays_manifest.sh <project-id> <dataset-name> <table-name> <ext-manifest-file> <manifest-schema>`

The dataset will be created if it doesn't exist. The table should not exist or duplicate data will be loaded. For manifest-schema, specify "manifest_schema.json". (TODO default the manifest file) For example: `./ingest/bq_ingest_arrays_manifest.sh spec-ops-aou aou_arrays_test probe_info GDA-8v1-0_A1.1.5.extended.csv manifest-schema.json`

### Array Data
There are several steps to ingest the vcf array data into BigQuery.

To get around BigQuery query limits, you should extract the probe_info table to a csv file in a google bucket. It will be used as input to the array ingest command.


Assign a sequential integer id for each sample. If you want to process several samples, you can ceate a csv file where the first column is the integer id and the second column is the sample name. For example:

	1,204126160095_R01C01
	2,204126160095_R02C01
	3,204126160095_R03C01

Run the gatk ingest tool to convert the vcf file to 2 tsv files: one for the sample mapping and one for the array data. 

	./gatk CreateArrayIngestFiles -V <input-vcf> --probe-info-table <gs-location-of-probe-info-export> --ref-version 37

Copy the resulting tsv files to a google bucket for upload 

	gsutil cp <output-of-gatk-command>*.tsv gs://broad-dsp-spec-ops/scratch/import/

Both of these steps (the gatk tool and the copy of the files) can be accomplished by running the CreateArrayImportTsvs.wdl script.

Run the bq ingest script for array data. This script will import the sample and raw array data files for the table specified and then move the files to a "done" subdirectory.

	./ingest/bq_ingest_arrays.sh <project-id> <dataset-name> <storage-location> <table-number>
	
For example:

	./ingest/bq_ingest_arrays.sh spec-ops-aou aou_arrays_test gs://broad-dsp-spec-ops/scratch/import 2

_**WARNING**_ 

It is important that new files are not being added to this directory during this process or they might be moved to the done directory without being processed. It is important not to reload the same file more than once or you will get duplicate entries in the database. 


## Extract

Here is a sample query you can use to create a cohort table for the samples you want to extract. (Soon there will be an option to pass this as a tsv).

	CREATE OR REPLACE TABLE `spec-ops-aou.aou_preprod.cohort_20` AS
		SELECT sample_id, sample_name FROM
		(
  			SELECT sample_id, sample_name, RAND() as x
  			FROM `spec-ops-aou.aou_preprod.sample_list`
  			ORDER BY x
  			LIMIT 20
		)
		
Once you have created a cohort table, you can run the `extract/raw_array_cohort_extract.wdl` with the `raw_array_cohort_extract.aou_demo_10.cloud.json` file as an example of the inputs needed. This will create a temp table with the cohort data and create an output vcf for each shard in the export.

## Issues
When running ./ingest/bq_ingest_array.sh the `gsutil mv` command at the end uses the `-m` option to multi-thread the move. Locally, I get this error (I think my python environment has problems). But I have been able to run the command with this option on from a web console. This should not be an error, but if so, we can always remove the `-m` option. If this error does occur, it just means not all of the ingested files are moved to the done directory. 

	Exception in thread Thread-5:
	Traceback (most recent call last):
	  File "/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/python3.7/multiprocessing/managers.py", line 811, in _callmethod
	    conn = self._tls.connection
	AttributeError: 'ForkAwareLocal' object has no attribute 'connection'
	
	During handling of the above exception, another exception occurred:
	
	Traceback (most recent call last):
	  File "/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/python3.7/threading.py", line 926, in _bootstrap_inner
	    self.run()
	  File "/Users/ahaessly/google-cloud-sdk/platform/gsutil/gslib/command.py", line 2348, in run
	    cls = copy.copy(class_map[caller_id])
	  File "<string>", line 2, in __getitem__
	  File "/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/python3.7/multiprocessing/managers.py", line 815, in _callmethod
	    self._connect()
	  File "/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/pyException in thread Thread-5:
	Traceback (most recent call last):
	  File "/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/python3.7/multiprocessing/managers.py", line 811, in _callmethod
	    conn = self._tls.connection
	AttributeError: 'ForkAwareLocal' object has no attribute 'connection'
	
	During handling of the above exception, another exception occurred:
	
	Traceback (most recent call last):
	  File "/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/python3.7/threading.py", line 926, in _bootstrap_inner
	    self.run()
	  File "/Users/ahaessly/google-cloud-sdk/platform/gsutil/gslib/command.py", line 2348, in run
	    cls = copy.copy(class_map[caller_id])
	  File "<string>", line 2, in __getitem__
	  File "/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/python3.7/multiprocessing/managers.py", line 815, in _callmethod
	    self._connect()
	  File "/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/python3.7/multiprocessing/managers.py", line 802, in _connect
	    conn = self._Client(self._token.address, authkey=self._authkey)
	  File "/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/python3.7/multiprocessing/connection.py", line 492, in Client
	    c = SocketClient(address)
	  File "/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/python3.7/multiprocessing/connection.py", line 619, in SocketClient
	    s.connect(address)
	ConnectionRefusedError: [Errno 61] Connection refused
	thon3.7/multiprocessing/managers.py", line 802, in _connect
	    conn = self._Client(self._token.address, authkey=self._authkey)
	  File "/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/python3.7/multiprocessing/connection.py", line 492, in Client
	    c = SocketClient(address)
	  File "/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/python3.7/multiprocessing/connection.py", line 619, in SocketClient
	    s.connect(address)
	ConnectionRefusedError: [Errno 61] Connection refused
