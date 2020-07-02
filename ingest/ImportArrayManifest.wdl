version 1.0

workflow ImportArrayManifest {

  input {
    File extended_manifest_csv
    File manifest_schema_json
    String project_id
    String dataset_name
    String? table_name
 
    Int? preemptible_tries
  }
 
  call CreateManifestCsv {
    input:
      extended_manifest_csv = extended_manifest_csv,
      preemptible_tries = preemptible_tries
  }

  call LoadManifest {
    input:
      project_id = project_id,
      dataset_name = dataset_name,
      table_name = table_name,
      manifest_schema_json = manifest_schema_json,
      manifest_csv = CreateManifestCsv.manifest_csv,
      preemptible_tries = preemptible_tries
  }
  output {
    File manifest_csv = CreateManifestCsv.manifest_csv
    File manifest_ingest_csv = CreateManifestCsv.manifest_ingest_csv
    File manifest_sub_csv = CreateManifestCsv.manifest_sub_csv
    File manifest_proc_csv = CreateManifestCsv.manifest_proc_csv
  }
}

task LoadManifest {
  input {
    String project_id
    String dataset_name
    String? table_name
    File manifest_csv
    File manifest_schema_json
    # runtime
    Int? preemptible_tries
  }

  String ingest_table = dataset_name + "." + select_first([table_name, "probe_info"])

  parameter_meta {
    manifest_schema_json: {
      localization_optional: false
    }
  }
   
    command <<<
      set +e
      bq ls --project_id ~{project_id} ~{dataset_name} > /dev/null
      if [ $? -ne 0 ]; then
        echo "making dataset ~{project_id}.~{dataset_name}"
        bq mk --project_id=~{project_id} ~{dataset_name}
      fi
      bq show --project_id ~{project_id} ~{ingest_table} > /dev/null
      if [ $? -ne 0 ]; then
        echo "making table ~{ingest_table}"
        bq --location=US mk --project_id=~{project_id} ~{ingest_table} ~{manifest_schema_json}
      fi
      set -e

      bq load --location=US --project_id=~{project_id} --source_format=CSV ~{ingest_table} ~{manifest_csv} ~{manifest_schema_json}
    >>>
    runtime {
      docker: "us.gcr.io/broad-gatk/gatk:4.1.7.0"
      memory: "4 GB"
      disks: "local-disk " + 20 + " HDD"
      preemptible: select_first([preemptible_tries, 5])
      cpu: 2
  }

}

task CreateManifestCsv {
  input {
    File extended_manifest_csv

    # runtime
    Int? preemptible_tries
  }

  Int disk_size = ceil(size(extended_manifest_csv, "GB") * 2.5) + 20

  meta {
    description: "Creates a tsv file for imort into BigQuery"
  }
  parameter_meta {
    extended_manifest_csv: {
      localization_optional: false
    }
  }
  command <<<
    set -e

    TMP_SORTED="manifest_ingest_sorted.csv"
    TMP_SUB="manifest_ingest_sub.csv"
    TMP_PROC="manifest_ingest_processed.csv"
    TMP="manifest_ingest.csv"

    sed 's/,X,/,23X,/g; s/,Y,/,24Y,/g; s/,MT,/,25MT,/g' ~{extended_manifest_csv} > $TMP_SUB
    sort -t , -k 22n,22 -k23n,23 $TMP_SUB > $TMP_SORTED
    # checking for != "build37Flag" skips the header row (we don't want that numbered)
    awk -F ',' 'NF==28 && ($28!="ILLUMINA_FLAGGED" && $28!="INDEL_NOT_MATCHED" && $28!="INDEL_CONFLICT" && $28!="build37Flag") { flag=$28; if ($28=="PASS") flag=""; print id++","$2","$9","$22","$23","$24","$25","$26","flag }' $TMP_SORTED > $TMP_PROC
    sed 's/,23X,/,X,/g; s/,24Y,/,Y,/g; s/,25MT,/,MT,/g' $TMP_PROC > $TMP
    echo "created file for ingest $TMP"

  >>>
  runtime {
      docker: "us.gcr.io/broad-gatk/gatk:4.1.7.0"
      memory: "4 GB"
      disks: "local-disk " + disk_size + " HDD"
      preemptible: select_first([preemptible_tries, 5])
      cpu: 2
  }
  output {
      File manifest_csv = "manifest_ingest.csv"
      File manifest_ingest_csv = "manifest_ingest_sorted.csv"
      File manifest_sub_csv = "manifest_ingest_sub.csv"
      File manifest_proc_csv = "manifest_ingest_processed.csv"
  
  }
}
