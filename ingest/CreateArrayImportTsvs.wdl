version 1.0

workflow CreateArrayImportTsvs {

  input {
    File input_vcf
    String probe_info_table
    File sampleMap

    Int? preemptible_tries
    File? gatk_override
  }
 
  call CreateImportTsvs {
    input:
      input_vcf = input_vcf,
      probe_info_table = probe_info_table,
      sampleMap = sampleMap,
      gatk_override = gatk_override,
      preemptible_tries = preemptible_tries
  }
  output {
    File metadata_tsv = CreateImportTsvs.metadata_tsv
    File arraydata_tsv = CreateImportTsvs.arraydata_tsv
  }
}


task CreateImportTsvs {
  input {
    File input_vcf
    String probe_info_table 
    File sampleMap

    # runtime
    Int? preemptible_tries
    File? gatk_override
  }

  Int disk_size = ceil(size(input_vcf, "GB") * 2.5) + 20

  meta {
    description: "Creates a tsv file for imort into BigQuery"
  }
  parameter_meta {
    input_vcf: {
      localization_optional: true
    }
  }
  command <<<
      set -e

      export GATK_LOCAL_JAR=~{default="/root/gatk.jar" gatk_override}

      gatk --java-options "-Xmx2500m" CreateArrayIngestFiles \
        -V ~{input_vcf} \
        --probe-info ~{probe_info_table} \
        -SNM ~{sampleMap} \
        --ref-version 37
  >>>
  runtime {
      docker: "us.gcr.io/broad-gatk/gatk:4.1.7.0"
      memory: "5 GB"
      disks: "local-disk " + disk_size + " HDD"
      preemptible: select_first([preemptible_tries, 5])
      cpu: 2
  }
  output {
      File metadata_tsv = glob("metadata_*.tsv")[0]
      File arraydata_tsv = glob("raw_*.tsv")[0] 
  }
}

