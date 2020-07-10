version 1.0

workflow CreateExomeImportTsvs {

  input {
    File input_vcf
    File sampleMap
    File interval_list
    String output_directory

    Int? preemptible_tries
    File? gatk_override
  }
 
  call CreateImportTsvs {
    input:
      input_vcf = input_vcf,
      sampleMap = sampleMap,
      interval_list = interval_list,
      output_directory = output_directory,
      gatk_override = gatk_override,
      preemptible_tries = preemptible_tries
  }
  output {
    File metadata_tsv = CreateImportTsvs.metadata_tsv
    File pet_tsv = CreateImportTsvs.pet_tsv
    File vet_tsv = CreateImportTsvs.vet_tsv
  }
}


task CreateImportTsvs {
  input {
    File input_vcf
    File sampleMap
    File interval_list
    String output_directory

    # runtime
    Int? preemptible_tries
    File? gatk_override
  }

  Int disk_size = ceil(size(input_vcf, "GB") * 2.5) + 20

  meta {
    description: "Creates a tsv file for import into BigQuery"
  }
  parameter_meta {
    input_vcf: {
      localization_optional: true
    }
  }
  command <<<
      set -e

      export GATK_LOCAL_JAR=~{default="/root/gatk.jar" gatk_override}

      gatk --java-options "-Xmx2500m" CreateVariantIngestFiles \
        -V ~{input_vcf} \
        -SNM ~{sampleMap} \
        -L ~{interval_list} \
        --ref-version 38

        gsutil cp *.tsv ~{output_directory}/ready/
  >>>
  runtime {
      docker: "us.gcr.io/broad-gatk/gatk:4.1.7.0"
      memory: "5 GB"
      disks: "local-disk " + disk_size + " HDD"
      preemptible: select_first([preemptible_tries, 5])
      cpu: 2
  }
  output {
      File pet_tsv = glob("pet_*.tsv")[0]
      File vet_tsv = glob("vet_*.tsv")[0] 
      File metadata_tsv = glob("metadata_*.tsv")[0] 
  }
}