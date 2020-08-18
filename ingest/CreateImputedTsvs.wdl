version 1.0

workflow CreateImputedTsvs {

  input {
    File imputed_vcf
    File imputed_vcf_index
    String sample_list_table
    File gatk_override
    
    Int? preemptible_tries
  }
 
  call CreateTsvs {
    input:
      imputed_vcf = imputed_vcf,
      imputed_vcf_index = imputed_vcf_index,
      sample_list_table = sample_list_table,
      gatk_override = gatk_override,
      preemptible_tries = preemptible_tries
  }

  output {
    Array[File] output_tsvs = CreateTsvs.tsvs
  }
}

task CreateTsvs {
  input {
    File imputed_vcf
    File imputed_vcf_index
    String sample_list_table
    File gatk_override

    # runtime
    Int? preemptible_tries
  }

  Int disk_size = ceil(size(imputed_vcf, "GB") * 2.5) + 20

  meta {
    description: "Creates a tsv file for imort into BigQuery"
  }
  parameter_meta {
    imputed_vcf: {
      localization_optional: false
    }
  }
  command <<<
      set -e

      export GATK_LOCAL_JAR=~{default="/root/gatk.jar" gatk_override}

      gatk --java-options "-Xmx6500m" CreateImputedIngestFiles \
        -V ~{imputed_vcf} \
        -SLT ~{sample_list_table}
        
  >>>
  runtime {
      docker: "us.gcr.io/broad-gatk/gatk:4.1.7.0"
      memory: "100 GB"
      disks: "local-disk " + disk_size + " HDD"
      preemptible: select_first([preemptible_tries, 5])
      cpu: 4
  }
  output {
    Array[File] tsvs = glob("*.tsv")
  }
}
