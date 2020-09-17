version 1.0

workflow CreateImputedSiteTsv {

  input {
    File imputed_vcf
    File imputed_vcf_index
    File gatk_override
    
    Int? preemptible_tries
  }
 
  call CreateSiteTsv {
    input:
      imputed_vcf = imputed_vcf,
      imputed_vcf_index = imputed_vcf_index,
      gatk_override = gatk_override,
      preemptible_tries = preemptible_tries
  }

  output {
    File site_tsv = CreateSiteTsv.site_tsv
  }
}

task CreateSiteTsv {
  input {
    File imputed_vcf
    File imputed_vcf_index
    File gatk_override

    # runtime
    Int? preemptible_tries
  }

  Int disk_size = ceil(size(imputed_vcf, "GB") * 50) + 20

  meta {
    description: "Creates an imputed site info tsv file for imort into BigQuery"
  }
  parameter_meta {
    imputed_vcf: {
      localization_optional: false
    }
  }
  command <<<
      set -e

      export GATK_LOCAL_JAR=~{default="/root/gatk.jar" gatk_override}

      gatk --java-options "-Xmx6500m" CreateImputedSiteInfoFile \
        -V ~{imputed_vcf} 
        
  >>>
  runtime {
      docker: "us.gcr.io/broad-gatk/gatk:4.1.7.0"
      memory: "50 GB"
      disks: "local-disk " + disk_size + " HDD"
      preemptible: select_first([preemptible_tries, 5])
      cpu: 4
  }
  output {
    File site_tsv = glob("*.tsv")[0]
  }
}
