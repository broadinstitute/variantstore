version 1.0

workflow CreateSitesTsv {

  input {
    File sites_only_vcf
    File sites_only_index
    File gatk_override
    
    Int? preemptible_tries
  }
 
  call CreateTsv {
    input:
      sites_only_vcf = sites_only_vcf,
      sites_only_index = sites_only_index,
      gatk_override = gatk_override,
      preemptible_tries = preemptible_tries
  }

  output {
    File sites_tsv = CreateTsv.sitesonly_tsv
  }
}

task CreateTsv {
  input {
    File sites_only_vcf
    File sites_only_index
    File gatk_override

    # runtime
    Int? preemptible_tries
  }

  Int disk_size = ceil(size(sites_only_vcf, "GB") * 2.5) + 20

  meta {
    description: "Creates a tsv file for imort into BigQuery"
  }
  parameter_meta {
    sites_only_vcf: {
      localization_optional: false
    }
  }
  command <<<
      set -e

      export GATK_LOCAL_JAR=~{default="/root/gatk.jar" gatk_override}

      gatk --java-options "-Xmx2500m" CreateSitesOnlyTsv \
        -V ~{sites_only_vcf} 
        
  >>>
  runtime {
      docker: "us.gcr.io/broad-gatk/gatk:4.1.7.0"
      memory: "20 GB"
      disks: "local-disk " + disk_size + " HDD"
      preemptible: select_first([preemptible_tries, 5])
      cpu: 2
  }
  output {
      File sitesonly_tsv = "imputedArrayData.tsv"
  }
}
