version 1.0

workflow RawArrayCohortExtract {
   input {
        Int number_of_partitions = 2
        Int probes_per_partition = 1000000
        
        File reference
        File reference_index
        File reference_dict
    
        String fq_dataset
        Int max_tables
        String fq_destination_dataset
        String query_project
        String fq_cohort_mapping_table
        Int ttl
        
        String output_file_base_name="export"
        
        String full_sample_info_table
    }
    
    call CreateExtractTable {
        input:
            fq_dataset                = fq_dataset,
            max_tables                = max_tables,
            fq_destination_dataset    = fq_destination_dataset,
            query_project             = query_project,
            fq_cohort_mapping_table   = fq_cohort_mapping_table,
            ttl                       = ttl,
            number_of_partitions      = number_of_partitions,
            probes_per_partition      = probes_per_partition
    }
  
    
    scatter(i in range(number_of_partitions)) {
        call ExtractTask {
            input:
                reference             = reference,
                reference_index       = reference_index,
                reference_dict        = reference_dict,
                probe_info_clause     = "--probe-info-table spec-ops-aou.aou_pmi_synthetic_100k.probe_info",
                min_probe_id          = 1 + i * probes_per_partition,
                max_probe_id          = (i+1) * probes_per_partition,
                sample_info_table     = full_sample_info_table,
                use_compressed_data   = "false",
                cohort_extract_table  = CreateExtractTable.cohort_extract_table,
                project_id            = query_project,
                output_file           = "${output_file_base_name}_${i}.vcf.gz"
        }
    }
}

################################################################################
task CreateExtractTable {
    # indicates that this task should NOT be call cached
    meta {
       volatile: true
    }

    # ------------------------------------------------
    # Input args:
    input {
        String fq_dataset
        Int max_tables
        String fq_destination_dataset
        String query_project
        String fq_cohort_mapping_table
        Int ttl
        Int number_of_partitions
        Int probes_per_partition
    }

    # ------------------------------------------------
    # Run our command:
    command <<<
        set -e

        uuid=$(cat /proc/sys/kernel/random/uuid | sed s/-/_/g)
        export_table="~{fq_destination_dataset}.${uuid}"
        echo "Exporting to ${export_table}"
        
        python /app/raw_array_cohort_extract.py \
          --dataset ~{fq_dataset} \
          --max_tables ~{max_tables} \
          --fq_destination_table ${export_table} \
          --query_project ~{query_project} \
          --fq_cohort_sample_mapping_table ~{fq_cohort_mapping_table} \
          --ttl ~{ttl} \
          --number_of_partitions ~{number_of_partitions} \
          --probes_per_partition ~{probes_per_partition}
          
        echo ${export_table} > cohort_extract_table.txt

    >>>

    # ------------------------------------------------
    # Runtime settings:
    runtime {
        docker: "kcibul/variantstore-export:latest"
        memory: "3 GB"
        disks: "local-disk 10 HDD"
        bootDiskSizeGb: 15
        preemptible: 0
        cpu: 1
    }

    # Outputs:
    output {
        String cohort_extract_table = read_string("cohort_extract_table.txt")
    }    
}

task ExtractTask {
    # indicates that this task should NOT be call cached
    meta {
       volatile: true
    }

    input {
        # ------------------------------------------------
        # Input args:
        File reference
        File reference_index
        File reference_dict
    
        String probe_info_clause
        Int min_probe_id
        Int max_probe_id
        String sample_info_table
        String use_compressed_data
        String cohort_extract_table
        String project_id
        String output_file
        
        # Runtime Options:
        File? gatk_override
    }


    # ------------------------------------------------
    # Run our command:
    command <<<
        set -e
        export GATK_LOCAL_JAR=~{default="/root/gatk.jar" gatk_override}

        df -h

        gatk --java-options "-Xmx4g" \
            ArrayExtractCohort \
                -R "~{reference}" \
                -O "~{output_file}" \
                ~{probe_info_clause} \
                --project-id "~{project_id}" \
                --sample-info-table "~{sample_info_table}" \
                --use-compressed-data "false" \
                --cohort-extract-table "~{cohort_extract_table}" \
                --local-sort-max-records-in-ram "10000000" \
                --min-probe-id ~{min_probe_id} --max-probe-id ~{max_probe_id}

    >>>

    # ------------------------------------------------
    # Runtime settings:
    runtime {
        docker: "us.gcr.io/broad-dsde-methods/broad-gatk-snapshots:varstore_0701e99b04594651d3c20375bed230b38420d58f_array_probe_id_ranges"
        memory: "7 GB"
        disks: "local-disk 10 HDD"
        bootDiskSizeGb: 15
        preemptible: 0
        cpu: 2
    }

    # ------------------------------------------------
    # Outputs:
    output {
        File evoked_variants = output_file
    }
 }


