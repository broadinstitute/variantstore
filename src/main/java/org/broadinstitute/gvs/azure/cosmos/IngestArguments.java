package org.broadinstitute.gvs.azure.cosmos;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class IngestArguments {
    public String getDatabase() {
        return database;
    }

    public String getContainer() {
        return container;
    }

    public Long getNumRecords() {
        return numRecords;
    }

    public Long getNumProgress() {
        return numProgress;
    }

    public String getAvroDir() {
        return avroDir;
    }

    @Parameter(names = {"--database"}, description = "Cosmos database", required = true)
    private String database;

    @Parameter(names = {"--container"}, description = "Cosmos container", required = true)
    private String container;

    @Parameter(names = {"--avro-dir"}, description = "Directory containing Avro files", required = true)
    private String avroDir;

    @Parameter(names = {"--num-records"}, description = "Max number of records to load")
    private Long numRecords = Long.MAX_VALUE;

    @Parameter(names = {"--num-progress"}, description = "Max number of records to load between progress messages")
    private Long numProgress = 10000L;

    private IngestArguments() {
    }

    public static IngestArguments parseArgs(String [] argv) {
        IngestArguments args = new IngestArguments();
        JCommander.newBuilder().
                addObject(args).
                build().
                parse(argv);
        return args;
    }
}
