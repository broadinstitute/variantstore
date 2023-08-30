package org.broadinstitute.gvs.azure.cosmos;


import com.beust.jcommander.ParameterException;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class IngestArgumentsTest {
    @Test(expectedExceptions = {ParameterException.class},
            expectedExceptionsMessageRegExp = "The following options are required: \\[--database], \\[--avro-dir]")
    public void containerOnly() {
        IngestArguments.parseArgs(new String[] {"--container", "mycontainer"});
    }

    @Test(expectedExceptions = {ParameterException.class},
            expectedExceptionsMessageRegExp = "The following options are required: \\[--avro-dir], \\[--container]")
    public void databaseOnly() {
        IngestArguments.parseArgs(new String[] {"--database", "mydatabase"});
    }

    @Test(expectedExceptions = {ParameterException.class},
            expectedExceptionsMessageRegExp = "The following options are required: \\[--database], \\[--container]")
    public void avroDirOnly() {
        IngestArguments.parseArgs(new String[] {"--avro-dir", "myavros"});
    }


    public void validInvocationWithDefaults() {
        IngestArguments args = IngestArguments.parseArgs(
                new String[] {"--container", "mycontainer", "--database", "mydatabase", "--avro-dir", "myavros"});
        Assert.assertEquals(args.getContainer(), "mycontainer");
        Assert.assertEquals(args.getDatabase(), "mydatabase");
        Assert.assertEquals(args.getNumRecords(), Long.MAX_VALUE);
        Assert.assertEquals(args.getNumProgress(), 10000L);
    }

    public void validInvocationWithOverrides() {
        IngestArguments args = IngestArguments.parseArgs(
                new String[] {"--container", "mycontainer", "--database", "mydatabase", "--avro-dir", "myavros", "--num-records", "99999", "--num-progress", "99"});
        Assert.assertEquals(args.getContainer(), "mycontainer");
        Assert.assertEquals(args.getDatabase(), "mydatabase");
        Assert.assertEquals(args.getNumRecords(), 99999L);
        Assert.assertEquals(args.getNumProgress(), 99L);
    }
}
