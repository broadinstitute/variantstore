package org.broadinstitute.gvs.azure.cosmos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class AvroReaderTest {

    @Test
    public void testFindAvroFiles() {
        List<Path> avroFiles = AvroReader.findAvroPaths("src/test/resources");
        Assert.assertEquals(avroFiles.size(), 1);

    }

    @Test
    public void testObjectNodeFromString() {
        ObjectMapper objectMapper = new ObjectMapper();
        String stringJson = """
                {
                    "sample_id": 7,
                    "location": 1000000010327,
                    "ref": "T",
                    "alt": "C",
                    "AS_RAW_MQ": "0|4842",
                    "AS_RAW_MQRankSum": null,
                    "QUALapprox": "72",
                    "AS_QUALapprox": "72",
                    "AS_RAW_ReadPosRankSum": null,
                    "AS_SB_TABLE": "0,0|0,3",
                    "AS_VarDP": "0|3",
                    "call_GT": "1/1",
                    "call_AD": "0,3",
                    "call_GQ": 7,
                    "call_PGT": "0|1",
                    "call_PID": "10327_T_C",
                    "call_PL": "72,7,0,72,7,72"
                }
                """.trim();
        AtomicLong id = new AtomicLong(0L);
        ObjectNode objectNode = AvroReader.objectNodeFromString(objectMapper, stringJson, id);
        Assert.assertEquals(id.get(), 1L);

        Assert.assertEquals(objectNode.get("id").asText(), String.valueOf(1L));
        Assert.assertEquals(objectNode.get("sample_id").asLong(), 7L);
    }
}
