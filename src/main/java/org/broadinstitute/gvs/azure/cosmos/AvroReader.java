package org.broadinstitute.gvs.azure.cosmos;

import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class AvroReader {

    private static final Logger logger = LoggerFactory.getLogger(AvroReader.class);

    public static List<Path> findAvroPaths(String avroDir) {
        try {
            try (Stream<Path> files = Files.list(Path.of(avroDir))) {
                return files.filter(p -> p.getFileName().toString().endsWith(".avro")).toList();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static ObjectNode objectNodeFromString(ObjectMapper objectMapper, String jsonString, AtomicLong id) {
        try {
            JsonNode jsonNode = objectMapper.readTree(jsonString);
            ObjectNode objectNode = (ObjectNode) jsonNode;
            long longId = id.addAndGet(1L);
            String stringId = String.valueOf(longId);
            objectNode.set("id", objectMapper.convertValue(stringId, JsonNode.class));
            return objectNode;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error reading String as JSON Object", e);
        }
    }

    public static Flux<CosmosItemOperation> itemFluxFromAvroPath(ObjectMapper objectMapper, Path path, AtomicLong id,
                                                                 Long numRecordsProgress, Long numRecordsToLoad) {
        // Where the Cosmos JSON serialization magic happens:
        // https://github.com/Azure/azure-sdk-for-java/blob/80b12e48aeb6ad2f49e86643dfd7223bde7a9a0c/sdk/cosmos/azure-cosmos/src/main/java/com/azure/cosmos/implementation/JsonSerializable.java#L255
        //

        // Making a Jackson `ObjectNode` (subtype of `JsonNode`) from a `GenericRecord#toString` JSON String seems like
        // the least bad option for Cosmos serialization (`JsonSerializable` looks like a Jackson type but is actually
        // an internal Cosmos type). This approach still necessitates an unsavory amount of bit shuffling from Avro =>
        // Stringified JSON => Jackson object => Stringified JSON, but likely still better than brittle, clunky POJOs.
        // And despite all the shuffling this code generates Cosmos items far faster than we can actually push them to
        // Cosmos with 100K RU/s container bandwidth configuration.
        File file = new File(path.toString());
        GenericDatumReader<?> reader = new GenericDatumReader<>();
        try {
            DataFileReader<?> dataFileReader = new DataFileReader<>(file, reader);
            return Flux.fromIterable(dataFileReader).
                    take(numRecordsToLoad - id.get()).
                    map(record -> {
                        ObjectNode objectNode = objectNodeFromString(objectMapper, record.toString(), id);
                        Long idLong = id.get();
                        CosmosItemOperation itemOperation = CosmosBulkOperations.getCreateItemOperation(
                                objectNode, new PartitionKey(objectNode.get("sample_id").longValue()));
                        if (idLong % numRecordsProgress == 0L) logger.info(idLong + "...");
                        return itemOperation;
                    });

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
