package com.amazonaws.services.schemaregistry.integrationtests.mm2converter;

import com.amazonaws.services.crossregion.schemaregistry.kafkaconnect.CrossRegionReplicationMM2Converter;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.DeleteSchemaRequest;
import software.amazon.awssdk.services.glue.model.SchemaId;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class GlueSchemaRegistryConverterIntegrationTest {

    private static CrossRegionReplicationMM2Converter converter;

    private static List<String> schemasToCleanUp = new ArrayList<>();

    private static final AwsCredentialsProvider defaultCredProvider = DefaultCredentialsProvider
            .builder()
            .build();

    @BeforeEach
    public void setUp() {
        converter = new CrossRegionReplicationMM2Converter();
        converter.configure(getConverterProperties(), false);
    }

    @AfterAll
    public static void tearDown() throws URISyntaxException {
        Map<String, Object> properties = getConverterProperties();
        GlueClient glueClientAtSourceRegion = GlueClient.builder()
                .credentialsProvider(defaultCredProvider)
                .region(Region.of((String) properties.get(AWSSchemaRegistryConstants.AWS_SOURCE_REGION)))
                .endpointOverride(new URI((String) properties.get(AWSSchemaRegistryConstants.AWS_SOURCE_ENDPOINT)))
                .httpClient(UrlConnectionHttpClient.builder()
                        .build())
                .build();

        GlueClient glueClientAtTargetRegion = GlueClient.builder()
                .credentialsProvider(defaultCredProvider)
                .region(Region.of((String) properties.get(AWSSchemaRegistryConstants.AWS_TARGET_REGION)))
                .endpointOverride(new URI((String) properties.get(AWSSchemaRegistryConstants.AWS_TARGET_ENDPOINT)))
                .httpClient(UrlConnectionHttpClient.builder()
                        .build())
                .build();

        for (String schemaName : schemasToCleanUp) {
            DeleteSchemaRequest deleteSchemaRequest = DeleteSchemaRequest.builder()
                    .schemaId(SchemaId.builder()
                            .registryName("default-registry")
                            .schemaName(schemaName)
                            .build())
                    .build();

            glueClientAtSourceRegion.deleteSchema(deleteSchemaRequest);
            glueClientAtTargetRegion.deleteSchema(deleteSchemaRequest);
        }
    }

    @Test
    public void SchemaReplication_AvroData_Succeeds() {

        AWSKafkaAvroSerializer avroSerializer = new AWSKafkaAvroSerializer(defaultCredProvider, getSourceGSRProperties());

        List<Schema> schemaList = AvroSchemaGenerator.createTestSchema();
        List<GenericRecord> recordList = AvroSchemaGenerator.createTestRecord();
        int index = 0;
        for (org.apache.avro.Schema schema : schemaList){
            GenericRecord record = recordList.get(index);
            byte[] serialize = avroSerializer.serialize(schema.getName(), record);

            com.amazonaws.services.schemaregistry.common.Schema returnSchema = converter.getSchema(serialize);
            schemasToCleanUp.add(returnSchema.getSchemaName());
            assertEquals(schema.toString(), returnSchema.getSchemaDefinition());

            UUID returnedUuid = converter.registerSchema(returnSchema);
            assertNotNull(returnedUuid);
            index ++;
        }
    }

    private static Map<String, Object> getConverterProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://glue.us-east-1.amazonaws.com");
        props.put(AWSSchemaRegistryConstants.AWS_SOURCE_REGION, "us-west-2");
        props.put(AWSSchemaRegistryConstants.AWS_SOURCE_ENDPOINT, "https://glue.us-west-2.amazonaws.com");
        props.put(AWSSchemaRegistryConstants.AWS_TARGET_REGION, "us-east-1");
        props.put(AWSSchemaRegistryConstants.AWS_TARGET_ENDPOINT, "https://glue.us-east-1.amazonaws.com");
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());

        return props;
    }

    private Map<String, Object> getSourceGSRProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://glue.us-west-2.amazonaws.com");
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());

        return props;
    }
}
