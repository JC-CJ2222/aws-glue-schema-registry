package com.amazonaws.services.schemaregistry.integrationtests.mm2converter;

import com.amazonaws.services.crossregion.schemaregistry.kafkaconnect.CrossRegionReplicationMM2Converter;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.amazonaws.services.schemaregistry.utils.RecordGenerator;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
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
import java.util.stream.Collectors;

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

    /**
     * Test when Avro schemas are being obtained from source region and registered in the target region successfully.
     */
    @Test
    public void SchemaReplication_AvroData_Succeeds() {
        converter.configure(getConverterProperties(), false);
        AWSKafkaAvroSerializer avroSerializer = new AWSKafkaAvroSerializer(defaultCredProvider, getSourceGSRProperties());

        List<Schema> schemaList = AvroSchemaGenerator.createTestSchema();
        List<GenericRecord> recordList = AvroSchemaGenerator.createTestRecord();
        int index = 0;
        for (org.apache.avro.Schema schema : schemaList){
            GenericRecord record = recordList.get(index);
            schemasToCleanUp.add(schema.getName());

            byte[] serialize = avroSerializer.serialize(schema.getName(), record);
            com.amazonaws.services.schemaregistry.common.Schema returnSchema = converter.getSchema(serialize);
            assertEquals(schema.toString(), returnSchema.getSchemaDefinition());

            UUID returnedUuid = converter.registerSchema(returnSchema);
            assertNotNull(returnedUuid);
            index ++;
        }
    }

    /**
     * Test when JSON schemas are being obtained from source region and registered in the target region successfully.
     */
    @Test
    public void SchemaReplication_JsonData_Succeeds() {
        converter.configure(getConverterProperties_Json(), false);
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = new GlueSchemaRegistryKafkaSerializer(getSourceGSRProperties_Json());
        List<JsonDataWithSchema> wrapperJsonRecords = Arrays.stream(RecordGenerator.TestJsonRecord.values())
                .filter(RecordGenerator.TestJsonRecord::isValid)
                .map(RecordGenerator::createGenericJsonRecord)
                .collect(Collectors.toList());

        int index = 0;
        for (JsonDataWithSchema record : wrapperJsonRecords){
            String title = "jsonSchema" + index;
            schemasToCleanUp.add(title);

            byte[] serializedBytes = glueSchemaRegistryKafkaSerializer.serialize(title, record);
            com.amazonaws.services.schemaregistry.common.Schema returnSchema = converter.getSchema(serializedBytes);
            assertEquals(record.getSchema(), returnSchema.getSchemaDefinition());

            UUID returnedUuid = converter.registerSchema(returnSchema);
            assertNotNull(returnedUuid);
            index++;
        }
    }

    /**
     * Test when Protobuf schemas are being obtained from source region and registered in the target region successfully.
     */
    @Test
    public void SchemaReplication_ProtobufData_Succeeds() {
        converter.configure(getConverterProperties_Protobuf(), false);
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = new GlueSchemaRegistryKafkaSerializer(getSourceGSRProperties_Protobuf());
        List<Message> pojoMessages = ProtobufGenerator.getAllPOJOMessages();
        List<DynamicMessage> dynamicMessages = ProtobufGenerator.getAllDynamicMessages();

        int index = 0;
        for (Message message: pojoMessages){
            String title = "protobufSchema" + index;
            schemasToCleanUp.add(title);

            byte[] serializedBytes = glueSchemaRegistryKafkaSerializer.serialize(title, message);
            com.amazonaws.services.schemaregistry.common.Schema returnSchema = converter.getSchema(serializedBytes);
            assertNotNull(returnSchema);

            UUID returnedUuid = converter.registerSchema(returnSchema);
            assertNotNull(returnedUuid);
            index ++;
        }

        for (Message message: dynamicMessages){
            String title = "protobufSchema" + index;
            schemasToCleanUp.add(title);

            byte[] serializedBytes = glueSchemaRegistryKafkaSerializer.serialize(title, message);
            com.amazonaws.services.schemaregistry.common.Schema returnSchema = converter.getSchema(serializedBytes);
            assertNotNull(returnSchema);

            UUID returnedUuid = converter.registerSchema(returnSchema);
            assertNotNull(returnedUuid);
            index ++;
        }

    }

    /**
     * To create a map of configurations for converter in Avro format.
     *
     * @return a map of configurations
     */
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

    /**
     * To create a map of configurations for serializer in Avro format.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getSourceGSRProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://glue.us-west-2.amazonaws.com");
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());

        return props;
    }

    /**
     * To create a map of configurations for converter in JSON format.
     *
     * @return a map of configurations
     */
    private static Map<String, Object> getConverterProperties_Json() {
        Map<String, Object> props = getConverterProperties();
        props.replace(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.JSON.name());
        return props;
    }

    /**
     * To create a map of configurations for serializer in JSON format.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getSourceGSRProperties_Json() {
        Map<String, Object> props = getSourceGSRProperties();
        props.replace(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.JSON.name());
        return props;
    }

    /**
     * To create a map of configurations for converter in Protobuf format.
     *
     * @return a map of configurations
     */
    private static Map<String, Object> getConverterProperties_Protobuf() {
        Map<String, Object> props = getConverterProperties();
        props.replace(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.PROTOBUF.name());
        return props;
    }

    /**
     * To create a map of configurations for serializer in Protobuf format.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getSourceGSRProperties_Protobuf() {
        Map<String, Object> props = getSourceGSRProperties();
        props.replace(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.PROTOBUF.name());
        return props;
    }
}
