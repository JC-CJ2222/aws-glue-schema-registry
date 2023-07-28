package com.amazonaws.services.schemaregistry.integrationtests.mm2converter;

import com.amazonaws.services.crossregion.schemaregistry.kafkaconnect.CrossRegionReplicationMM2Converter;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.integrationtests.kafka.ProducerProperties;
import com.amazonaws.services.schemaregistry.integrationtests.properties.GlueSchemaRegistryConnectionProperties;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

    private static String topicName = "test-topic";

//    private static final String REGION = GlueSchemaRegistryConnectionProperties.REGION;
//    private static final String SCHEMA_REGISTRY_ENDPOINT_OVERRIDE = GlueSchemaRegistryConnectionProperties.ENDPOINT;
    private static AwsCredentialsProvider defaultCredProvider = DefaultCredentialsProvider
            .builder()
            .build();
    private static List<org.apache.avro.Schema> schemasToCleanUp = new ArrayList<>();

    @Before
    public void setUp() {
        converter = new CrossRegionReplicationMM2Converter();
        converter.configure(getConverterProperties(), false);
    }

//    @AfterAll
//    public static void tearDown() throws URISyntaxException {
//        GlueClient glueClient = GlueClient.builder()
//                .credentialsProvider(defaultCredProvider)
//                .region(Region.of(REGION))
//                .endpointOverride(new URI(SCHEMA_REGISTRY_ENDPOINT_OVERRIDE))
//                .httpClient(UrlConnectionHttpClient.builder()
//                        .build())
//                .build();
//
//        for (org.apache.avro.Schema schema : schemasToCleanUp) {
//            String schemaName = schema.getName();
//            DeleteSchemaRequest deleteSchemaRequest = DeleteSchemaRequest.builder()
//                    .schemaId(SchemaId.builder()
//                            .registryName("default-registry")
//                            .schemaName(schemaName)
//                            .build())
//                    .build();
//
//            glueClient.deleteSchema(deleteSchemaRequest);
//        }
//    }


    @Test
    public void SchemaRegisteredToTargetGSR_Succeeds() {
        System.out.print("Converter: " + converter);
        AWSKafkaAvroSerializer avroSerializer = new AWSKafkaAvroSerializer(defaultCredProvider, getSourceGSRProperties());

        // Create a test schema for registration
        org.apache.avro.Schema testSchema = TestSchemaFactory.createTestSchema();
        GenericRecord record = new GenericData.Record(testSchema);
        record.put("field1", "hello word");
        record.put("field2", 5);
        byte[] serialize = avroSerializer.serialize(topicName, record);
        System.out.println(Arrays.toString(serialize));


        // Register the schema from the source region Glue Schema Registry
        // and get the registered schema ID
        com.amazonaws.services.schemaregistry.common.Schema returnSchema = converter.getSchema(serialize);

        // Verify that the schema is successfully registered in the source Glue Schema Registry
        assertNotNull(returnSchema);

        // Register the same schema to the target region Glue Schema Registry
        UUID returnedUuid = converter.registerSchema(returnSchema);


        // Verify that the schema is successfully registered in the target Glue Schema Registry
        assertNotNull(returnedUuid);
    }

    private Map<String, Object> getConverterProperties() {
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

    private Map<String, Object> getTargetGSRProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());

        return props;
    }
}
