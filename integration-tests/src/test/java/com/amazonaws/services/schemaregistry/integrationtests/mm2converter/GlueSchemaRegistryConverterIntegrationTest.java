package com.amazonaws.services.schemaregistry.integrationtests.mm2converter;

import com.amazonaws.services.crossregion.schemaregistry.kafkaconnect.CrossRegionReplicationConverter;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.amazonaws.services.schemaregistry.utils.RecordGenerator;
import com.google.protobuf.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class GlueSchemaRegistryConverterIntegrationTest {

    private static CrossRegionReplicationConverter converter;
    private static List<String> schemasToCleanUp = new ArrayList<>();
    private static final AwsCredentialsProvider defaultCredProvider = DefaultCredentialsProvider
            .builder()
            .build();

    @BeforeEach
    public void setUp() {
        converter = new CrossRegionReplicationConverter();
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
     * Test when Avro schemas are replicated through MM2 successfully.
     */
    @ParameterizedTest
    @MethodSource("schemaRecordPairs")
    public void SchemaReplication_AvroData_Succeeds(Schema schema, GenericRecord record) {
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(getKafkaProducerProperties());
        ProducerRecord<String, GenericRecord> newRecord = new ProducerRecord<>(schema.getName(), record);
        producer.send(newRecord);
        schemasToCleanUp.add(schema.getName());

        Properties consumerProperties = getKafkaConsumerProperties();
        consumerProperties.replace(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList("A." + schema.getName()));
        Schema returnedSchema = null;
        ConsumerRecords<String, GenericRecord> returnRecords = consumer.poll(1000);
        for (ConsumerRecord<String, GenericRecord> returnRecord : returnRecords) {
            GenericRecord value = returnRecord.value();
            returnedSchema = value.getSchema();
        }
        assertEquals(schema, returnedSchema);
    }

    private static Stream<Arguments> schemaRecordPairs() {
        List<Schema> schemas = AvroSchemaGenerator.createTestSchema();
        List<GenericRecord> records = AvroSchemaGenerator.createTestRecord();

        return IntStream.range(0, schemas.size())
                .mapToObj(i -> Arguments.of(schemas.get(i), records.get(i)));
    }

    //TODO: Find a way to use consistent topic name
    /**
     * Test when JSON schemas are replicated through MM2 successfully.
     */
    @ParameterizedTest
    @MethodSource("wrapperJsonRecords")
    public void SchemaReplication_JsonData_Succeeds(JsonDataWithSchema schema) {
        String topicName = UUID.randomUUID().toString();
        KafkaProducer<String, JsonDataWithSchema> producer = new KafkaProducer<>(getKafkaProducerProperties_json());
        ProducerRecord<String, JsonDataWithSchema> newRecord = new ProducerRecord<>(topicName, schema);
        producer.send(newRecord);
        schemasToCleanUp.add(topicName);

        Properties consumerProperties = getKafkaConsumerProperties_json();
        consumerProperties.replace(ConsumerConfig.GROUP_ID_CONFIG, topicName);
        KafkaConsumer<String, JsonDataWithSchema> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList("A." + topicName));
        JsonDataWithSchema  returnedSchema = null;
        ConsumerRecords<String, JsonDataWithSchema> returnRecords = consumer.poll(1000);
        for (ConsumerRecord<String, JsonDataWithSchema> returnRecord : returnRecords) {
            returnedSchema = returnRecord.value();
        }
        assertEquals(schema, returnedSchema);
    }

    private static Stream<JsonDataWithSchema> wrapperJsonRecords() {
        return Arrays.stream(RecordGenerator.TestJsonRecord.values())
                .filter(RecordGenerator.TestJsonRecord::isValid)
                .map(RecordGenerator::createGenericJsonRecord);
    }

    //TODO: Find a way to use consistent topic name
    /**
     * Test when Protobuf schemas are replicated through MM2 successfully.
     */
    @ParameterizedTest
    @MethodSource("pojoMessages")
    public void SchemaReplication_ProtobufData_Succeeds(Message schema) {
        String topicName = UUID.randomUUID().toString();
        KafkaProducer<String, Message> producer = new KafkaProducer<>(getKafkaProducerProperties_protobuf());
        ProducerRecord<String, Message> newRecord = new ProducerRecord<>(topicName, schema);
        producer.send(newRecord);
        schemasToCleanUp.add(topicName);

        Properties consumerProperties = getKafkaConsumerProperties_protobuf();
        consumerProperties.replace(ConsumerConfig.GROUP_ID_CONFIG, topicName);
        KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList("A." + topicName));
        Message  returnedSchema = null;
        ConsumerRecords<String, Message> returnRecords = consumer.poll(1000);
        for (ConsumerRecord<String, Message> returnRecord : returnRecords) {
            returnedSchema = returnRecord.value();
        }
        assertEquals(schema, returnedSchema);
    }

    static List<Message> pojoMessages() {
        return ProtobufGenerator.getAllPOJOMessages();
    }

    private static Properties getKafkaProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true");
        props.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, "NONE");
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
        return props;
    }

    private static Properties getKafkaConsumerProperties(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "None");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaDeserializer.class.getName());
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true");
        props.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, "NONE");
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static Properties getKafkaProducerProperties_json() {
        Properties props = getKafkaProducerProperties();
        props.replace(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.JSON.name());
        return props;
    }

    private static Properties getKafkaConsumerProperties_json(){
        Properties props = getKafkaConsumerProperties();
        props.replace(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.JSON.name());
        return props;
    }

    private static Properties getKafkaProducerProperties_protobuf() {
        Properties props = getKafkaProducerProperties();
        props.replace(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.PROTOBUF.name());
        return props;
    }

    private static Properties getKafkaConsumerProperties_protobuf(){
        Properties props = getKafkaConsumerProperties();
        props.replace(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.PROTOBUF.name());
        return props;
    }

    /**
     * To create a map of configurations for converter.
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
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
        return props;
    }
}
