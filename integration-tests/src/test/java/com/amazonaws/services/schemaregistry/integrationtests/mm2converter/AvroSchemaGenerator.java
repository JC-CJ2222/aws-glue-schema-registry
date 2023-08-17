package com.amazonaws.services.schemaregistry.integrationtests.mm2converter;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroSchemaGenerator {

    static Schema.Parser parser = new Schema.Parser();
    static Schema schemaUser;
    static Schema schemaPayment;
    static Schema schemaPerson;

    static {
        try {
            schemaUser = parser.parse(new File("src/test/resources/avro/user.avsc"));
            schemaPayment = parser.parse(new File("src/test/resources/avro/Payment.avsc"));
            schemaPerson = parser.parse(new File("src/test/resources/avro/Person.avsc"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Schema schema1 = SchemaBuilder.record("schema1")
            .fields()
            .name("name").type().stringType().noDefault()
            .name("favorite_number").type().intType().noDefault()
            .endRecord();

    static Schema schema2 = SchemaBuilder.record("schema2")
            .fields()
            .name("name").type().stringType().noDefault()
            .name("favorite_number").type().intType().noDefault()
            .name("favorite_fruit").type().stringType().noDefault()
            .endRecord();

    static Schema schema3 = SchemaBuilder.record("schema3")
            .fields()
            .name("id").type().stringType().noDefault()
            .name("total_amount").type().doubleType().noDefault()
            .endRecord();

    static Schema schema4 = SchemaBuilder.record("schema4")
            .fields()
            .name("id").type().stringType().noDefault()
            .name("amount").type().doubleType().noDefault()
            .name("number").type().intType().noDefault()
            .endRecord();

    public static List<Schema> createTestSchema() {
        List<Schema> schemaList = new ArrayList<>();
        schemaList.add(schemaUser);
        schemaList.add(schemaPayment);
        schemaList.add(schemaPerson);
        schemaList.add(schema1);
        schemaList.add(schema2);
        schemaList.add(schema3);
        schemaList.add(schema4);
        return schemaList;
    }

    public static List<GenericRecord> createTestRecord() {
        List<GenericRecord> recordList = new ArrayList<>();

        GenericRecord sansa = new GenericData.Record(schemaUser);
        sansa.put("name", "Sansa");
        sansa.put("favorite_number", 99);
        sansa.put("favorite_color", "white");

        GenericRecord grocery = new GenericData.Record(schemaPayment);
        grocery.put("id", "grocery_1");
        grocery.put("amount", 25.5);

        GenericRecord harry = new GenericData.Record(schemaPerson);
        harry.put("firstName", "Harry");
        harry.put("lastName", "Potter");
        harry.put("age", 18);
        harry.put("height", 170);
        harry.put("employed", true);

        GenericRecord record1 = new GenericData.Record(schema1);
        record1.put("name", "Harry");
        record1.put("favorite_number", 5);

        GenericRecord record2 = new GenericData.Record(schema2);
        record2.put("name", "Jay");
        record2.put("favorite_number", 10);
        record2.put("favorite_fruit", "peach");

        GenericRecord record3 = new GenericData.Record(schema3);
        record3.put("id", "grocery");
        record3.put("total_amount", 25.5);

        GenericRecord record4 = new GenericData.Record(schema4);
        record4.put("id", "grocery");
        record4.put("amount", 25.5);
        record4.put("number", 88);

        recordList.add(sansa);
        recordList.add(grocery);
        recordList.add(harry);
        recordList.add(record1);
        recordList.add(record2);
        recordList.add(record3);
        recordList.add(record4);

        return recordList;
    }
}