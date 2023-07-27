package com.amazonaws.services.schemaregistry.integrationtests.mm2converter;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
public class TestSchemaFactory {

    public static Schema createTestSchema() {
        Schema record = SchemaBuilder.record("record")
                .fields()
                .name("field1").type().nullable().stringType().noDefault()
                .name("field2").type().nullable().intType().noDefault()
                .endRecord();
        // Add more fields as needed
        return record;
    }

}
