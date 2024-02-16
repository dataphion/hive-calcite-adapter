package com.dataphion.calcite.adapter.hive;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class HiveSchemaFactory implements SchemaFactory {

    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        String hiveConnectionUrl = (String) operand.get("hiveConnectionUrl");
        String hiveUser = (String) operand.get("hiveUser");
        String hivePassword = (String) operand.get("hivePassword");
        String namespace = (String) operand.get("namespace");

        System.out.println(hiveConnectionUrl);

        // Create and return an instance of HiveSchema
        return new HiveSchema(hiveConnectionUrl, hiveUser, hivePassword, namespace);
    }
}
