package com.dataphion.calcite.adapter.hive;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HiveSchema extends AbstractSchema {

    private final String hiveConnectionUrl;
    private final String hiveUser;
    private final String hivePassword;
    private final String catalog;
    private final String namespace;
	private Connection connection;

    public HiveSchema(String hiveConnectionUrl, String hiveUser, String hivePassword, String catalog, String namespace) {
        this.hiveConnectionUrl = hiveConnectionUrl;
        this.hiveUser = hiveUser;
        this.hivePassword = hivePassword;
        this.catalog = catalog;
        this.namespace = namespace;
        
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("Hive JDBC driver not found");
        }
        
    }

    // Override method to return sub-schema
    @Override
    protected Map<String, org.apache.calcite.schema.Schema> getSubSchemaMap() {
    	System.out.println("Inside get Subschema Map.. => "+catalog);
    	System.out.println("Inside get Subschema Map namespace.. => "+namespace);
    	Map<String, org.apache.calcite.schema.Schema> schemaMap = new HashMap<>();
    	if(namespace == null || namespace == "") {
    		// Add sub-schema to the map
            schemaMap.put("finance", new HiveSchema(hiveConnectionUrl, hiveUser, hivePassword, catalog, "finance"));
            schemaMap.put("docs", new HiveSchema(hiveConnectionUrl, hiveUser, hivePassword, catalog, "docs"));
            schemaMap.put("test", new HiveSchema(hiveConnectionUrl, hiveUser, hivePassword, catalog, "test"));
            schemaMap.put("data", new HiveSchema(hiveConnectionUrl, hiveUser, hivePassword, catalog, "data"));
            schemaMap.put("school", new HiveSchema(hiveConnectionUrl, hiveUser, hivePassword, catalog, "school"));
            schemaMap.put("institute", new HiveSchema(hiveConnectionUrl, hiveUser, hivePassword, catalog, "institute"));
    	}
        return schemaMap;
    }


    @SuppressWarnings("finally")
	@Override
    protected Map<String, Table> getTableMap() {
        System.out.println("DBG:: Inside getTableMap.. => "+catalog);
        Map<String, Table> tableMap = new HashMap<>();
        SessionData sessionData = SessionData.getInstance();
	    Properties properties = new Properties();
        if(sessionData.getTables(catalog, namespace) != null) {
            System.out.println("DBG:: Serving from cache..");
        	System.out.println("tableMap from sessionData: " + sessionData.getTables(catalog, namespace));
        	return sessionData.getTables(catalog, namespace);
        }
	    properties.setProperty("user", hiveUser);
	    properties.setProperty("password", hivePassword);
        // Initialize Hive JDBC connection
        try (Connection connection = DriverManager.getConnection(hiveConnectionUrl, properties)) {
            // Fetch table names from Hive
        	System.out.println("connected!");
        	Statement Statement = connection.createStatement();
        	Statement.executeQuery("use " + namespace);
       	    ResultSet tables = Statement.executeQuery("show tables");
       	    while (tables.next()) {
	            String tableName = tables.getString(2);
	            System.out.println("tableName: " + tableName);
	            System.out.println("namespace: " + tables.getString(1));
	            // Create a HiveTable for each table and add it to the map
	            tableMap.put(tableName, new HiveTable(tableName, hiveConnectionUrl, hiveUser, hivePassword, namespace));
	            System.out.println("tableMap" + tableMap);
	        }
            sessionData.setTableMaps(catalog, namespace, tableMap);
        } catch (SQLException e) {
            // Handle exception appropriately
            e.printStackTrace();
        } finally {
            // If needed, close the connection in the finally block
            try {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        return tableMap;
    }
}
}
    