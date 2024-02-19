package com.dataphion.calcite.adapter.hive;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.sql.DatabaseMetaData;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
        Map<String, Table> tableMap = new HashMap<>();
                
	    Properties properties = new Properties();
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
       	    
       	    
       	    // ResultSetMetaData metaData = resultSet.getMetaData();
//                int columnCount = metaData.getColumnCount();
//
//                while (resultSet.next()) {
//                    Map<String, Object> resultRow = new HashMap<>();
//                    for (int i = 1; i <= columnCount; i++) {
//                        String columnName = metaData.getColumnName(i);
//                        Object columnValue = resultSet.getObject(i);
//                        resultRow.put(columnName, columnValue);
//                        System.out.println("columnName -> "+columnName);
//                        System.out.println(columnValue);
//                    }
//            }
        	
//        	DatabaseMetaData metaData = connection.getMetaData();   
//            System.out.println("metaData:" + metaData);
//            System.out.println("Database Name: " + metaData.getDatabaseProductName());
//            try (ResultSet tables = metaData.getTables(null, null, null, new String[]{"TABLE"})) {
//            	System.out.println("tables:" + tables);
//                while (tables.next()) {
//                    String tableName = tables.getString("TABLE_NAME");
//                    System.out.println("tableName:" + tableName);
//                    System.out.println("1:");
//                    // Create a HiveTable for each table and add it to the map
//                    tableMap.put(tableName, new HiveTable(tableName, connection));
//                }
//                System.out.println("out");
//            }
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
    