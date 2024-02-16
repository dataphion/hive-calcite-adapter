package com.dataphion.calcite.adapter.hive;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HiveTable extends AbstractTable {

	private final String tableName;
	private final String hiveConnectionUrl;
    private final String hiveUser;
    private final String hivePassword;
    private final String namespace;

    public HiveTable(String tableName, String hiveConnectionUrl, String hiveUser, String hivePassword, String namespace) {
        this.tableName = tableName;
        this.hiveConnectionUrl = hiveConnectionUrl;
        this.hiveUser = hiveUser;
        this.hivePassword = hivePassword;
        this.namespace = namespace;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    	System.out.println("getting metadata for " + tableName);
        
        try (Connection hiveConnection = establishConnection(hiveConnectionUrl, hiveUser, hivePassword); 
        	Statement statement = hiveConnection.createStatement()) {
            // Execute DESCRIBE statement to get table metadata
        	statement.executeQuery("use " + namespace);
        	System.out.println("DESCRIBE table " + tableName);
            ResultSet resultSet = statement.executeQuery("DESCRIBE table " + tableName);
            // Process the ResultSet and build the RelDataType
            List<RelDataType> types = new ArrayList<>();
            List<String> names = new ArrayList<>();
            while (resultSet.next()) {
                String columnName = resultSet.getString("col_name");
                //System.out.println("columnName " + columnName);
                String typeName = resultSet.getString("data_type");

                RelDataType dataType = typeFactory.createJavaType(getJavaType(typeName));
                types.add(dataType);
                names.add(columnName);
            }
            System.out.println(names);
            System.out.println(types);
            return typeFactory.createStructType(types, names);
//            RelDataType rowType = typeFactory.createStructType(types, names);
//            System.out.println("rowType: " + rowType);
//            return rowType;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to retrieve table metadata using DESCRIBE", e);
        }
    }


    private Class<?> getJavaType(String typeName) {
        switch (typeName.toUpperCase()) {
            case "INT":
                return Integer.class;
            case "STRING":
                return String.class;
            case "BOOLEAN":
                return Boolean.class;
            case "DOUBLE":
                return Double.class;
            case "DECIMAL":
                return BigDecimal.class;
            case "DATE":
                return LocalDate.class;
            case "TIME":
                return LocalTime.class;
            case "TIMESTAMP":
                return Instant.class;
            case "ARRAY":
                return Object[].class;  
            case "LIST":
                return List.class;  
            default:
                return Object.class;
        }
    }

    public Enumerable<Object[]> scan(DataContext dataContext) {
    	System.out.println("iam here");
        // Implement scanning logic to retrieve data from the Hive table
    	try (Connection hiveConnection = establishConnection(hiveConnectionUrl, hiveUser, hivePassword); 
             Statement statement = hiveConnection.createStatement()) {
    		 System.out.println("iam here");
    		 statement.executeQuery("use " + namespace);
    		 
             ResultSet resultSet = statement.executeQuery("SELECT * FROM " + tableName); {
            return new AbstractEnumerable<Object[]>() {
                @Override
                public Enumerator<Object[]> enumerator() {
                    return Linq4j.iterableEnumerator(() -> new Iterator<Object[]>() {
                        @Override
                        public boolean hasNext() {
                            try {
                                return resultSet.next();
                            } catch (SQLException e) {
                                throw new RuntimeException("Error while reading from ResultSet", e);
                            }
                        }

                        @Override
                        public Object[] next() {
                            try {
                                ResultSetMetaData metaData = resultSet.getMetaData();
                                int columnCount = metaData.getColumnCount();
                                Object[] row = new Object[columnCount];
                                for (int i = 0; i < columnCount; i++) {
                                    row[i] = resultSet.getObject(i + 1);
                                }
                                return row;
                            } catch (SQLException e) {
                                throw new RuntimeException("Error while reading from ResultSet", e);
                            }
                        }
                    });
                }
            };
        } }catch (SQLException e) {
            throw new RuntimeException("Failed to execute query", e);
        }
    }

    public void executeUpdate(String query) {
        // Execute a non-query SQL statement (e.g., INSERT, UPDATE, DELETE)
    	try (Connection hiveConnection = establishConnection(hiveConnectionUrl, hiveUser, hivePassword); 
            Statement statement = hiveConnection.createStatement()) {
            statement.executeUpdate(query);
        } catch (SQLException e) {
            throw new RuntimeException("Error executing update query", e);
        }
    }

    public ResultSet executeQuery(String query) {
        // Execute a SQL query and return the ResultSet
    	try (Connection hiveConnection = establishConnection(hiveConnectionUrl, hiveUser, hivePassword); 
            Statement statement = hiveConnection.createStatement()) {
            return statement.executeQuery(query);
        } catch (SQLException e) {
            throw new RuntimeException("Error executing query", e);
        }
    }
    
    private Connection establishConnection(String hiveConnectionUrl, String hiveUser, String hivePassword) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            return DriverManager.getConnection(hiveConnectionUrl, hiveUser, hivePassword);
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Error establishing Hive connection", e);
        }
    }

}
