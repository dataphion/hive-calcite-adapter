package com.dataphion.calcite.adapter.hive;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;

import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class HiveTable extends AbstractTable implements FilterableTable {

	private final String tableName;
	private final String hiveConnectionUrl;
    private final String hiveUser;
    private final String hivePassword;
    private final String namespace;
    
    private @Nullable RelDataType rowType;
    private @Nullable List<RelDataType> fieldTypes;

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

	@Override
	public Enumerable<@Nullable Object[]> scan(DataContext root, List<RexNode> filters) {
		// TODO Auto-generated method stub
		System.out.println("Insid Enumerator\n\n-------------------\n");
		JavaTypeFactory typeFactory = root.getTypeFactory();
	    final List<RelDataType> fieldTypes = getFieldTypes(typeFactory);
	    final @Nullable String[] filterValues = new String[fieldTypes.size()];
	    filters.removeIf(filter -> addFilter(filter, filterValues));
	    final List<Integer> fields = ImmutableIntList.identity(fieldTypes.size());
	    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
	    return new AbstractEnumerable<@Nullable Object[]>() {
	      @Override public Enumerator<@Nullable Object[]> enumerator() {
	        return new CsvEnumerator<>(source, cancelFlag, false, filterValues,
	            CsvEnumerator.arrayConverter(fieldTypes, fields, false));
	      }
	    };
	}
	
	private static boolean addFilter(RexNode filter, @Nullable Object[] filterValues) {
	    if (filter.isA(SqlKind.AND)) {
	        // We cannot refine(remove) the operands of AND,
	        // it will cause o.a.c.i.TableScanNode.createFilterable filters check failed.
	      ((RexCall) filter).getOperands().forEach(subFilter -> addFilter(subFilter, filterValues));
	    } else if (filter.isA(SqlKind.EQUALS)) {
	      final RexCall call = (RexCall) filter;
	      RexNode left = call.getOperands().get(0);
	      if (left.isA(SqlKind.CAST)) {
	        left = ((RexCall) left).operands.get(0);
	      }
	      final RexNode right = call.getOperands().get(1);
	      if (left instanceof RexInputRef
	          && right instanceof RexLiteral) {
	        final int index = ((RexInputRef) left).getIndex();
	        if (filterValues[index] == null) {
	          filterValues[index] = ((RexLiteral) right).getValue2().toString();
	          return true;
	        }
	      }
	    }
	    return false;
	  }

}
