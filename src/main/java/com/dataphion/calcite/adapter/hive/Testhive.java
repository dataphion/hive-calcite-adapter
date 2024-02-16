package com.dataphion.calcite.adapter.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.jdbc.CalciteConnection;

/**
 * Hello world!
 *
 */
public class Testhive 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        List<Map<String, Object>> results = new ArrayList<>();
    	
        Properties info = new Properties();
        info.put("model", "D:/hive-calcite-adapter/src/main/java/com/dataphion/calcite/adapter/hive/model.json");
        info.list(System.out);
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
            CalciteConnection calciteConn = connection.unwrap(CalciteConnection.class);
            Statement statement = calciteConn.createStatement();
            ResultSet resultSet = statement.executeQuery(            		
            		"select * from \"sqlreport\".\"students\""
            );

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                Map<String, Object> resultRow = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = resultSet.getObject(i);
                    resultRow.put(columnName, columnValue);
                    System.out.println("columnName -> "+columnName);
                    System.out.println(columnValue);
                }
                results.add(resultRow);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
