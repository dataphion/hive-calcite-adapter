package com.dataphion.calcite.adapter.hive;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.schema.Table;

// Singleton class to store session data

public class SessionData {
    private static SessionData instance = null;
    private Map<String, Map<String, Table>> tableMaps = new HashMap<>();
    private SessionData() {
    }

    public static synchronized SessionData getInstance() {
        if (instance == null) {
            instance = new SessionData();
        }
        return instance;
    }

    public Map<String, Table> getTables(String catalog, String namespace){
        Map<String, Table> tableMap = new HashMap<>();
        tableMap = tableMaps.get(catalog + ":" + namespace);
        return tableMap;
    }

    public void setTableMaps(String catalog, String namespace, Map<String, Table> tableMap){
        tableMaps.put(catalog + ":" + namespace, tableMap);
    }
    
}