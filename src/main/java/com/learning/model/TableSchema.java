package com.learning.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TableSchema {

    public Map<String, String> columns = new LinkedHashMap<>();
    public List<String> primaryKeys = new ArrayList<>();
    public List<List<String>> uniqueConstraints = new ArrayList<>();
    public List<List<String>> indexes = new ArrayList<>();
    public Map<List<String>, Map<String, List<String>>> foreignKeys = new LinkedHashMap<>(); // column -> referenced table.column
    public Map<String, String> specs = new LinkedHashMap<>();

}
