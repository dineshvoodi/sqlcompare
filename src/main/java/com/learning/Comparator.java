package com.learning;

import com.learning.model.TableSchema;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.ForeignKeyIndex;
import net.sf.jsqlparser.statement.create.table.Index;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Comparator {

    public static Map<String, TableSchema> parseSqlFile(Path path) throws Exception {
        Map<String, TableSchema> schema = new HashMap<>();

        String sql = Files.readString(path);
        sql = preprocessSql(sql);
        List<Statement> statements = CCJSqlParserUtil.parseStatements(sql);

        for (Statement stmt : statements) {
            if (stmt instanceof CreateTable createTable) {
                String tableName = createTable.getTable().getName().toUpperCase();
                TableSchema table = new TableSchema();

                // Read column Definition
                for (ColumnDefinition colDef : createTable.getColumnDefinitions()) {
                    table.columns.put(colDef.getColumnName().toUpperCase(), colDef.getColDataType().toString().toUpperCase());

                    // Read Column Specifications
                    if (colDef.getColumnSpecs() != null) {
                        String specs = String.join(" ", colDef.getColumnSpecs());
                        table.specs.put(colDef.getColumnName().toUpperCase(), specs.toUpperCase());
                    }
                }

                // Read Table Indexes
                if (createTable.getIndexes() != null) {
                    for (Index index : createTable.getIndexes()) {
                        String type = index.getType().toUpperCase();
                        List<String> cols = index.getColumnsNames().stream().map(String::toUpperCase).toList(); // Indexed Columns

                        switch (type) {
                            case "PRIMARY KEY" -> table.primaryKeys.addAll(cols);
                            case "UNIQUE", "UNIQUE KEY" -> table.uniqueConstraints.add(cols);
                            case "INDEX", "KEY" -> table.indexes.add(cols);
                            case "FOREIGN KEY" -> {
                                ForeignKeyIndex foreignKeyIndex = (ForeignKeyIndex) index;

                                if (foreignKeyIndex.getTable() != null) {

                                    List<String> refColumn = foreignKeyIndex.getReferencedColumnNames().stream().map(String::toUpperCase).toList();
                                    String refTable = foreignKeyIndex.getTable().getName().toUpperCase();

                                    Map<String, List<String>> ref = new HashMap<>();
                                    ref.put(refTable, refColumn);
                                    table.foreignKeys.put(cols, ref);
                                }
                            }
                        }
                    }
                }

                schema.put(tableName, table);
            }
        }

        return schema;
    }

    private static String preprocessSql(String sql) {
        Pattern pattern = Pattern.compile("(?i)UNIQUE\\s+INDEX");
        Matcher matcher = pattern.matcher(sql);
        return matcher.replaceAll("UNIQUE KEY");
    }

    public static void compareSchemas(Map<String, TableSchema> schema1,
                                      Map<String, TableSchema> schema2, String[] files) {
        Set<String> tables1 = schema1.keySet();
        Set<String> tables2 = schema2.keySet();

        Set<String> tableSet1 = diff(tables1, tables2);
        Set<String> tableSet2 = diff(tables2, tables1);

        if(!tableSet1.isEmpty())
            System.out.println("Tables only in " + files[0] + ": " + tableSet1);
        if(!tableSet2.isEmpty())
            System.out.println("Tables only in " + files[1] + ": " + tableSet2);

        for (String table : intersect(tables1, tables2)) {

            StringBuilder error = new StringBuilder();

            TableSchema cols1 = schema1.get(table);
            TableSchema cols2 = schema2.get(table);

            Set<String> colNames1 = cols1.columns.keySet();
            Set<String> colNames2 = cols2.columns.keySet();

            Set<String> columnSet1 = diff(colNames1, colNames2);
            Set<String> columnSet2 = diff(colNames2, colNames1);

            // Check Columns
            if(!columnSet1.isEmpty())
                error.append("  Columns only in ").append(files[0]).append(": ").append(columnSet1).append("\n");
            if(!columnSet2.isEmpty())
                error.append("  Columns only in ").append(files[1]).append(": ").append(columnSet2).append("\n");

            for (String col : intersect(colNames1, colNames2)) {
                String type1 = cols1.columns.get(col);
                String type2 = cols2.columns.get(col);
                if (!type1.equalsIgnoreCase(type2)) {
                    error.append(String.format("    Data type mismatch in column '%s': %s (vs) %s%n", col, type1, type2));
                }

                // Check Table Specs
                String check1 = cols1.specs.get(col);
                String check2 = cols1.specs.get(col);
                if (!Objects.equals(check1, check2)) {
                    error.append(String.format("    Specs mismatch in column '%s': %s (vs) %s%n", col, check1, check2));
                }
            }

            //Check Primary Keys
            if(!Objects.equals(new HashSet<>(cols1.primaryKeys), new HashSet<>(cols2.primaryKeys))) {
                error.append(String.format("  Primary Key mismatch: %s (vs) %s%n", cols1.primaryKeys, cols2.primaryKeys));
            }

            // Check Unique constraints
            Set<List<String>> uniqueSet1 = new HashSet<>(cols1.uniqueConstraints);
            Set<List<String>> uniqueSet2 = new HashSet<>(cols2.uniqueConstraints);
            for(List<String> list : uniqueSet1) {
                if(!uniqueSet2.contains(list)) {
                    error.append(String.format("  Unique constraints missing in %s: %s%n", files[1], list));
                }
            }

            for(List<String> list : uniqueSet2) {
                if(!uniqueSet1.contains(list)) {
                    error.append(String.format("  Unique constraints missing in %s: %s%n", files[0], list));
                }
            }

            // Check Indexes
            Set<List<String>> indexSet1 = new HashSet<>(cols1.indexes);
            Set<List<String>> indexSet2 = new HashSet<>(cols2.indexes);
            for(List<String> list : indexSet1) {
                if(!indexSet2.contains(list)) {
                    error.append(String.format("  Indexes missing in %s: %s%n", files[1], list));
                }
            }

            for(List<String> list : indexSet2) {
                if(!indexSet1.contains(list)) {
                    error.append(String.format("  Indexes missing in %s: %s%n", files[0], list));
                }
            }

            // Check Foreign Keys
            for(List<String> keys : cols1.foreignKeys.keySet()) {
                if(!cols2.foreignKeys.containsKey(keys)) {
                    error.append(String.format("  Foreign Key missing in %s: %s%n", files[1], keys));
                    continue;
                }

                Map<String, List<String>> innerMap1 = cols1.foreignKeys.get(keys);
                Map<String, List<String>> innerMap2 = cols2.foreignKeys.get(keys);

                if(!Objects.equals(innerMap1, innerMap2)) {
                    error.append(String.format("  Foreign Key Reference Table mismatch for key %s: %s (vs) %s%n", keys, innerMap1, innerMap2));
                }
            }

            for(List<String> keys : cols2.foreignKeys.keySet()) {
                if (!cols1.foreignKeys.containsKey(keys)) {
                    error.append(String.format("  Foreign Key missing in %s: %s%n", files[0], keys));
                }
            }

            if(!error.isEmpty()) {
                System.out.println("\nComparing Table: " + table);
                System.out.println(error.toString());
            }
        }
    }

    public static Set<String> diff(Set<String> a, Set<String> b) {
        Set<String> result = new HashSet<>(a);
        result.removeAll(b);
        return result;
    }

    public static Set<String> intersect(Set<String> a, Set<String> b) {
        Set<String> result = new HashSet<>(a);
        result.retainAll(b);
        return result;
    }

    public static void main(String[] args) throws Exception {
       /* if (args.length != 2) {
            System.err.println("Usage: java -jar comparator.jar file1.sql file2.sql");
            System.exit(1);
        }*/

        args = new String[2];
        args[0] = "C:\\Users\\dines\\Downloads\\Testing\\SQLTesting\\sql1.sql";
        args[1] = "C:\\Users\\dines\\Downloads\\Testing\\SQLTesting\\sql2.sql";

        Path path1 = Paths.get(args[0]);
        Path path2 = Paths.get(args[1]);
        String[] files = {String.valueOf(path1.getFileName()), String.valueOf(path2.getFileName())};

        if(!Files.exists(path1) || !Files.isRegularFile(path1)) {
            System.out.println("Invalid file: " + files[0]);
            return;
        }

        if(!Files.exists(path2) || !Files.isRegularFile(path2)) {
            System.out.println("Invalid file: " + files[1]);
            return;
        }

        Map<String, TableSchema> schema1 = parseSqlFile(path1);
        Map<String, TableSchema> schema2 = parseSqlFile(path2);
        compareSchemas(schema1, schema2, files);
    }

}
