package com.learning;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class Comparator {

    public static Map<String, Map<String, String>> parseSqlFile(String path) throws Exception {
        Map<String, Map<String, String>> schema = new HashMap<>();

        String sql = Files.readString(Paths.get(path));
        List<Statement> statements = CCJSqlParserUtil.parseStatements(sql);

        for (Statement stmt : statements) {
            if (stmt instanceof CreateTable) {
                CreateTable createTable = (CreateTable) stmt;
                String tableName = createTable.getTable().getName();
                TableSchema table = new TableSchema();

                for (ColumnDefinition colDef : createTable.getColumnDefinitions()) {
                    table.columns.put(colDef.getColumnName(), colDef.getColDataType().toString());

                    if (colDef.getColumnSpecs() != null) {
                        List<String> specs = colDef.getColumnSpecs().stream()
                                .map(String::toUpperCase).collect(Collectors.toList());
                        for (int i = 0; i < specs.size(); i++) {
                            if (specs.get(i).equals("CHECK")) {
                                String check = String.join(" ", specs.subList(i, specs.size()));
                                table.checks.add(check);
                            }
                        }
                    }
                }

                if (createTable.getIndexes() != null) {
                    for (Index index : createTable.getIndexes()) {
                        String type = index.getType().toUpperCase();
                        List<String> cols = index.getColumnsNames();

                        switch (type) {
                            case "PRIMARY KEY" -> table.primaryKeys.addAll(cols);
                            case "UNIQUE" -> table.uniqueConstraints.add(cols);
                            case "INDEX" -> table.indexes.add(cols);
                            /*case "FOREIGN KEY" -> {
                                if (index.getOptions() != null && index.getOptions().contains("REFERENCES")) {
                                    String ref = String.join(" ", index.getOptions());
                                    for (String col : cols) {
                                        table.foreignKeys.put(col, ref);
                                    }
                                }
                            }*/
                        }
                    }
                }

                schema.put(tableName, table);
            }
        }

        return schema;
    }

    public static void compareSchemas(Map<String, Map<String, String>> schema1,
                                      Map<String, Map<String, String>> schema2) {
        Set<String> tables1 = schema1.keySet();
        Set<String> tables2 = schema2.keySet();

        System.out.println("Tables only in file1: " + diff(tables1, tables2));
        System.out.println("Tables only in file2: " + diff(tables2, tables1));

        for (String table : intersect(tables1, tables2)) {
            System.out.println("\nComparing table: " + table);

            Map<String, String> cols1 = schema1.get(table);
            Map<String, String> cols2 = schema2.get(table);

            Set<String> colNames1 = cols1.keySet();
            Set<String> colNames2 = cols2.keySet();

            System.out.println(" Columns only in file1: " + diff(colNames1, colNames2));
            System.out.println(" Columns only in file2: " + diff(colNames2, colNames1));

            for (String col : intersect(colNames1, colNames2)) {
                String type1 = cols1.get(col);
                String type2 = cols2.get(col);
                if (!type1.equalsIgnoreCase(type2)) {
                    System.out.printf("  Data type mismatch in column '%s': %s vs %s%n", col, type1, type2);
                }
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
            System.err.println("Usage: java -jar sqlschema-diff.jar file1.sql file2.sql");
            System.exit(1);
        }*/

        Map<String, Map<String, String>> schema1 = parseSqlFile("C:\\Users\\dines\\Downloads\\Testing\\SQLTesting\\sql1.sql");
        Map<String, Map<String, String>> schema2 = parseSqlFile("C:\\Users\\dines\\Downloads\\Testing\\SQLTesting\\sql2.sql");

        compareSchemas(schema1, schema2);
    }

}
