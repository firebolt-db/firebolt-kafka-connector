package com.firebolt.kafka.connect;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.Getter;

/**
 * Represents the schema of a database table.
 * Contains column information including name, type, and constraints.
 */
@Data
@Getter
public class TableSchema {

    private final String tableName;
    private final List<Column> columns;

    public TableSchema(String tableName) {
        this.tableName = tableName;
        this.columns = new ArrayList<>();
    }

    public void addColumn(String name, String dataType, int sqlType, boolean nullable) {
        columns.add(new Column(name, dataType, sqlType, nullable));
    }

    public void addColumn(String name, String dataType, int sqlType, boolean nullable, int precision, int scale) {
        columns.add(new Column(name, dataType, sqlType, nullable, precision, scale));
    }

    /**
     * Represents a column in the table schema.
     */
    @Data
    @Getter
    public static class Column {
        private final String name;
        private final String dataType;
        private final int sqlType;
        private final boolean nullable;
        private final int precision;
        private final int scale;

        public Column(String name, String dataType, int sqlType, boolean nullable) {
            this(name, dataType, sqlType, nullable, 0, 0);
        }

        public Column(String name, String dataType, int sqlType, boolean nullable, int precision, int scale) {
            this.name = name;
            this.dataType = dataType;
            this.sqlType = sqlType;
            this.nullable = nullable;
            this.precision = precision;
            this.scale = scale;
        }
    }
}