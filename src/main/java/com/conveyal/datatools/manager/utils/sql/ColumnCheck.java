package com.conveyal.datatools.manager.utils.sql;

import com.conveyal.gtfs.loader.Field;

/**
 * Contains the outcome of a column check (i.e. whether the type is as expected).
 */
public class ColumnCheck {
    public final String columnName;
    private String dataType;
    private String expectedType;

    public ColumnCheck(String columnName, String dataType) {
        this.columnName = columnName;
        this.dataType = dataType;

        // Normalize data types to the ones used by the GTFS-lib tables.
        if (dataType.equals("character varying")) {
            this.dataType = "varchar";
        } else if (dataType.equals("ARRAY")) {
            this.dataType = "text[]";
        }
    }

    public ColumnCheck(Field field) {
        this(field.name, field.getSqlTypeName());
        this.expectedType = this.dataType;
    }

    public String getAlterColumnTypeSql() {
        return String.format("ALTER COLUMN %s TYPE %s USING %s::%s", columnName, expectedType, columnName, expectedType);
    }

    public String getAddColumnSql() {
        return String.format("ADD COLUMN IF NOT EXISTS %s %s", columnName, expectedType);
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getExpectedType() {
        return expectedType;
    }

    public void setExpectedType(String expectedType) {
        this.expectedType = expectedType;
    }

    public boolean isSameTypeAs(Field field) {
        return dataType.equals(field.getSqlTypeName());
    }
}
