package com.conveyal.datatools.manager.utils.sql;

import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.Requirement;
import com.conveyal.gtfs.loader.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Contains the outcome of a table check (i.e. whether columns are missing or of the wrong type).
 */
public class TableCheck {
    private final String namespace;
    public final Table table;
    public final List<ColumnCheck> missingColumns = new ArrayList<>();
    public final List<ColumnCheck> columnsWithWrongType = new ArrayList<>();

    public TableCheck(Table table, String namespace, String namespaceType, List<ColumnCheck> columns) {
        this.namespace = namespace;
        this.table = table;

        for (Field field : table.fields) {
            Optional<ColumnCheck> foundColumnForField = columns
                .stream()
                .filter(c -> c.columnName.equals(field.name))
                .findFirst();

            if (foundColumnForField.isPresent()) {
                ColumnCheck columnForField = foundColumnForField.get();
                // Are fields that are present of the correct type?
                if (!columnForField.isSameTypeAs(field)) {
                    columnForField.setExpectedType(field.getSqlTypeName());
                    columnsWithWrongType.add(columnForField);
                }
                // Only the id column seems to be marked as not nullable, so we won't check that for now.
            } else if (
                namespaceType.equals("editor") || field.requirement != Requirement.EDITOR
            ) {
                // Report missing editor-only tables only if the namespace is an editor namespace.
                // Report missing non-editor tables in all other cases.
                missingColumns.add(new ColumnCheck(field));
            }
        }
    }

    public TableCheck(Table table, String namespace, String namespaceType, SqlSchemaUpdater schemaUpdater) {
        this(table, namespace, namespaceType, schemaUpdater.getColumns(namespace, table));
    }

    public boolean hasColumnIssues() {
        return !missingColumns.isEmpty() || !columnsWithWrongType.isEmpty();
    }

    /**
     * Builds the ALTER TABLE SQL statement to add columns to the table.
     */
    public String getAddColumnsSql() {
        return String.format(
            "ALTER TABLE %s.%s %s;",
            namespace,
            table.name,
            String.join(", ", getColumnSqlParts(missingColumns, ColumnCheck::getAddColumnSql))
        );
    }

    /**
     * Builds the ALTER TABLE SQL statement to modify existing columns to the table.
     */
    public String getAlterColumnsSql() {
        return String.format(
            "ALTER TABLE %s.%s %s;",
            namespace,
            table.name,
            String.join(", ", getColumnSqlParts(columnsWithWrongType, ColumnCheck::getAlterColumnTypeSql))
        );
    }

    /**
     * Builds the part of an SQL statement that addscolumns.
     */
    private List<String> getColumnSqlParts(
        List<ColumnCheck> columns,
        Function<ColumnCheck, String> mapper
    ) {
        return columns
            .stream()
            .map(mapper)
            .collect(Collectors.toList());
    }

    public void printReport() {
        if (hasColumnIssues()) {
            System.out.printf("\t\t\tIssues in table: %s%n", table.name);
            missingColumns.forEach(c -> System.out.printf("\t\t\t\tMissing column: %s%n", c.columnName));
            columnsWithWrongType.forEach(
                c -> System.out.printf(
                    "\t\t\t\tIncorrect type for column: %s - expected: %s - actual: %s%n",
                    c.columnName,
                    c.getExpectedType(),
                    c.getDataType()
                )
            );
        }
    }
}
