package com.conveyal.datatools.manager.utils.sql;

import com.conveyal.gtfs.loader.Table;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableCheckTest {
    private static final String NAMESPACE = "test-namespace";
    private static final String NAMESPACE_TYPE = "namespace";

    @Test
    void shouldDetectMissingTableColumns() {
        List<String> removedColumns = Lists.newArrayList("route_id", "route_short_name");
        List<ColumnCheck> columns = Arrays
            .stream(Table.ROUTES.fields)
            .filter(f -> !removedColumns.contains(f.name))
            .map(ColumnCheck::new)
            .collect(Collectors.toList());

        TableCheck tableInfo = new TableCheck(Table.ROUTES, NAMESPACE, NAMESPACE_TYPE, columns);
        assertEquals(
            String.join(",", removedColumns),
            tableInfo.missingColumns.stream().map(c -> c.columnName).collect(Collectors.joining(","))
        );
    }

    @Test
    void shouldDetectColumnsWithWrongType() {
        List<ColumnCheck> columns = Arrays
            .stream(Table.ROUTES.fields)
            .map(ColumnCheck::new)
            .collect(Collectors.toList());

        TableCheck tableInfo = new TableCheck(Table.ROUTES, NAMESPACE, NAMESPACE_TYPE, columns);
        assertTrue(tableInfo.columnsWithWrongType.isEmpty());

        // Modify the type of one column.
        for (ColumnCheck c : columns) {
            if (c.columnName.equals("route_short_name")) {
                c.setDataType("smallint");
                break;
            }
        }

        // The modified column should be flagged.
        tableInfo = new TableCheck(Table.ROUTES, NAMESPACE, NAMESPACE_TYPE, columns);
        assertEquals(1, tableInfo.columnsWithWrongType.size());
        ColumnCheck columnWithWrongType = tableInfo.columnsWithWrongType.get(0);
        assertEquals("route_short_name", columnWithWrongType.columnName);
        assertEquals("smallint", columnWithWrongType.getDataType());
        assertEquals("varchar", columnWithWrongType.getExpectedType());
    }
}
