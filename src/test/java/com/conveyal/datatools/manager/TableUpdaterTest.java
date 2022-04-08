package com.conveyal.datatools.manager;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.utils.GtfsUtils;
import com.conveyal.gtfs.loader.Table;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TableUpdaterTest extends DatatoolsTest {

    @BeforeAll
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
    }

    @AfterAll
    public static void tearDown() {

    }

    @Test
    void shouldDetectMissingTableInNamespace() {
        List<String> excludedTables = Lists.newArrayList(Table.ROUTES.name, Table.TRIPS.name);
        GtfsUtils.NamespaceInfo nsInfo = new GtfsUtils.NamespaceInfo("namespace", excludedTables);
        assertEquals(
            String.join(",", excludedTables),
            nsInfo.missingTables.stream().map(t -> t.name).collect(Collectors.joining(","))
        );
    }

    @Test
    void shouldDetectMissingTableColumns() {
        List<String> removedColumns = Lists.newArrayList("route_id", "route_short_name");
        List<GtfsUtils.ColumnInfo> columns = Arrays
            .stream(Table.ROUTES.fields)
            .filter(f -> !removedColumns.contains(f.name))
            .map(f -> new GtfsUtils.ColumnInfo(f.name, f.getSqlTypeName()))
            .collect(Collectors.toList());

        GtfsUtils.TableInfo tableInfo = new GtfsUtils.TableInfo(Table.ROUTES, columns);
        assertEquals(
            String.join(",", removedColumns),
            tableInfo.missingColumns.stream().map(c -> c.columnName).collect(Collectors.joining(","))
        );
    }

    @Test
    void shouldDetectColumnsWithWrongType() {
        List<GtfsUtils.ColumnInfo> columns = Arrays
            .stream(Table.ROUTES.fields)
            .map(f -> new GtfsUtils.ColumnInfo(f.name, f.getSqlTypeName()))
            .collect(Collectors.toList());

        GtfsUtils.TableInfo tableInfo = new GtfsUtils.TableInfo(Table.ROUTES, columns);
        assertTrue(tableInfo.columnsWithWrongType.isEmpty());

        // Modify the type of one column
        for (GtfsUtils.ColumnInfo c : columns) {
            if (c.columnName.equals("route_short_name")) {
                c.dataType = "smallint";
                break;
            }
        }

        tableInfo = new GtfsUtils.TableInfo(Table.ROUTES, columns);
        assertEquals(1, tableInfo.columnsWithWrongType.size());
    }

    @Test
    void shouldDetectOrphanNamespace() {
        List<String> excludedTables = Arrays.stream(Table.tablesInOrder).map(t -> t.name).collect(Collectors.toList());
        GtfsUtils.NamespaceInfo nsInfo = new GtfsUtils.NamespaceInfo("namespace", excludedTables);
        assertTrue(nsInfo.isOrphan());
    }
}
