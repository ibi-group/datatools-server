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
            String.join(",", nsInfo.missingTables)
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
            String.join(",", tableInfo.missingColumns)
        );
    }
}
