package com.conveyal.datatools.manager.utils.sql;

import com.conveyal.gtfs.loader.Table;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NamespaceCheckTest {
    @Test
    void shouldDetectMissingTables() {
        List<String> excludedTables = Lists.newArrayList(Table.ROUTES.name, Table.TRIPS.name);
        NamespaceCheck nsInfo = new NamespaceCheck("namespace", null, "type", excludedTables);
        assertEquals(
            String.join(",", excludedTables),
            nsInfo.missingTables.stream().map(t -> t.name).collect(Collectors.joining(","))
        );
    }

    @Test
    void shouldDetectOrphanNamespace() {
        List<String> excludedTables = Arrays.stream(Table.tablesInOrder).map(t -> t.name).collect(Collectors.toList());
        NamespaceCheck nsInfo = new NamespaceCheck("namespace", null, "type", excludedTables);
        assertTrue(nsInfo.isOrphan());
    }
}
