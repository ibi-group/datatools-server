package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.gtfsplus.tables.GtfsPlusTable;
import com.conveyal.gtfs.loader.Table;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class dealing with GTFS tables.
 */
public class GtfsUtils {
    /**
     * Obtains a GTFS table.
     * @param tableName The name of the table to obtain (e.g. "agency.txt").
     * @return
     */
    public static Table getGtfsTable(String tableName) {
        final List<Table> tables = Arrays.stream(Table.tablesInOrder)
            .filter(Table::isSpecTable)
            .collect(Collectors.toList());
        if (DataManager.isModuleEnabled("gtfsplus")) {
            // Add GTFS+ tables only if MTC extension is enabled.
            tables.addAll(Arrays.asList(GtfsPlusTable.tables));
        }
        // Check that table name is valid (to prevent SQL injection).
        for (Table specTable : tables) {
            if (specTable.name.equals(tableName)) {
                return specTable;
            }
        }

        return null;
    }
}
