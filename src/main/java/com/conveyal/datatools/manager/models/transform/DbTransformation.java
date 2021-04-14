package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.gtfsplus.tables.GtfsPlusTable;
import com.conveyal.gtfs.loader.JdbcGtfsLoader;
import com.conveyal.gtfs.loader.Table;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This is an abstract class that represents a transformation that should apply to a GTFS in database form. In other
 * words, subclasses will provide a transform override method that acts on a database namespace. Sample fields
 * matchField and matchValues can be used to construct a WHERE clause for applying updates to a filtered set of records.
 */
public abstract class DbTransformation extends FeedTransformation {
    public String matchField;
    public List<String> matchValues;

    public abstract void transform(FeedTransformDbTarget target, MonitorableJob.Status status);

    @Override
    public void doTransform(FeedTransformTarget target, MonitorableJob.Status status) {
        if (!(target instanceof FeedTransformDbTarget)) {
            status.fail("Target must be FeedTransformDbTarget.");
            return;
        }

        // Validate parameters before running transform.
        validateTableAndFieldNames(status);
        validateParameters(status);
        if (status.error) {
            return;
        }

        // Cast transform target to DB flavor and pass it to subclasses to transform.
        transform((FeedTransformDbTarget)target, status);
    }

    private void validateTableAndFieldNames(MonitorableJob.Status status) {
        // Validate fields before running transform.
        final List<Table> tables =
            Arrays.stream(Table.tablesInOrder)
                .filter(Table::isSpecTable)
                .collect(Collectors.toList());
        if (DataManager.isModuleEnabled("gtfsplus")) {
            // Add GTFS+ tables only if MTC extension is enabled.
            tables.addAll(Arrays.asList(GtfsPlusTable.tables));
        }
        // Check that table name is valid (to prevent SQL injection).
        Table matchingTable = null;
        for (Table specTable : tables) {
            if (specTable.name.equals(table)) {
                matchingTable = specTable;
                break;
            }
        }
        if (matchingTable == null) {
            status.fail("Table must be valid GTFS spec table name (without .txt).");
            return;
        }
        if (matchField == null) {
            status.fail("Must provide valid match field");
            return;
        }
        String cleanField = JdbcGtfsLoader.sanitize(matchField, null);
        if (!matchField.equals(cleanField)) {
            status.fail("Input match field contained disallowed special characters (only alphanumeric and underscores are permitted).");
        }
    }
}
