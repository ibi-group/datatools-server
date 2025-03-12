package com.conveyal.datatools.manager.gtfsplus;

import java.util.List;

/**
 * Abstract class for collecting data from a GTFS+ table
 */
public abstract class TableScanner {

    /** Called to set up fields for a table */
    public abstract void setFields(List<String> fields);

    /** Called to scan/process a record in a table */
    public abstract void scanRecord(String[] rowValues);
}
