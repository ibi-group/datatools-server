package com.conveyal.datatools.manager.gtfsplus;

import java.util.List;

/**
 * Interface for collecting data from a GTFS+ table
 */
public interface TableScanner {

    /** Called to set up fields for a table */
    void setFields(List<String> fields);

    /** Called to scan/process a record in a table */
    void scanRecord(String[] rowValues);

    /** Whether the headers can be used (e.g. are there required columns missing) */
    boolean canUseHeaders();
}
