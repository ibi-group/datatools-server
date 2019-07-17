package com.conveyal.datatools.manager.gtfsplus;

import java.io.Serializable;

/** A validation issue for a GTFS+ field. Use rowIndex = -1 for a table level issue. */
public class ValidationIssue implements Serializable {
    private static final long serialVersionUID = 1L;
    public String tableId;
    public String fieldName;
    public int rowIndex;
    public String description;

    public ValidationIssue(String tableId, String fieldName, int rowIndex, String description) {
        this.tableId = tableId;
        this.fieldName = fieldName;
        this.rowIndex = rowIndex;
        this.description = description;
    }
}
