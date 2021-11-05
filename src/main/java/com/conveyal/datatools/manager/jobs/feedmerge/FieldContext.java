package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.gtfs.loader.Field;


/**
 * Holds data when processing a field in a CSV row during a feed merge.
 */
public class FieldContext {
    private final Field field;
    private final int index;
    private String value;
    private String valueToWrite;

    public FieldContext(Field field, int index, String value) {
        this.field = field;
        // Get index of field from GTFS spec as it appears in feed
        this.index = index;
        // Default value to write is unchanged from value found in csv (i.e. val). Note: if looking to
        // modify the value that is written in the merged file, you must update valueToWrite (e.g.,
        // updating this feed's end_date or accounting for cases where IDs conflict).
        resetValue(value);
    }

    public Field getField() {
        return field;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String newValue) {
        value = newValue;
    }

    public String getValueToWrite() {
        return valueToWrite;
    }

    public void setValueToWrite(String newValue) {
        valueToWrite = newValue;
    }

    /**
     * Resets both value and valueToWrite to a desired new value.
     */
    public void resetValue(String newValue) {
        value = valueToWrite = newValue;
    }
}
