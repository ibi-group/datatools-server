package com.conveyal.datatools.editor.models.transit;

public enum AttributeAvailabilityType {
    UNKNOWN,
    AVAILABLE,
    UNAVAILABLE;

    public int toGtfs () {
        switch (this) {
            case AVAILABLE:
                return 1;
            case UNAVAILABLE:
                return 2;
            default: // if value is UNKNOWN or missing
                return 0;
        }
    }
}