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

    public static AttributeAvailabilityType fromGtfs (int availabilityType) {
        switch (availabilityType) {
            case 1:
                return AVAILABLE;
            case 2:
                return UNAVAILABLE;
            default: // if value is UNKNOWN or missing
                return UNKNOWN;
        }
    }
}