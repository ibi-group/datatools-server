package com.conveyal.datatools.editor.models.transit;

public enum StatusType {
    IN_PROGRESS,
    PENDING_APPROVAL,
    APPROVED,
    DISABLED;

    public int toInt () {
        switch (this) {
            case APPROVED:
                return 2;
            case IN_PROGRESS:
                return 1;
            case PENDING_APPROVAL:
                return 0;
            default:
                return 0;
        }
    }
}