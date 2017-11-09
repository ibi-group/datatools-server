package com.conveyal.datatools.manager.models;

/** Why a GTFS feed failed to load */
public enum LoadStatus {
    SUCCESS, INVALID_ZIP_FILE, OTHER_FAILURE, MISSING_REQUIRED_FIELD, INCORRECT_FIELD_COUNT_IMPROPER_QUOTING;
}
