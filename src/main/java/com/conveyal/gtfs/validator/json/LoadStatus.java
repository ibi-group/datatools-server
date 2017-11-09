package com.conveyal.gtfs.validator.json;

/**
 * Created by landon on 7/31/17.
 * Note: this is effectively a placeholder class to override the
 * conveyal/gtfs-validator class in the same package (com.conveyal.gtfs.validator.json).
 * gtfs-validator has been removed from this application, but things won't
 * deserialize without this enum class (in this package).
 */
/** Why a GTFS feed failed to load */
public enum LoadStatus {
    SUCCESS, INVALID_ZIP_FILE, OTHER_FAILURE, MISSING_REQUIRED_FIELD, INCORRECT_FIELD_COUNT_IMPROPER_QUOTING
}
