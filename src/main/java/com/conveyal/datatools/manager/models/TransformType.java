package com.conveyal.datatools.manager.models;

public enum TransformType {
    REPLACE_FILE_FROM_STRING, // NYSDOT case for feed_info.txt
    REPLACE_FILE_FROM_VERSION, // MTC case for GTFS+
    APPEND_ROWS_FROM_STRING,
    ADD_FIELDS_FROM_VERSION,
    ADD_FIELDS_FROM_STRING, // route_id,route_color
    RUN_JOB
}
