package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.Table;

import java.util.Set;
import java.util.stream.Collectors;

import static com.conveyal.datatools.manager.jobs.feedmerge.MergeLineContext.SERVICE_ID;

public class ReferenceTableDiscovery {

    public static final String REF_TABLE_SEPARATOR = "#~#";

    /**
     * Tables that have two or more foreign references.
     */
    public enum ReferenceTableKey {

        TRIP_SERVICE_ID_KEY(
            String.join(
                REF_TABLE_SEPARATOR,
                Table.TRIPS.name,
                SERVICE_ID,
                Table.CALENDAR.name,
                Table.CALENDAR_DATES.name,
                Table.SCHEDULE_EXCEPTIONS.name
            )
        ),
        LOCATION_GROUP_STOPS_STOP_ID_KEY(
            String.join(
                REF_TABLE_SEPARATOR,
                Table.LOCATION_GROUP_STOPS.name,
                "stop_id",
                Table.STOPS.name,
                Table.LOCATIONS.name
            )
        ),
        STOP_TIMES_STOP_ID_KEY(
            String.join(
                REF_TABLE_SEPARATOR,
                Table.STOP_TIMES.name,
                "stop_id",
                Table.STOPS.name,
                Table.LOCATIONS.name,
                Table.LOCATION_GROUP_STOPS.name
            )
        );

        private final String value;

        ReferenceTableKey(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static ReferenceTableKey fromValue(String key) {
            for (ReferenceTableKey ref: ReferenceTableKey.values()) {
                if (ref.getValue().equals(key)) {
                    return ref;
                }
            }
            throw new UnsupportedOperationException(String.format("Unsupported reference table key: %s.", key));
        }
    }

    /**
     * Get reference table key by matching the provided values to predefined reference table keys.
     */
    public static ReferenceTableKey getReferenceTableKey(Field field, Table table) {
        return ReferenceTableKey.fromValue(createKey(field, table));
    }

    /**
     * Create a unique key for this table, field and reference tables.
     */
    public static String createKey(Field field, Table table) {
        return String.format(
            "%s%s%s%s%s",
            table.name,
            REF_TABLE_SEPARATOR,
            field.name,
            REF_TABLE_SEPARATOR,
            field.referenceTables.stream().map(r -> r.name).collect(Collectors.joining(REF_TABLE_SEPARATOR))
        );
    }

    /**
     * Define the reference table for a trip service id.
     */
    public static Table getTripServiceIdReferenceTable(
        String fieldValue,
        MergeFeedsResult mergeFeedsResult,
        String calendarKey,
        String calendarDatesKey
    ) {
        if (
            mergeFeedsResult.calendarServiceIds.contains(fieldValue) ||
            mergeFeedsResult.skippedIds.contains(calendarKey)
        ) {
            return Table.CALENDAR;
        } else if (
            mergeFeedsResult.calendarDatesServiceIds.contains(fieldValue) ||
            mergeFeedsResult.skippedIds.contains(calendarDatesKey)
        ) {
            return Table.CALENDAR_DATES;
        }
        return null;
    }

    /**
     * Define the reference table for a location group stop: stop id. This will either be a stop or null.
     */
    public static Table getLocationGroupStopReferenceTable(
        String fieldValue,
        MergeFeedsResult mergeFeedsResult
    ) {
        if (mergeFeedsResult.stopIds.contains(fieldValue)) {
            return Table.STOPS;
        }
        return null;
    }

    /**
     * Define the reference table for a stop time's stop id. This will either be a stop, location or location group.
     * TODO: In later PR's this will be redundant as a stop time: stop id will only reference a stop.
     */
    public static Table getStopTimeReferenceTable(
        String fieldValue,
        MergeFeedsResult mergeFeedsResult,
        Set<String> locationIds
    ) {
        if (mergeFeedsResult.stopIds.contains(fieldValue)) {
            return Table.STOPS;
        } else if (locationIds.contains(fieldValue)) {
            return Table.LOCATIONS;
        } else if (mergeFeedsResult.locationGroupStopIds.contains(fieldValue)) {
            return Table.LOCATION_GROUP_STOPS;
        }
        return null;
    }
}
