package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.Table;

import java.util.stream.Collectors;

import static com.conveyal.datatools.manager.jobs.feedmerge.MergeLineContext.SERVICE_ID;

public class ReferenceTableDiscovery {

    public static final String REF_TABLE_SEPARATOR = "#~#";

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
}
