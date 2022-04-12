package com.conveyal.datatools.manager.gtfsplus.tables;

import com.conveyal.gtfs.loader.DateField;
import com.conveyal.gtfs.loader.DoubleField;
import com.conveyal.gtfs.loader.IntegerField;
import com.conveyal.gtfs.loader.ShortField;
import com.conveyal.gtfs.loader.StringField;
import com.conveyal.gtfs.loader.Table;

import static com.conveyal.gtfs.loader.Requirement.OPTIONAL;
import static com.conveyal.gtfs.loader.Requirement.PROPRIETARY;
import static com.conveyal.gtfs.loader.Requirement.REQUIRED;

/**
 * This class contains GTFS+ table definitions that are based on gtfs-lib's {@link Table} constants.
 * Currently, these are only used when operating on tables being merged within
 * {@link com.conveyal.datatools.manager.jobs.MergeFeedsJob}. The definition of these tables can be
 * found at https://www.transitwiki.org/TransitWiki/images/e/e7/GTFS%2B_Additional_Files_Format_Ver_1.7.pdf.
 */
public class GtfsPlusTable {
    public static final Table REALTIME_ROUTES = new Table("realtime_routes", RealtimeRoute.class, PROPRIETARY,
        new StringField("route_id", REQUIRED).isReferenceTo(Table.ROUTES),
        new ShortField("realtime_enabled", REQUIRED, 1),
        new StringField("realtime_routename",  REQUIRED),
        new StringField("realtime_routecode", REQUIRED)
    );

    public static final Table REALTIME_STOPS = new Table("realtime_stops", RealtimeStop.class, PROPRIETARY,
        new StringField("trip_id", REQUIRED).isReferenceTo(Table.TRIPS),
        new StringField("stop_id", REQUIRED).isReferenceTo(Table.STOPS),
        new StringField("realtime_stop_id",  REQUIRED)
    ).keyFieldIsNotUnique();

    public static final Table DIRECTIONS = new Table("directions", Direction.class, PROPRIETARY,
        new StringField("route_id", REQUIRED).isReferenceTo(Table.ROUTES),
        new ShortField("direction_id", REQUIRED, 1),
        new StringField("direction", REQUIRED))
        .keyFieldIsNotUnique()
        .hasCompoundKey();

    public static final Table REALTIME_TRIPS = new Table("realtime_trips", RealtimeTrip.class, PROPRIETARY,
        new StringField("trip_id", REQUIRED).isReferenceTo(Table.TRIPS),
        new StringField("realtime_trip_id", REQUIRED)
    );

    public static final Table STOP_ATTRIBUTES = new Table("stop_attributes", StopAttribute.class, PROPRIETARY,
        new StringField("stop_id", REQUIRED).isReferenceTo(Table.STOPS),
        new ShortField("accessibility_id", REQUIRED, 8),
        new StringField("cardinal_direction", OPTIONAL),
        new StringField("relative_position", OPTIONAL),
        new StringField("stop_city", REQUIRED)
    );

    public static final Table TIMEPOINTS = new Table("timepoints", TimePoint.class, PROPRIETARY,
        new StringField("trip_id", REQUIRED).isReferenceTo(Table.TRIPS),
        new StringField("stop_id", REQUIRED).isReferenceTo(Table.STOPS)
    ).keyFieldIsNotUnique();

    public static final Table RIDER_CATEGORIES = new Table("rider_categories", RiderCategory.class, PROPRIETARY,
        new IntegerField("rider_category_id", REQUIRED, 1, 25),
        new StringField("rider_category_description", REQUIRED)
    );

    public static final Table FARE_RIDER_CATEGORIES = new Table("fare_rider_categories", FareRiderCategory.class, PROPRIETARY,
        new StringField("fare_id", REQUIRED),
        new IntegerField("rider_category_id", REQUIRED, 2, 25).isReferenceTo(RIDER_CATEGORIES),
        new DoubleField("price", REQUIRED, 0, Double.MAX_VALUE, 2),
        new DateField("expiration_date", OPTIONAL),
        new DateField("commencement_date", OPTIONAL)
    ).keyFieldIsNotUnique();

    public static final Table CALENDAR_ATTRIBUTES = new Table("calendar_attributes", CalendarAttribute.class, PROPRIETARY,
        new StringField("service_id", REQUIRED).isReferenceTo(Table.CALENDAR),
        new StringField("service_description", REQUIRED)
    );

    public static final Table FAREZONE_ATTRIBUTES = new Table("farezone_attributes", FareZoneAttribute.class, PROPRIETARY,
        new StringField("zone_id", REQUIRED),
        new StringField("zone_name", REQUIRED)
    );

    public static final Table ROUTE_ATTRIBUTES = new Table("route_attributes", RouteAttribute.class, PROPRIETARY,
        new StringField("route_id", REQUIRED),
        new StringField("category", REQUIRED),
        new StringField("subcategory", REQUIRED),
        new StringField("running_way", REQUIRED)
    );

    /**
     * List of tables in the order such that internal references can be appropriately checked as
     * tables are loaded/encountered.
     */
    public static final Table[] tables = new Table[] {
        REALTIME_ROUTES,
        REALTIME_STOPS,
        REALTIME_TRIPS,
        DIRECTIONS,
        STOP_ATTRIBUTES,
        TIMEPOINTS,
        RIDER_CATEGORIES,
        FARE_RIDER_CATEGORIES,
        CALENDAR_ATTRIBUTES,
        FAREZONE_ATTRIBUTES,
        ROUTE_ATTRIBUTES
    };
}
