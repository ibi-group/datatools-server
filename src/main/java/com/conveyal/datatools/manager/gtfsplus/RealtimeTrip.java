package com.conveyal.datatools.manager.gtfsplus;

import com.conveyal.gtfs.model.Entity;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class RealtimeTrip extends Entity {

    private static final long serialVersionUID = 1L;

    public String trip_id;
    public String realtime_trip_id;

    // TODO
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {

    }
}
