package com.conveyal.datatools.manager.gtfsplus;

import com.conveyal.gtfs.model.Entity;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CalendarAttribute extends Entity {

    private static final long serialVersionUID = 1L;

    public String service_id;
    public String service_description;

    // TODO
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {

    }
}
