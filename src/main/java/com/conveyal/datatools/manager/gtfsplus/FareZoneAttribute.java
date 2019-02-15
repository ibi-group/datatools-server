package com.conveyal.datatools.manager.gtfsplus;

import com.conveyal.gtfs.model.Entity;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FareZoneAttribute extends Entity {

    private static final long serialVersionUID = 1L;

    public String zone_id;
    public String zone_name;

    // TODO
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {

    }
}
