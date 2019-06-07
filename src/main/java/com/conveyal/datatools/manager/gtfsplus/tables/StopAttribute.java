package com.conveyal.datatools.manager.gtfsplus.tables;

import com.conveyal.gtfs.model.Entity;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StopAttribute extends Entity {

    private static final long serialVersionUID = 1L;

    public String stop_id;
    public int accessibility_id;
    public String cardinal_direction;
    public String relative_position;
    public String stop_city;

    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        throw new UnsupportedOperationException(
            "Cannot call setStatementParameters because loading a GTFS+ table into RDBMS is unsupported.");
    }
}
