package com.conveyal.datatools.manager.gtfsplus.tables;

import com.conveyal.gtfs.model.Entity;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Direction extends Entity {

    private static final long serialVersionUID = 1L;

    public String route_id;
    public int direction_id;
    public String direction;


    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        throw new UnsupportedOperationException(
            "Cannot call setStatementParameters because loading a GTFS+ table into RDBMS is unsupported.");
    }
}
