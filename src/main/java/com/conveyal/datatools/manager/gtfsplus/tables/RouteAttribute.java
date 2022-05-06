package com.conveyal.datatools.manager.gtfsplus.tables;

import com.conveyal.gtfs.model.Entity;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class RouteAttribute extends Entity {

    private static final long serialVersionUID = 1L;

    public String route_id;
    public String category;
    public String subcategory;
    public String running_way;

    @Override
    public void setStatementParameters(PreparedStatement preparedStatement, boolean b) throws SQLException {
        throw new UnsupportedOperationException(
            "Cannot call setStatementParameters because loading a GTFS+ table into RDBMS is unsupported.");
    }
}
