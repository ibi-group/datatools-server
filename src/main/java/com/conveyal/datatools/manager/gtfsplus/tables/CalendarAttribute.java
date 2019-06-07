package com.conveyal.datatools.manager.gtfsplus.tables;

import com.conveyal.gtfs.model.Entity;

import javax.naming.OperationNotSupportedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CalendarAttribute extends Entity {

    private static final long serialVersionUID = 1L;

    public String service_id;
    public String service_description;

    @Override public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) {
        throw new UnsupportedOperationException(
            "Cannot call setStatementParameters because loading a GTFS+ table into RDBMS is unsupported.");
    }
}
