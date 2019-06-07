package com.conveyal.datatools.manager.gtfsplus.tables;

import com.conveyal.gtfs.model.Entity;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class FareRiderCategory extends Entity {

    private static final long serialVersionUID = 1L;

    public String fare_id;
    public int rider_category_id;
    public double price;
    public LocalDate expiration_date;
    public LocalDate commencement_date;

    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        throw new UnsupportedOperationException(
            "Cannot call setStatementParameters because loading a GTFS+ table into RDBMS is unsupported.");
    }
}
