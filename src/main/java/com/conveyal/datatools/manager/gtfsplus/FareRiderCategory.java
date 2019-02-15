package com.conveyal.datatools.manager.gtfsplus;

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

    // TODO
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {

    }
}
