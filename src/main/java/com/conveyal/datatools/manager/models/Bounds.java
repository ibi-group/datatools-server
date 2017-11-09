package com.conveyal.datatools.manager.models;

import java.awt.geom.Rectangle2D;
import java.io.Serializable;

/**
 * Created by landon on 9/22/17.
 */
public class Bounds implements Serializable {
    private static final long serialVersionUID = 1L;
    public double north, south, east, west;

    /**
     * No-arg constructor for serialization.
     */
    public Bounds () {}

    /**
     * Create Bounds from java.awt.geom.Rectangle2D
     */
    public Bounds (Rectangle2D rectangle2D) {
        this.north = rectangle2D.getMaxY();
        this.south = rectangle2D.getMinY();
        this.east = rectangle2D.getMaxX();
        this.west = rectangle2D.getMinX();
    }

    public Rectangle2D.Double toRectangle2D () {
        return new Rectangle2D.Double(west, south,
                east - west, north - south);
    }

    public String toCSVString () {
        return west + "," + + south + "," + east + "," + north;
    }
}
