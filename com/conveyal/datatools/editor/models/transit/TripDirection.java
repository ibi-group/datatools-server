package com.conveyal.datatools.editor.models.transit;

public enum TripDirection {
    A,
    B;

    public int toGtfs () {
        return this == TripDirection.A ? 0 : 1;
    }

    public static TripDirection fromGtfs (int dir) {
        return dir == 0 ? TripDirection.A : TripDirection.B;
    }
}