package com.conveyal.datatools.manager.models;

/**
 * Defines all of the JSON views.
 * This is basically just a list.
 * @author mattwigway
 *
 */
public class JsonViews {
    /**
     * Data that is exposed to the UI via the API.
     */
    public static class UserInterface {};

    /**
     * Data that should be included in a database dump.
     */
    public static class DataDump {};
}
