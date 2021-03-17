package com.conveyal.datatools.manager.models;

/**
 * A class for a custom file used during trip planner deployments. The file can either be linked to via a URL or S3
 * bucket or the raw contents of it can be provided.
 */
public class CustomFile {
    /**
     * The raw contents of the custom file.
     */
    public String contents;

    /**
     * The name of the file as it should be named in the base folder for trip planner deployments.
     */
    public String filename;

    /**
     * The URL or S3 path to the file.
     */
    public String uri;

    /**
     * If true, this file should be downloaded and used when building a graph.
     */
    public boolean useDuringBuild = false;

    /**
     * If true, this file should be downloaded when running a trip planner server.
     */
    public boolean useDuringServe = false;

    @Override public String toString() {
        return "CustomFile{" + "useDuringBuild=" + useDuringBuild + ", useDuringServe=" + useDuringServe
            + ", filename='" + filename + '\'' + ", uri='" + uri + '\'' + ", contents='" + contents + '\'' + '}';
    }
}
