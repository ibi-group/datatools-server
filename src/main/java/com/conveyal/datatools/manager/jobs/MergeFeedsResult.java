package com.conveyal.datatools.manager.jobs;

import java.util.Date;

public class MergeFeedsResult {

    public int feedCount;
    public int errors;
    public Date startTime;
    public boolean failed;

    public MergeFeedsResult () {
        startTime = new Date();
    }
}
