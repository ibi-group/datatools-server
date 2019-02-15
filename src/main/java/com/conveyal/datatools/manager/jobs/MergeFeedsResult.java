package com.conveyal.datatools.manager.jobs;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MergeFeedsResult {

    public int feedCount;
    public int errorCount;
    public MergeFeedsType type;
    public Set<String> idConflicts = new HashSet<>();
    public Set<String> skippedIds = new HashSet<>();
    public Map<String, String> remappedIds = new HashMap<>();
    public Map<String, Integer> linesPerTable = new HashMap<>();
    public int remappedReferences;
    public int recordsSkipCount;
    public Date startTime;
    public boolean failed;
    public Set<String> failureReasons = new HashSet<>();

    public MergeFeedsResult (MergeFeedsType type) {
        this.type = type;
        this.startTime = new Date();
    }

    public void addIdConflict (String id) {
        idConflicts.add(id);
        errorCount++;
        failed = true;
    }
}
