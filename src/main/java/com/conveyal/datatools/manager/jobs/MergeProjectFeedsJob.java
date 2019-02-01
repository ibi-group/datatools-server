package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.Consts;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * Created by landon on 9/19/17.
 */
public class MergeProjectFeedsJob extends MonitorableJob {

    private static final Logger LOG = LoggerFactory.getLogger(MergeProjectFeedsJob.class);
    public final Project project;

    public MergeProjectFeedsJob(Project project, String owner) {
        super(owner, "Merging project feeds for " + project.name, JobType.MERGE_PROJECT_FEEDS);
        this.project = project;
        status.message = "Merging feeds...";
    }

    @Override
    public void jobLogic () {
        Set<FeedVersion> feedVersions = new HashSet<>();
        // Get latest version for each feed source in project
        Collection<FeedSource> feedSources = project.retrieveProjectFeedSources();
        for (FeedSource fs : feedSources) {
            // check if feed version exists
            FeedVersion version = fs.retrieveLatest();
            if (version == null) {
                LOG.warn("Skipping {} because it has no feed versions", fs.name);
                continue;
            }
            // modify feed version to use prepended feed id
            LOG.info("Adding {} feed to merged zip", fs.name);
            feedVersions.add(version);
        }
        addNextJob(new MergeFeedsJob(owner, feedVersions, String.format("%s.zip", project.id), MergeFeedsJob.MergeType.REGIONAL));

    }
}
