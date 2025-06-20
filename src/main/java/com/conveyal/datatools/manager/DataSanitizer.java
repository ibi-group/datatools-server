package com.conveyal.datatools.manager;

import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.FeedVersionSummary;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.mongodb.client.model.Sorts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.conveyal.datatools.manager.DataManager.initializeApplication;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.nin;

/**
 * The Data sanitizer requires the env.yml and server.yml files for configuration. Data sanitizer specific parameters
 * should be provided after these e.g.:
 * configurations/test/env.yml.tmp configurations/test/server.yml.tmp --orphaned --delete
 */
public class DataSanitizer {
    public static final Logger LOG = LoggerFactory.getLogger(DataSanitizer.class);

    public static final List<String> ORPHANED_FLAGS = List.of("--orphaned", "-O");
    public static final List<String> DELETE_FLAGS = List.of("--delete", "-D");
    public static final List<String> FEED_VERSIONS_FLAGS = List.of("--feedversions", "-F");

    public static void main(String[] args) throws IOException {
        initializeApplication(args, false);
        parseArguments(args);
    }

    /**
     * Parse the arguments provided on the command line. The first two arguments must reference the env.yml and
     * server.yml files.
     */
    public static void parseArguments(String[] arguments) {
        Map<String, Set<String>> groupedParams = parseCommandLineArguments(arguments);
        groupedParams.forEach((flag, commandValues) -> {
            if (ORPHANED_FLAGS.contains(flag)) {
                sanitizeOrphanedFeedVersions(DELETE_FLAGS.contains(flag));
            }
            if (FEED_VERSIONS_FLAGS.contains(flag)) {
                feedVersionAudit();
            }
        });
    }

    /**
     * Group commands and command arguments.
     */
    private static Map<String, Set<String>> parseCommandLineArguments(String[] args) {
        Map<String, Set<String>> groupedParams = new HashMap<>();
        String currentKey = null;

        for (String arg : args) {
            if (arg.startsWith("-")) {
                currentKey = arg;
                groupedParams.put(currentKey, new HashSet<>());
            } else if (currentKey != null) {
                groupedParams.get(currentKey).add(arg);
            }
        }
        return groupedParams;
    }

    /**
     * Group orphaned feed versions and delete.
     */
    public static int sanitizeOrphanedFeedVersions(boolean delete) {
        List<FeedVersion> feedVersions = getOrphanedFeedVersions();
        int orphaned = feedVersions.size();
        if (orphaned == 0) {
            System.out.println("No orphaned versions found!");
        } else {
            System.out.printf("Orphaned feed versions (%s)%n", orphaned);
            for (FeedVersion feedVersion : feedVersions) {
                System.out.printf("%-10s | %-10s | %-10s | %-10s%n", "ID", "Version", "Created", "Updated");
                System.out.printf(
                    "%-10s | %-10s | %-10s | %-10s%n",
                    feedVersion.id,
                    feedVersion.version,
                    feedVersion.dateCreated,
                    feedVersion.lastUpdated
                );
            }
        }
        if (delete && !feedVersions.isEmpty()) {
            int deletedFeedVersions = 0;
            for (FeedVersion feedVersion : feedVersions) {
                try {
                    System.out.println("Deleting orphaned feed version: " + feedVersion.id);
                    feedVersion.deleteOrphan();
                } catch (SQLException | CheckedAWSException | InvalidNamespaceException e) {
                    System.err.printf("Failed to delete feed version: %s. %s%n", feedVersion.id, e.getMessage());
                }
                deletedFeedVersions++;
            }
            System.out.println("Total orphaned feed versions deleted: " + deletedFeedVersions);
        }
        return orphaned;
    }

    /**
     * For a given feed source, delete feed version prior to the keep number.
     */
    public static int sanitizeObsoleteFeedVersions(String feedSourceId, int numberOfPreviousVersionsToKeep) {
        Collection<FeedVersion> feedVersions = Persistence.feedVersions.getFiltered(
            eq("feedSourceId", feedSourceId),
            Sorts.descending("version")
        );
        if (feedVersions.isEmpty() || numberOfPreviousVersionsToKeep >= feedVersions.size()) {
            LOG.info("No feed versions or none that qualify for deletion.");
            return -1;
        }

        int keepCount = 0;
        int deleteCount = 0;
        
        for (FeedVersion feedVersion : feedVersions) {
            if (keepCount < numberOfPreviousVersionsToKeep) {
                keepCount++;
            } else {
                feedVersion.delete();
                deleteCount++;
            }
        }
        return deleteCount;
    }

    /**
     * Group feed source and number of feed versions.
     */
    public static Map<String, Integer> feedVersionAudit() {
        Map<String, Integer> audit = new HashMap<>();
        List<FeedSource> feedSources = Persistence.feedSources.getAll();
        System.out.printf("Feed version audit for %s feed sources%n", feedSources.size());
        for (FeedSource feedSource : feedSources) {
            Collection<FeedVersionSummary> feedVersions = feedSource.retrieveFeedVersionSummaries();
            System.out.printf("%-10s | %-10s%n", "Feed Source", "No. Feed Versions");
            System.out.printf(
                "%-10s | %-10s%n",
                feedSource.id,
                feedVersions.size()
            );
            audit.put(feedSource.id, feedVersions.size());
        }
        return audit;
    }

    /**
     * Produce a list of feed versions that are not attached to a feed source.
     */
    private static List<FeedVersion> getOrphanedFeedVersions() {
        List<FeedSource> feedSources = Persistence.feedSources.getAll();
        Set<String> feedSourceIds = feedSources
            .stream()
            .map(feedSource -> feedSource.id)
            .collect(Collectors.toSet());
        return Persistence.feedVersions.getFiltered(nin("feedSourceId", feedSourceIds));
    }
}
