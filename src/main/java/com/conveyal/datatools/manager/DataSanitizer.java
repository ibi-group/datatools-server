package com.conveyal.datatools.manager;

import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.GTFS;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.conveyal.gtfs.util.Util;
import com.google.common.collect.Lists;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.conveyal.datatools.manager.DataManager.GTFS_DATA_SOURCE;
import static com.conveyal.datatools.manager.DataManager.initializeApplication;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.nin;

/**
 * The Data sanitizer requires the env.yml and server.yml files for configuration. Data sanitizer specific command-line parameters
 * should be provided after these e.g.:
 * configurations/test/env.yml.tmp configurations/test/server.yml.tmp --orphaned --delete
 */
public class DataSanitizer {
    private static final Logger LOG = LoggerFactory.getLogger(DataSanitizer.class);
    public static final List<String> ORPHANED_FLAGS = List.of("--orphaned", "-O");
    public static final List<String> DELETE_FLAGS = List.of("--delete", "-D");

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
                sanitizeFeedVersions(DELETE_FLAGS.contains(flag));
                sanitizeDBSchemas(DELETE_FLAGS.contains(flag));
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
     * Group orphaned feed versions and optionally delete.
     */
    public static int sanitizeFeedVersions(boolean delete) {
        List<FeedVersion> feedVersions = getOrphanedFeedVersions();
        int orphaned = feedVersions.size();
        if (orphaned == 0) {
            System.out.println("No orphaned feed versions found!");
        } else {
            System.out.printf("%d orphaned feed versions:%n", orphaned);
            for (FeedVersion feedVersion : feedVersions) {
                System.out.printf("%-10s | %-10s | %-10s | %-10s%n", "ID", "Version", "Created", "Updated");
                System.out.printf(
                    "%-10s | %-10s | %-10s | %-10s%n",
                    feedVersion.name,
                    feedVersion.version,
                    feedVersion.dateCreated,
                    feedVersion.lastUpdated
                );
            }
        }

        if (delete && !feedVersions.isEmpty()) {
            System.out.println("Total orphaned feed versions deleted: " + deleteOrphanedFeedVersions(feedVersions));
        }
        return orphaned;
    }

    /**
     * Group orphaned schemas and optionally delete.
     */
    public static int sanitizeDBSchemas(boolean delete) {
        List<String> orphanedSchemas = getOrphanedDBSchemas(getFeedVersionSchemas());
        if (orphanedSchemas.isEmpty()) {
            System.out.println("No orphaned DB schemas found!");

        } else {
            System.out.println("Orphaned DB schemas: " + orphanedSchemas.size());
            for (String schema : orphanedSchemas) {
                System.out.println(schema);
            }
        }

        if (delete && !orphanedSchemas.isEmpty()) {
            System.out.println("Total orphaned DB schemas deleted: " + deleteOrphanedDBSchemas(orphanedSchemas));
        }
        return orphanedSchemas.size();
    }

    /**
     * Delete orphaned feed versions.
     */
    private static int deleteOrphanedFeedVersions(List<FeedVersion> feedVersions) {
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
        return deletedFeedVersions;
    }

    /**
     * Delete orphaned DB schemas.
     */
    public static int deleteOrphanedDBSchemas(List<String> orphanedSchemas) {
        int deletedSchemas = 0;
        for (String orphanedSchema : orphanedSchemas) {
            try {
                GTFS.delete(orphanedSchema, DataManager.GTFS_DATA_SOURCE);
                LOG.info("Dropped orphaned DB schema from Postgres.");
            } catch (SQLException | InvalidNamespaceException e) {
                System.err.printf("Failed to delete DB schema: %s. %s%n", orphanedSchema, e.getMessage());
            }
            deletedSchemas++;
        }
        return deletedSchemas;
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

    /**
     * Get all qualifying schemas that are not associated with a feed version.
     */
    public static List<String> getOrphanedDBSchemas(List<String> associatedSchemas) {
        String whereClause = associatedSchemas.isEmpty() ? "" : String.format(" WHERE nspname NOT IN (%s)", associatedSchemas
            .stream()
            .map(schema -> "'" + schema + "'")
            .collect(Collectors.joining(", "))
        );
        List<String> orphanedSchemas = new ArrayList<>();
        try (Connection connection = GTFS_DATA_SOURCE.getConnection()) {
            String sql = String.format("SELECT nspname FROM pg_namespace %s", whereClause);
            LOG.info(sql);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1);
                if (isValidSchema(schemaName)) {
                    orphanedSchemas.add(schemaName);
                }
            }
        } catch (SQLException e) {
            LOG.error("Unable to get orphaned DB schemas", e);
        }
        return orphanedSchemas;
    }

    /**
     * Make sure the schema qualifies as datatools-related schema.
     */
    private static boolean isValidSchema(String schemaName) {
        List<String> criticalSchemas = List.of("catalog", "information_schema", "public", "temp", "toast");
        try {
            Util.ensureValidNamespace(schemaName);
            if (criticalSchemas.stream().noneMatch(schemaName::contains)) {
                // Belts and braces in case the previous check changes.
                return true;
            }
        } catch (InvalidNamespaceException e) {
            return false;
        }
        return false;
    }

    /**
     * Extract the namespace (schema) from all FeedVersion documents.
     */
    public static List<String> getFeedVersionSchemas() {
        List<String> associatedSchemas = new ArrayList<>();

        List<Bson> stages = Lists.newArrayList(
            project(
                Projections.fields(Projections.include("namespace"))
            )
        );
        for (Document feedVersionDocument : Persistence.getDocuments("FeedVersion", stages)) {
            associatedSchemas.add(feedVersionDocument.getString("namespace"));
        }
        return associatedSchemas;
    }
}