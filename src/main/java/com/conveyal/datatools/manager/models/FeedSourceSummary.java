package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.editor.utils.JacksonSerializers;
import com.conveyal.datatools.manager.gtfsplus.GtfsPlusValidation;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.graphql.fetchers.ErrorCountFetcher;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Lists;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UnwindOptions;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.replaceRoot;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.in;

/**
 *  For explicit mongo queries (matching the queries defined in this class) see resources/mongo and README.md for
 *  explanation of use.
 */
public class FeedSourceSummary {
    public String projectId;

    public String id;

    public String name;
    public boolean deployable;
    public boolean isPublic;

    /** An optional display filename for the feed in the bundle, e.g. "agency_transit.zip" */
    public String filename;

    @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
    @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
    public LocalDate lastUpdated;

    public List<String> labelIds = new ArrayList<>();

    public String deployedFeedVersionId;

    @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
    @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
    public LocalDate deployedFeedVersionStartDate;

    @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
    @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
    public LocalDate deployedFeedVersionEndDate;

    public Integer deployedFeedVersionIssues;

    public LatestValidationResult latestValidation;

    public String url;

    public List<String> noteIds = new ArrayList<>();

    public String organizationId;

    public Date latestProcessedByExternalPublisher;

    public Date latestSentToExternalPublisher;

    public GtfsPlusValidation latestGtfsPlusValidation;

    public String latestPublishedVersionId;

    public String latestNamespace;

    public FeedValidationResultSummary publishedValidationSummary;

    public List<ErrorCountFetcher.ErrorCount> latestErrorCounts = new ArrayList<>();

    public FeedSourceSummary() {
    }

    public FeedSourceSummary(String projectId, String organizationId, Document feedSourceDocument) {
        this.projectId = projectId;
        this.organizationId = organizationId;
        id = feedSourceDocument.getString("_id");
        name = feedSourceDocument.getString("name");
        deployable = feedSourceDocument.getBoolean("deployable");
        isPublic = feedSourceDocument.getBoolean("isPublic");
        List<String> documentLabelIds = feedSourceDocument.getList("labelIds", String.class);
        if (documentLabelIds != null) {
            labelIds = documentLabelIds;
        }
        List<String> documentNoteIds = feedSourceDocument.getList("noteIds", String.class);
        if (documentNoteIds != null) {
            noteIds = documentNoteIds;
        }
        // Convert to local date type for consistency.
        lastUpdated = getLocalDateFromDate(feedSourceDocument.getDate("lastUpdated"));
        url = feedSourceDocument.getString("url");
        // Get optional filename.
        filename = feedSourceDocument.getString("filename");
    }

    /**
     * Set the common feed version values.
     */
    public void setFeedVersionValues(FeedVersionSummary feedVersionSummary) {
        if (feedVersionSummary == null) {
            return;
        }
        latestValidation = new LatestValidationResult(feedVersionSummary);
        latestProcessedByExternalPublisher = feedVersionSummary.processedByExternalPublisher;
        latestSentToExternalPublisher = feedVersionSummary.sentToExternalPublisher;
        latestGtfsPlusValidation = feedVersionSummary.gtfsPlusValidation;
        latestNamespace = feedVersionSummary.namespace;
        latestPublishedVersionId = feedVersionSummary.publishedVersionId;
        publishedValidationSummary = new FeedValidationResultSummary();
        publishedValidationSummary.errorCount = feedVersionSummary.publishedFeedVersionErrorCount;
        publishedValidationSummary.startDate = feedVersionSummary.publishedFeedVersionStartDate;
        publishedValidationSummary.endDate = feedVersionSummary.publishedFeedVersionEndDate;
    }

    /**
     * Set the deployed feed version values. For consistency, if no error count is available set the related number of
     * issues to zero.
     */
    public void setDeployedFeedVersionValues(FeedVersionSummary feedVersionSummary) {
        if (feedVersionSummary == null) {
            return;
        }
        deployedFeedVersionId = feedVersionSummary.id;
        deployedFeedVersionStartDate = feedVersionSummary.validationResult.firstCalendarDate;
        deployedFeedVersionEndDate = feedVersionSummary.validationResult.lastCalendarDate;
        deployedFeedVersionIssues = (feedVersionSummary.validationResult.errorCount == -1)
            ? 0
            : feedVersionSummary.validationResult.errorCount;
    }

    /**
     * Get all feed source summaries matching the project id.
     */
    public static List<FeedSourceSummary> getFeedSourceSummaries(String projectId, String organizationId) {
        List<Bson> stages = Lists.newArrayList(
            match(
                in("projectId", projectId)
            ),
            project(
                Projections.fields(Projections.include(
                    "_id",
                    "name",
                    "deployable",
                    "isPublic",
                    "lastUpdated",
                    "labelIds",
                    "url",
                    "filename",
                    "noteIds")
                )
            ),
            sort(Sorts.ascending("name"))
        );
        return extractFeedSourceSummaries(projectId, organizationId, stages);
    }

    /**
     * Get the latest feed version from all feed sources for this project.
     */
    public static Map<String, FeedVersionSummary> getLatestFeedVersionForFeedSources(String projectId) {
        List<Bson> stages = Lists.newArrayList(
            match(
                in("projectId", projectId)
            ),
            lookup("FeedVersion", "_id", "feedSourceId", "feedVersions"),
            lookup("FeedVersion", "publishedVersionId", "namespace", "publishedFeedVersion"),
            unwind("$feedVersions"),
            unwind("$publishedFeedVersion", new UnwindOptions().preserveNullAndEmptyArrays(true)),
            group(
                "$_id",
                Accumulators.first("publishedVersionId", "$publishedVersionId"),
                Accumulators.first("publishedFeedVersionErrorCount", "$publishedFeedVersion.validationResult.errorCount"),
                Accumulators.first("publishedFeedVersionStartDate", "$publishedFeedVersion.validationResult.firstCalendarDate"),
                Accumulators.first("publishedFeedVersionEndDate", "$publishedFeedVersion.validationResult.lastCalendarDate"),
                Accumulators.last("feedVersionId", "$feedVersions._id"),
                Accumulators.last("firstCalendarDate", "$feedVersions.validationResult.firstCalendarDate"),
                Accumulators.last("lastCalendarDate", "$feedVersions.validationResult.lastCalendarDate"),
                Accumulators.last("errorCount", "$feedVersions.validationResult.errorCount"),
                Accumulators.last("processedByExternalPublisher", "$feedVersions.processedByExternalPublisher"),
                Accumulators.last("sentToExternalPublisher", "$feedVersions.sentToExternalPublisher"),
                Accumulators.last("gtfsPlusValidation", "$feedVersions.gtfsPlusValidation"),
                Accumulators.last("namespace", "$feedVersions.namespace")
            )
        );
        return extractFeedVersionSummaries(
            "FeedSource",
            "feedVersionId",
            "_id",
            false,
            stages
        );
    }

    /**
     * Get the deployed feed versions from the latest deployment for this project.
     */
    public static Map<String, FeedVersionSummary> getFeedVersionsFromLatestDeployment(String projectId) {
        List<Bson> stages = Lists.newArrayList(
            match(
                in("_id", projectId)
            ),
            lookup("Deployment", "_id", "projectId", "deployments"),
            unwind("$deployments"),
            replaceRoot("$deployments"),
            sort(Sorts.descending("lastUpdated")),
            limit(1),
            lookup("FeedVersion", "feedVersionIds", "_id", "feedVersions"),
            unwind("$feedVersions"),
            replaceRoot("$feedVersions"),
            project(
                Projections.fields(Projections.include(
                    "feedSourceId",
                    "validationResult.firstCalendarDate",
                    "validationResult.lastCalendarDate",
                    "validationResult.errorCount")
                )
            )
        );
        return extractFeedVersionSummaries(
            "Project",
            "_id",
            "feedSourceId",
            true,
            stages
        );
    }

    /**
     * Get the deployed feed version from the pinned deployment for this feed source.
     */
    public static Map<String, FeedVersionSummary> getFeedVersionsFromPinnedDeployment(String projectId) {
        List<Bson> stages = Lists.newArrayList(
            match(
                in("_id", projectId)
            ),
            project(
                Projections.fields(Projections.include("pinnedDeploymentId"))
            ),
            lookup("Deployment", "pinnedDeploymentId", "_id", "deployment"),
            unwind("$deployment"),
            lookup("FeedVersion", "deployment.feedVersionIds", "_id", "feedVersions"),
            unwind("$feedVersions"),
            replaceRoot("$feedVersions"),
            project(
                Projections.fields(Projections.include(
                    "feedSourceId",
                    "validationResult.firstCalendarDate",
                    "validationResult.lastCalendarDate",
                    "validationResult.errorCount")
                )
            )
        );
        return extractFeedVersionSummaries(
            "Project",
            "_id",
            "feedSourceId",
            true,
            stages
        );
    }


    /**
     * Produce a list of all feed source summaries for a project.
     */
    private static List<FeedSourceSummary> extractFeedSourceSummaries(
        String projectId,
        String organizationId,
        List<Bson> stages
    ) {
        List<FeedSourceSummary> feedSourceSummaries = new ArrayList<>();
        for (Document feedSourceDocument : Persistence.getDocuments("FeedSource", stages)) {
            feedSourceSummaries.add(new FeedSourceSummary(projectId, organizationId, feedSourceDocument));
        }
        return feedSourceSummaries;
    }

    /**
     * Extract feed version summaries from feed version documents. Each feed version is held against the matching feed
     * source.
     */
    private static Map<String, FeedVersionSummary> extractFeedVersionSummaries(
        String collection,
        String feedVersionKey,
        String feedSourceKey,
        boolean hasChildValidationResultDocument,
        List<Bson> stages
    ) {
        Map<String, FeedVersionSummary> feedVersionSummaries = new HashMap<>();
        AggregateIterable<Document> feedVersions = Persistence.getDocuments(collection, stages);
        for (Document feedVersion : feedVersions) {
            FeedVersionSummary feedVersionSummary = new FeedVersionSummary(feedVersionKey, hasChildValidationResultDocument, feedVersion);
            feedVersionSummaries.put(feedVersion.getString(feedSourceKey), feedVersionSummary);
        }
        return feedVersionSummaries;
    }

    /**
     * Convert Date object into LocalDate object.
     */
    private static LocalDate getLocalDateFromDate(Date date) {
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }

    public static class LatestValidationResult {

        public String feedVersionId;
        @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
        @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
        public LocalDate startDate;

        @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
        @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
        public LocalDate endDate;

        public Integer errorCount;

        /**
         * Required for JSON de/serializing.
         **/
        public LatestValidationResult() {
        }

        LatestValidationResult(FeedVersionSummary feedVersionSummary) {
            this.feedVersionId = feedVersionSummary.id;
            this.startDate = feedVersionSummary.validationResult.firstCalendarDate;
            this.endDate = feedVersionSummary.validationResult.lastCalendarDate;
            this.errorCount = (feedVersionSummary.validationResult.errorCount == -1)
                ? null
                : feedVersionSummary.validationResult.errorCount;
        }
    }

}