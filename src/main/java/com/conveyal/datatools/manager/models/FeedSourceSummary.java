package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.editor.utils.JacksonSerializers;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.extensions.ExternalPropertiesRetriever;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Lists;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;
import static com.conveyal.datatools.manager.DataManager.hasConfigProperty;
import static com.conveyal.datatools.manager.DataManager.isExtensionEnabled;
import static com.conveyal.datatools.manager.DataManager.isModuleEnabled;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.include;
import static java.util.Objects.requireNonNullElse;

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

    public Date latestSentToExternalPublisher;

    public FeedValidationResultSummary publishedValidationSummary;

    public String publishedVersionId;

    public PublishState publishState;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Map<String, Map<String, String>> externalProperties;

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
        publishedVersionId = feedSourceDocument.getString("publishedVersionId");
        // Get optional filename.
        filename = feedSourceDocument.getString("filename");
        // Optional external properties, if enabled by config.
        if (
            isModuleEnabled("gtfsapi") &&
            hasConfigProperty("modules.gtfsapi.use_extension") &&
            isExtensionEnabled(getConfigPropertyAsText("modules.gtfsapi.use_extension"))
        ) {
            externalProperties = ExternalPropertiesRetriever.retrieveFeedSourceExternalProperties(id);
        }
    }

    /**
     * Assign deployed feed version. Prioritise pinned deployment feed version over latest deployment deployed feed version.
     */
    private static void assignDeployedVersion(String projectId, List<FeedSourceSummary> feedSourceSummaries) {
        Map<String, FeedVersionSummary> latestFeedVersionForFeedSources = FeedVersionSummary.getLatestFeedVersionForFeedSources(projectId);
        Map<String, FeedVersionSummary> pinnedDeploymentFeedVersions = FeedVersionSummary.getFeedVersionsFromPinnedDeployment(projectId);
        Map<String, FeedVersionSummary> latestDeploymentDeployedFeedVersions = FeedVersionSummary.getFeedVersionsFromLatestDeployment(projectId);

        feedSourceSummaries.forEach(feedSourceSummary -> {
            feedSourceSummary.updatePublishAndValidationState(latestFeedVersionForFeedSources.get(feedSourceSummary.id));
            FeedVersionSummary deployedVersion = pinnedDeploymentFeedVersions.getOrDefault(
                feedSourceSummary.id,
                latestDeploymentDeployedFeedVersions.get(feedSourceSummary.id)
            );
            feedSourceSummary.setDeployedFeedVersionValues(deployedVersion);
        });
    }

    /**
     * Update the publish and validation state based on the provided feed version summary.
     */
    private void updatePublishAndValidationState(FeedVersionSummary feedVersionSummary) {
        if (feedVersionSummary == null) {
            return;
        }
        latestSentToExternalPublisher = feedVersionSummary.sentToExternalPublisher;
        publishedValidationSummary = new FeedValidationResultSummary();
        publishedValidationSummary.errorCount = requireNonNullElse(feedVersionSummary.publishedFeedVersionErrorCount, -1);
        publishedValidationSummary.startDate = feedVersionSummary.publishedFeedVersionStartDate;
        publishedValidationSummary.endDate = feedVersionSummary.publishedFeedVersionEndDate;
        publishState = feedVersionSummary.getPublishState();
        latestValidation = new LatestValidationResult(feedVersionSummary);
    }

    /**
     * Set the deployed feed version values. For consistency, if no error count is available set the related number of
     * issues to zero.
     */
    private void setDeployedFeedVersionValues(FeedVersionSummary feedVersionSummary) {
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
     * Get all feed source summaries matching the project id. For equivalent Mongo query, see
     * <a href="src/main/resources/mongo/getFeedSourceSummaries.js">getFeedSourceSummaries.js</a>.
     * For equivalent Mongo query, @see src/main/resources/mongo/getFeedSourceSummaries.js.
     * If this is updated, be sure to also update the matching Mongo query.
     */
    public static List<FeedSourceSummary> getFeedSourceSummaries(String projectId, String organizationId) {
        List<Bson> stages = Lists.newArrayList(
            match(
                in("projectId", projectId)
            ),

            // Project only necessary fields early to reduce document size.
            project(
                include(
                    "_id",
                    "name",
                    "deployable",
                    "isPublic",
                    "lastUpdated",
                    "labelIds",
                    "url",
                    "filename",
                    "noteIds",
                    "publishedVersionId"
                )
            ),
            sort(Sorts.ascending("name"))
        );

        List<FeedSourceSummary> feedSourceSummaries = extractFeedSourceSummaries(projectId, organizationId, stages);
        FeedSourceSummary.assignDeployedVersion(projectId, feedSourceSummaries);
        return feedSourceSummaries;
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