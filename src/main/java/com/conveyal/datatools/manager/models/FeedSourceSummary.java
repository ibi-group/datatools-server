package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.editor.utils.JacksonSerializers;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.extensions.ExternalPropertiesRetriever;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Lists;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UnwindOptions;
import com.mongodb.client.model.Variable;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;
import static com.conveyal.datatools.manager.DataManager.hasConfigProperty;
import static com.conveyal.datatools.manager.DataManager.isExtensionEnabled;
import static com.conveyal.datatools.manager.DataManager.isModuleEnabled;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.replaceRoot;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.descending;
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
     * Update the publish and validation state based on the provided feed version summary.
     */
    public void updatePublishAndValidationState(FeedVersionSummary feedVersionSummary) {
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
                    "noteIds"
                )
            ),
            sort(Sorts.ascending("name"))
        );

        return extractFeedSourceSummaries(projectId, organizationId, stages);
    }

    /**
     * Get the latest feed version from all feed sources for this project. For equivalent Mongo query, see
     * <a href="src/main/resources/mongo/getLatestFeedVersionForFeedSources.js">getLatestFeedVersionForFeedSources.js</a>.
     * If this is updated, be sure to also update the matching Mongo query.
     */
    public static Map<String, FeedVersionSummary> getLatestFeedVersionForFeedSources(String projectId) {
        List<Bson> feedVersionPipeline = Arrays.asList(
            // Match FeedVersion documents where feedSourceId equals the feedSourceId passed from the outer document.
            match(
                expr(
                    new Document("$eq", Arrays.asList("$feedSourceId", "$$feedSourceId"))
                )
            ),
            sort(descending("version")),
            limit(1),
            // Project only the fields needed from the FeedVersion to reduce payload size.
            project(
                include(
                    "version",
                    "_id",
                    "validationResult",
                    "processedByExternalPublisher",
                    "sentToExternalPublisher",
                    "gtfsPlusValidation",
                    "namespace"
                )
            )
        );

        // Define the variable passed into the lookup pipeline.
        List<Variable<String>> feedSourceId = List.of(new Variable<>("feedSourceId", "$_id"));

        // $lookup that uses the above pipeline to produce "latestFeedVersion" (an array with at most one element).
        Bson lookupLatestFeedVersion = lookup(
            "FeedVersion",
            feedSourceId,
            feedVersionPipeline,
            "latestFeedVersion"
        );

        // Pipeline to find the published FeedVersion by namespace (or identifier stored in publishedVersionId)
        List<Bson> publishedFeedVersionPipeline = Arrays.asList(
            // Match FeedVersion documents where namespace equals the outer document's publishedVersionId.
            match(
                expr(
                    new Document("$eq", Arrays.asList("$namespace", "$$publishedVersionId"))
                )
            ),
            limit(1),
            // Project only the validationResult because that's all that is needed later.
            project(include("validationResult"))
        );

        // Pass publishedVersionId from the local document into the lookup pipeline.
        List<Variable<String>> publishedVersionId = List.of(new Variable<>("publishedVersionId", "$publishedVersionId"));

        // $lookup that uses the above pipeline to produce "publishedFeedVersion" (an array with at most one element).
        Bson lookupPublishedFeedVersion = lookup(
            "FeedVersion",
            publishedVersionId,
            publishedFeedVersionPipeline,
            "publishedFeedVersion"
        );

        // Top-level aggregation stages that combine the lookups and map required fields into a slimmed down result.
        List<Bson> stages = Arrays.asList(
            // Start by filtering documents by projectId (reduces the number of input documents early).
            match(in("projectId", projectId)),

            // Attach the latest FeedVersion (as an array "latestFeedVersion").
            lookupLatestFeedVersion,

            // Attach the published FeedVersion (as an array "publishedFeedVersion").
            lookupPublishedFeedVersion,

            // Unwind the latestFeedVersion array into a single document.
            unwind("$latestFeedVersion", new UnwindOptions().preserveNullAndEmptyArrays(true)),

            // Unwind the publishedFeedVersion array into a single document.
            unwind("$publishedFeedVersion", new UnwindOptions().preserveNullAndEmptyArrays(true)),

            // Final projection: select and compute only the fields needed for the output to minimize size.
            project(fields(
                // keep the raw publishedVersionId field for reference.
                include("publishedVersionId"),

                // Published feed version fields (mapped from the nested publishedFeedVersion.validationResult).
                computed("publishedFeedVersionErrorCount", "$publishedFeedVersion.validationResult.errorCount"),
                computed("publishedFeedVersionStartDate", "$publishedFeedVersion.validationResult.firstCalendarDate"),
                computed("publishedFeedVersionEndDate", "$publishedFeedVersion.validationResult.lastCalendarDate"),

                // Latest feed version fields (mapped from the nested latestFeedVersion).
                computed("feedVersionId", "$latestFeedVersion._id"),
                computed("firstCalendarDate", "$latestFeedVersion.validationResult.firstCalendarDate"),
                computed("lastCalendarDate", "$latestFeedVersion.validationResult.lastCalendarDate"),
                computed("errorCount", "$latestFeedVersion.validationResult.errorCount"),
                computed("processedByExternalPublisher", "$latestFeedVersion.processedByExternalPublisher"),
                computed("sentToExternalPublisher", "$latestFeedVersion.sentToExternalPublisher"),
                computed("gtfsPlusValidation", "$latestFeedVersion.gtfsPlusValidation"),
                computed("namespace", "$latestFeedVersion.namespace")
            ))
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
     * Get the deployed feed versions from the latest deployment for this project. For equivalent Mongo query, see
     * <a href="src/main/resources/mongo/getFeedVersionsFromLatestDeployment.js">getFeedVersionsFromLatestDeployment.js</a>.
     * If this is updated, be sure to also update the matching Mongo query.
     */
    public static Map<String, FeedVersionSummary> getFeedVersionsFromLatestDeployment(String projectId) {
        List<Bson> stages = new ArrayList<>();
        stages.add(match(in("_id", projectId)));

        // Lookup Deployments for the project.
        stages.add(lookup(
            "Deployment",
            "_id",
            "projectId",
            "deployments"
        ));

        // Unwind deployments array to get individual deployment documents.
        stages.add(unwind("$deployments"));

        // Project only fields needed from deployment to reduce doc size before sorting.
        stages.add(project(fields(
            computed("deployment", "$deployments._id"),
            computed("lastUpdated", "$deployments.lastUpdated"),
            computed("feedVersionIds", "$deployments.feedVersionIds")
        )));

        // Sort deployments by lastUpdated descending.
        stages.add(sort(descending("lastUpdated")));
        stages.add(limit(1));

        List<Bson> feedVersionPipeline = Arrays.asList(
            match(expr(new Document("$in", Arrays.asList("$_id", "$$feedVersionIds")))),
            project(
                include(
                    "feedSourceId",
                    "validationResult.firstCalendarDate",
                    "validationResult.lastCalendarDate",
                    "validationResult.errorCount"
                )
            )
        );

        // Use pipeline form of lookup to fetch FeedVersions matching deployment’s feedVersionIds
        List<Variable<String>> feedVersionIds = List.of(new Variable<>("feedVersionIds", "$feedVersionIds"));

        stages.add(lookup(
            "FeedVersion",
            feedVersionIds,
            feedVersionPipeline,
            "feedVersions"
        ));
        stages.add(unwind("$feedVersions", new UnwindOptions().preserveNullAndEmptyArrays(false)));
        stages.add(replaceRoot("$feedVersions"));
        // Final projection: select and compute only the fields needed for the output to minimize size.
        stages.add(project(
            include(
                "_id",
                "feedSourceId",
                "validationResult"
            )
        ));

        return extractFeedVersionSummaries(
            "Project",
            "_id",
            "feedSourceId",
            true,
            stages
        );
    }

    /**
     * Get the deployed feed version from the pinned deployment for this feed source. For equivalent Mongo query, see
     * <a href="src/main/resources/mongo/getFeedVersionsFromPinnedDeployment.js">getFeedVersionsFromPinnedDeployment.js</a>.
     */
    public static Map<String, FeedVersionSummary> getFeedVersionsFromPinnedDeployment(String projectId) {
        List<Bson> stages = new ArrayList<>();

        // Match projects by projectId.
        stages.add(match(in("_id", projectId)));

        // Project only pinnedDeploymentId to keep doc small.
        stages.add(project(include("pinnedDeploymentId")));

        // Lookup Deployment documents by pinnedDeploymentId.
        stages.add(lookup("Deployment", "pinnedDeploymentId", "_id", "deployment"));

        // Unwind deployment array (assuming single deployment per project).
        stages.add(unwind("$deployment"));

        // Define pipeline in $lookup to filter and project FeedVersion docs.
        List<Bson> feedVersionPipeline = Arrays.asList(
            match(
                expr(
                    new Document("$in", Arrays.asList("$_id", "$$feedVersionIds"))
                )
            ),
            project(
                include(
                    "_id",
                    "feedSourceId",
                    "validationResult.firstCalendarDate",
                    "validationResult.lastCalendarDate",
                    "validationResult.errorCount"
                )
            )
        );

        // Define variable for correlated lookup on FeedVersion collection.
        List<Variable<String>> feedVersionIds = List.of(
            new Variable<>("feedVersionIds", "$deployment.feedVersionIds")
        );

        // Lookup FeedVersion docs with pipeline and store as feedVersions array.
        stages.add(lookup("FeedVersion", feedVersionIds, feedVersionPipeline, "feedVersions"));

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
        for (Document feedVersion : Persistence.getDocuments(collection, stages)) {
            feedVersionSummaries.put(
                feedVersion.getString(feedSourceKey),
                new FeedVersionSummary(feedVersionKey, hasChildValidationResultDocument, feedVersion)
            );
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