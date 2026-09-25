package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.editor.utils.JacksonSerializers;
import com.conveyal.datatools.manager.gtfsplus.GtfsPlusValidation;
import com.conveyal.datatools.manager.gtfsplus.ValidationIssue;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.validator.ValidationResult;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.mongodb.client.model.UnwindOptions;
import com.mongodb.client.model.Variable;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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

/**
 * Includes summary data (a subset of fields) for a feed version.
 */
public class FeedVersionSummary extends Model implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    public static Boolean hasBlockingIssueForPublishingForTesting = null;

    public FeedRetrievalMethod retrievalMethod;
    public int version;
    public String feedSourceId;
    public String name;
    public String namespace;
    public String originNamespace;
    public Long fileSize;
    public Date updated;
    /** Only a subset of the validation results are serialized to JSON via getValidationSummary. */
    @JsonIgnore
    public ValidationResult validationResult;
    private PartialValidationSummary validationSummary;
    public Date processedByExternalPublisher;
    public Date sentToExternalPublisher;
    public GtfsPlusValidation gtfsPlusValidation;
    public String feedSourcePublishedVersionId;

    public Integer publishedFeedVersionErrorCount;
    public LocalDate publishedFeedVersionStartDate;
    public LocalDate publishedFeedVersionEndDate;

    public PartialValidationSummary getValidationSummary() {
        if (validationSummary == null) {
            validationSummary = new PartialValidationSummary();
        }
        return validationSummary;
    }

    /** Empty constructor for serialization */
    public FeedVersionSummary() {
        // Do nothing
    }

    public FeedVersionSummary(
        String feedVersionKey,
        boolean hasChildValidationResultDocument,
        Document feedVersionDocument
    ) {
        id = feedVersionDocument.getString(feedVersionKey);
        processedByExternalPublisher = feedVersionDocument.getDate("processedByExternalPublisher");
        sentToExternalPublisher = feedVersionDocument.getDate("sentToExternalPublisher");
        gtfsPlusValidation = getGtfsPlusValidation(id, feedVersionDocument);
        namespace = feedVersionDocument.getString("namespace");
        validationResult = getValidationResult(hasChildValidationResultDocument, feedVersionDocument);

        // The feed source's published feed version. Feed source's publishedVersionId mapped to feed version's namespace.
        feedSourcePublishedVersionId = feedVersionDocument.getString("publishedVersionId");

        publishedFeedVersionErrorCount = feedVersionDocument.getInteger("publishedFeedVersionErrorCount");
        publishedFeedVersionStartDate = getDateFromString(feedVersionDocument.getString("publishedFeedVersionStartDate"));
        publishedFeedVersionEndDate = getDateFromString(feedVersionDocument.getString("publishedFeedVersionEndDate"));
    }

    /**
     * Holds a subset of fields from {@link:FeedValidationResultSummary} for UI use only.
     */
    public class PartialValidationSummary {
        /** Copied from FeedVersion */
        @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
        @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
        public LocalDate startDate;

        /** Copied from FeedVersion */
        @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
        @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
        public LocalDate endDate;

        PartialValidationSummary() {
            // Older feeds created in datatools may not have validationResult
            if (validationResult != null) {
                this.startDate = validationResult.firstCalendarDate;
                this.endDate = validationResult.lastCalendarDate;
            }
        }
    }

    /**
     * Build GtfsPlusValidation object from feed version document.
     */
    private static GtfsPlusValidation getGtfsPlusValidation(String feedVersionId, Document feedVersionDocument) {
        Document gtfsPlusValidationDocument = getDocumentChild(feedVersionDocument, "gtfsPlusValidation");
        if (gtfsPlusValidationDocument == null) {
            return null;
        }
        List<ValidationIssue> issues = null;
        if (gtfsPlusValidationDocument.get("issues") != null) {
            List<Document> issueDocs = gtfsPlusValidationDocument.getList("issues", Document.class);
            issues = issueDocs
                .stream()
                .map(doc -> mapper.convertValue(doc, ValidationIssue.class))
                .collect(Collectors.toList());
        }
        boolean published = Boolean.TRUE.equals(gtfsPlusValidationDocument.getBoolean("published"));
        return new GtfsPlusValidation(feedVersionId, published, issues);
    }

    /**
     * Build validation result from feed version document.
     */
    private static ValidationResult getValidationResult(boolean hasChildValidationResultDocument, Document feedVersionDocument) {
        ValidationResult validationResult = new ValidationResult();
        validationResult.errorCount = getValidationResultErrorCount(hasChildValidationResultDocument, feedVersionDocument);
        validationResult.firstCalendarDate = getValidationResultDate(hasChildValidationResultDocument, feedVersionDocument, "firstCalendarDate");
        validationResult.lastCalendarDate = getValidationResultDate(hasChildValidationResultDocument, feedVersionDocument, "lastCalendarDate");
        return validationResult;
    }

    /**
     * Convert String date (if not null) into LocalDate.
     */
    private static LocalDate getDateFromString(String date) {
        return (date == null) ? null : LocalDate.parse(date, formatter);
    }

    /**
     * Extract child document matching provided name.
     */
    private static Document getDocumentChild(Document document, String name) {
        return (Document) document.get(name);
    }

    /**
     * Extract date value from parent document or child validation result document.
     */
    private static LocalDate getValidationResultDate(
        boolean hasChildValidationResultDocument,
        Document feedVersionDocument,
        String key
    ) {
        return (hasChildValidationResultDocument)
            ? getDateFieldFromDocument(feedVersionDocument, key)
            : getDateFromString(feedVersionDocument.getString(key));
    }

    /**
     * Extract date value from validation result document.
     */
    private static LocalDate getDateFieldFromDocument(Document document, String dateKey) {
        Document validationResult = getDocumentChild(document, "validationResult");
        return (validationResult != null)
            ? getDateFromString(validationResult.getString(dateKey))
            : null;
    }

    /**
     * Extract the error count from the parent document or child validation result document. If the error count is not
     * available, return -1.
     */
    private static int getValidationResultErrorCount(boolean hasChildValidationResultDocument, Document feedVersionDocument) {
        int errorCount;
        try {
            errorCount = (hasChildValidationResultDocument)
                ? getErrorCount(feedVersionDocument)
                : feedVersionDocument.getInteger("errorCount");
        } catch (NullPointerException e) {
            errorCount = -1;
        }
        return errorCount;
    }

    /**
     * Get the child validation result document and extract the error count from this.
     */
    private static int getErrorCount(Document document) {
        return getDocumentChild(document, "validationResult").getInteger("errorCount");
    }

    /**
     * Determine the published state of the feed version.
     */
    public PublishState getPublishState() {
        if (isPublished()) {
            return PublishState.PUBLISHED;
        } else if (isPublishing()) {
            return PublishState.PUBLISHING;
        } else if (isPublishBlocked()) {
            return PublishState.PUBLISH_BLOCKED;
        }
        return PublishState.READY_TO_PUBLISH;
    }

    /**
     * Determine the published state of the feed version.
     */
    private boolean isPublished() {
        return namespace != null && namespace.equals(feedSourcePublishedVersionId);
    }

    /**
     * Deemed to be publishing if it has been sent to external publisher but not yet processed.
     */
    private boolean isPublishing() {
        return sentToExternalPublisher != null && processedByExternalPublisher == null;
    }

    /**
     * Determine if publishing is blocked due to validation, expiration, blocking issues or loading.
     */
    private boolean isPublishBlocked() {
        return
            gtfsPlusValidation == null ||
            gtfsPlusValidation.issues == null ||
            !gtfsPlusValidation.issues.isEmpty() ||
            !gtfsPlusValidation.published ||
            FeedVersion.hasExpired(validationResult) ||
            FeedVersion.isFuture(validationResult) ||
            hasBlockingIssuesForPublishing();
    }

    /**
     * Determine if there are blocking issues for publishing.
     */
    private boolean hasBlockingIssuesForPublishing() {
        return Objects.requireNonNullElseGet(hasBlockingIssueForPublishingForTesting, () -> FeedVersion.hasBlockingIssuesForPublishing(
            validationResult,
            namespace,
            name
        ));
    }

    public static void setHasBlockingIssueForPublishingOverrideForTesting(Boolean value) {
        hasBlockingIssueForPublishingForTesting = value;
    }


    /**
     * Get the latest feed version from all feed sources for this project. For equivalent Mongo query, see
     * <a href="src/main/resources/mongo/getLatestFeedVersionForFeedSources.js">getLatestFeedVersionForFeedSources.js</a>.
     * If this is updated, be sure to also update the matching Mongo query.
     */
    static Map<String, FeedVersionSummary> getLatestFeedVersionForFeedSources(String projectId) {
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
        Variable<String> feedSourceIdVariable = new Variable<>("feedSourceId", "$_id");
        List<Variable<String>> feedSourceId = List.of(feedSourceIdVariable);

        // $lookup that uses the above pipeline to produce "latestFeedVersion" (an array with at most one element).
        Bson lookupLatestFeedVersion = lookup(
            "FeedVersion",
            feedSourceId,
            feedVersionPipeline,
            "latestFeedVersion"
        );

        // Pipeline to find the published FeedVersion by namespace (or identifier stored in publishedVersionId)
        List<Bson> publishedFeedVersionPipeline = Arrays.asList(
            // Match FeedVersion documents where namespace equals the outer document's
            // feedSourceId (important when dealing with many FeedVersions)
            // and publishedVersionId.
            match(
                expr(
                    new Document("$eq", Arrays.asList("$feedSourceId", "$$feedSourceId"))
                )
            ),
            match(
                expr(
                    new Document("$eq", Arrays.asList("$namespace", "$$publishedVersionId"))
                )
            ),
            limit(1),
            // Project only the validationResult because that's all that is needed later.
            project(include("validationResult"))
        );

        // Pass feedSourceId and publishedVersionId from the local document into the lookup pipeline.
        List<Variable<String>> feedSourceIdAndPublishedVersionId = List.of(
            feedSourceIdVariable,
            new Variable<>("publishedVersionId", "$publishedVersionId")
        );

        // $lookup that uses the above pipeline to produce "publishedFeedVersion" (an array with at most one element).
        Bson lookupPublishedFeedVersion = lookup(
            "FeedVersion",
            feedSourceIdAndPublishedVersionId,
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
    static Map<String, FeedVersionSummary> getFeedVersionsFromLatestDeployment(String projectId) {
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
    static Map<String, FeedVersionSummary> getFeedVersionsFromPinnedDeployment(String projectId) {
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
}
