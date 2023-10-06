package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.editor.utils.JacksonSerializers;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.validator.ValidationResult;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Lists;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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

public class FeedSourceSummary {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

    public String projectId;

    public String id;

    public String name;
    public boolean deployable;
    public boolean isPublic;

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

    public FeedSourceSummary() {
    }

    public FeedSourceSummary(String projectId, Document feedSourceDocument) {
        this.projectId = projectId;
        this.id = feedSourceDocument.getString("_id");
        this.name = feedSourceDocument.getString("name");
        this.deployable = feedSourceDocument.getBoolean("deployable");
        this.isPublic = feedSourceDocument.getBoolean("isPublic");
        List<String> documentLabelIds = feedSourceDocument.getList("labelIds", String.class);
        if (documentLabelIds != null) {
            this.labelIds = documentLabelIds;
        }
        // Convert to local date type for consistency.
        this.lastUpdated = getLocalDateFromDate(feedSourceDocument.getDate("lastUpdated"));
        this.url = feedSourceDocument.getString("url");
    }

    /**
     * Set the appropriate feed version. For consistency, if no error count is available set the related number of
     * issues to null.
     */
    public void setFeedVersion(FeedVersionSummary feedVersionSummary, boolean isDeployed) {
        if (feedVersionSummary != null) {
            if (isDeployed) {
                this.deployedFeedVersionId = feedVersionSummary.id;
                this.deployedFeedVersionStartDate = feedVersionSummary.validationResult.firstCalendarDate;
                this.deployedFeedVersionEndDate = feedVersionSummary.validationResult.lastCalendarDate;
                this.deployedFeedVersionIssues = (feedVersionSummary.validationResult.errorCount == -1)
                    ? 0
                    : feedVersionSummary.validationResult.errorCount;
            } else {
                this.latestValidation = new LatestValidationResult(feedVersionSummary);
            }
        }
    }

    /**
     * Get all feed source summaries matching the project id.
     */
    public static List<FeedSourceSummary> getFeedSourceSummaries(String projectId) {
        /*
            db.getCollection('FeedSource').aggregate([
                {
                    // Match provided project id.
                    $match: {
                        projectId: "<projectId>"
                    }
                },
                {
                    $project: {
                        "_id": 1,
                        "name": 1,
                        "deployable": 1,
                        "isPublic": 1,
                        "lastUpdated": 1,
                        "labelIds": 1,
                        "url": 1
                    }
                },
                {
                    $sort: {
                        "name": 1
                    }
                }
            ])
         */
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
                    "url")
                )
            ),
            sort(Sorts.ascending("name"))
        );
        return extractFeedSourceSummaries(projectId, stages);
    }

    /**
     * Get the latest feed version from all feed sources for this project.
     */
    public static Map<String, FeedVersionSummary> getLatestFeedVersionForFeedSources(String projectId) {
        /*
            Note: To test this script:
                1) Comment out the call to tearDownDeployedFeedVersion() in FeedSourceControllerTest -> tearDown().
                2) Run FeedSourceControllerTest to created required objects referenced here.
                3) Once complete, delete documents via MongoDB.
                4) Uncomment the call to tearDownDeployedFeedVersion() in FeedSourceControllerTest -> tearDown().
                5) Re-run FeedSourceControllerTest to confirm deletion of objects.

            db.getCollection('FeedSource').aggregate([
                {
                    // Match provided project id.
                    $match: {
                        projectId: "project-with-latest-deployment"
                    }
                },
                {
                    $lookup: {
                        from: "FeedVersion",
                        localField: "_id",
                        foreignField: "feedSourceId",
                        as: "feedVersions"
                    }
                },
                {
                    $unwind: "$feedVersions"
                },
                {
                    $group: {
                        _id: "$_id",
                        doc: {
                            $max: {
                                version: "$feedVersions.version",
                                feedVersionId: "$feedVersions._id",
                                firstCalendarDate: "$feedVersions.validationResult.firstCalendarDate",
                                lastCalendarDate: "$feedVersions.validationResult.lastCalendarDate",
                                issues: "$feedVersions.validationResult.errorCount"
                            }
                        }
                    }
                }
            ])
        */
        List<Bson> stages = Lists.newArrayList(
            match(
                in("projectId", projectId)
            ),
            lookup("FeedVersion", "_id", "feedSourceId", "feedVersions"),
            unwind("$feedVersions"),
            group(
                "$_id",
                Accumulators.last("feedVersionId", "$feedVersions._id"),
                Accumulators.last("firstCalendarDate", "$feedVersions.validationResult.firstCalendarDate"),
                Accumulators.last("lastCalendarDate", "$feedVersions.validationResult.lastCalendarDate"),
                Accumulators.last("errorCount", "$feedVersions.validationResult.errorCount")
            )
        );
        return extractFeedVersionSummaries(
            "FeedSource",
            "feedVersionId",
            "_id",
            false,
            stages);
    }

    /**
     * Get the deployed feed versions from the latest deployment for this project.
     */
    public static Map<String, FeedVersionSummary> getFeedVersionsFromLatestDeployment(String projectId) {
        /*
            Note: To test this script:
                1) Comment out the call to tearDownDeployedFeedVersion() in FeedSourceControllerTest -> tearDown().
                2) Run FeedSourceControllerTest to created required objects referenced here.
                3) Once complete, delete documents via MongoDB.
                4) Uncomment the call to tearDownDeployedFeedVersion() in FeedSourceControllerTest -> tearDown().
                5) Re-run FeedSourceControllerTest to confirm deletion of objects.

            db.getCollection('Project').aggregate([
                {
                    // Match provided project id.
                    $match: {
                        _id: "project-with-latest-deployment"
                    }
                },
                {
                    // Get all deployments for this project.
                    $lookup:{
                        from:"Deployment",
                        localField:"_id",
                        foreignField:"projectId",
                        as:"deployment"
                    }
                },
                {
                    // Deconstruct deployments array to a document for each element.
                    $unwind: "$deployment"
                },
                {
                    // Make the deployment documents the input/root document.
                    "$replaceRoot": {
                        "newRoot": "$deployment"
                    }
                },
                {
                    // Sort descending.
                    $sort: {
                        lastUpdated : -1
                    }
                },
                {
                    // At this point we will have the latest deployment for a project.
                    $limit: 1
                },
                {
                    $lookup:{
                        from:"FeedVersion",
                        localField:"feedVersionIds",
                        foreignField:"_id",
                        as:"feedVersions"
                    }
                },
                {
                    // Deconstruct feedVersions array to a document for each element.
                    $unwind: "$feedVersions"
                },
                {
                    // Make the feed version documents the input/root document.
                    "$replaceRoot": {
                        "newRoot": "$feedVersions"
                    }
                },
                {
                    $project: {
                        "_id": 1,
                        "feedSourceId": 1,
                        "validationResult.firstCalendarDate": 1,
                        "validationResult.lastCalendarDate": 1,
                        "validationResult.errorCount": 1
                    }
                }
            ])
        */
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
            stages);
    }

    /**
     * Get the deployed feed version from the pinned deployment for this feed source.
     */
    public static Map<String, FeedVersionSummary> getFeedVersionsFromPinnedDeployment(String projectId) {
        /*
            Note: To test this script:
                1) Comment out the call to tearDownDeployedFeedVersion() in FeedSourceControllerTest -> tearDown().
                2) Run FeedSourceControllerTest to created required objects referenced here.
                3) Once complete, delete documents via MongoDB.
                4) Uncomment the call to tearDownDeployedFeedVersion() in FeedSourceControllerTest -> tearDown().
                5) Re-run FeedSourceControllerTest to confirm deletion of objects.

            db.getCollection('Project').aggregate([
                {
                    // Match provided project id.
                    $match: {
                        _id: "project-with-pinned-deployment"
                    }
                },
                {
                    $project: {
                        pinnedDeploymentId: 1
                    }
                },
                {
                    $lookup:{
                        from:"Deployment",
                        localField:"pinnedDeploymentId",
                        foreignField:"_id",
                        as:"deployment"
                    }
                },
                {
                    $unwind: "$deployment"
                },
                {
                    $lookup:{
                        from:"FeedVersion",
                        localField:"deployment.feedVersionIds",
                        foreignField:"_id",
                        as:"feedVersions"
                    }
                },
                {
                    // Deconstruct feedVersions array to a document for each element.
                    $unwind: "$feedVersions"
                },
                {
                    // Make the feed version documents the input/root document.
                    "$replaceRoot": {
                        "newRoot": "$feedVersions"
                    }
                },
                {
                    $project: {
                        "_id": 1,
                        "feedSourceId": 1,
                        "validationResult.firstCalendarDate": 1,
                        "validationResult.lastCalendarDate": 1,
                        "validationResult.errorCount": 1
                    }
                }
            ])
        */

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
            stages);
    }


    /**
     * Produce a list of all feed source summaries for a project.
     */
    private static List<FeedSourceSummary> extractFeedSourceSummaries(String projectId, List<Bson> stages) {
        List<FeedSourceSummary> feedSourceSummaries = new ArrayList<>();
        for (Document feedSourceDocument : Persistence.getDocuments("FeedSource", stages)) {
            feedSourceSummaries.add(new FeedSourceSummary(projectId, feedSourceDocument));
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
        for (Document feedVersionDocument : Persistence.getDocuments(collection, stages)) {
            FeedVersionSummary feedVersionSummary = new FeedVersionSummary();
            feedVersionSummary.id = feedVersionDocument.getString(feedVersionKey);
            feedVersionSummary.validationResult = getValidationResult(hasChildValidationResultDocument, feedVersionDocument);
            feedVersionSummaries.put(feedVersionDocument.getString(feedSourceKey), feedVersionSummary);
        }
        return feedVersionSummaries;
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
     * Extract child document matching provided name.
     */
    private static Document getDocumentChild(Document document, String name) {
        return (Document) document.get(name);
    }

    /**
     * Convert String date (if not null) into LocalDate.
     */
    private static LocalDate getDateFromString(String date) {
        return (date == null) ? null : LocalDate.parse(date, formatter);
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

        /** Required for JSON de/serializing. **/
        public LatestValidationResult() {}

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