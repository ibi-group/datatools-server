package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.editor.utils.JacksonSerializers;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Lists;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.replaceRoot;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.in;

public class FeedVersionDeployed {
    public String id;

    @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
    @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
    public LocalDate startDate;

    @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
    @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
    public LocalDate endDate;

    public FeedVersionDeployed() {
    }

    public FeedVersionDeployed(Document feedVersionDocument) {
        this.id = feedVersionDocument.getString("_id");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        Document validationResult = (Document) feedVersionDocument.get("validationResult");
        if (validationResult != null) {
            String first = validationResult.getString("firstCalendarDate");
            String last = validationResult.getString("lastCalendarDate");
            this.startDate = (first == null) ? null : LocalDate.parse(first, formatter);
            this.endDate = (last == null) ? null : LocalDate.parse(last, formatter);
        }
    }

    /**
     * Get the deployed feed version from the pinned deployment for this feed source.
     */
    public static FeedVersionDeployed getFeedVersionFromPinnedDeployment(String projectId, String feedSourceId) {
        /*
            Note: To test this script:
                1) Comment out the call to tearDownDeployedFeedVersion() in FeedSourceControllerTest -> tearDown().
                2) Run FeedSourceControllerTest to created required objects referenced here.
                3) Once complete, delete documents via MongoDB.
                4) Uncomment the call to tearDownDeployedFeedVersion() in FeedSourceControllerTest -> tearDown().
                5) Re-run FeedSourceControllerTest to confirm deletion of objects.

            db.getCollection('Project').aggregate([
            {
                $match: {
                    _id: "project-with-pinned-deployment"
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
                $lookup:{
                    from:"FeedVersion",
                    localField:"deployment.feedVersionIds",
                    foreignField:"_id",
                    as:"feedVersions"
                }
            },
            {
                $unwind: "$feedVersions"
            },
            {
                "$replaceRoot": {
                    "newRoot": "$feedVersions"
                }
            },
            {
                $match: {
                    feedSourceId: "feed-source-with-pinned-deployment-feed-version"
                }
            },
            {
                $sort: {
                    lastUpdated : -1
                }
            },
            {
               $limit: 1
            }
            ])
         */
        List<Bson> stages = Lists.newArrayList(
            match(
                in("_id", projectId)
            ),
            lookup("Deployment", "pinnedDeploymentId", "_id", "deployment"),
            lookup("FeedVersion", "deployment.feedVersionIds", "_id", "feedVersions"),
            unwind("$feedVersions"),
            replaceRoot("$feedVersions"),
            match(
                in("feedSourceId", feedSourceId)
            ),
            // If more than one feed version for a feed source is held against a deployment the latest is used.
            sort(Sorts.descending("lastUpdated")),
            limit(1)
        );
        return getFeedVersionDeployed(stages);
    }

    /**
     * Get the deployed feed version from the latest deployment for this feed source.
     */
    public static FeedVersionDeployed getFeedVersionFromLatestDeployment(String projectId, String feedSourceId) {
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
                        as:"deployments"
                    }
                },
                {
                    // Deconstruct deployments array to a document for each element.
                    $unwind: "$deployments"
                },
                {
                    // Make the deployment documents the input/root document.
                    "$replaceRoot": {
                        "newRoot": "$deployments"
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
                    // Get all feed versions that have been deployed as part of the latest deployment.
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
                    // Match the required feed source.
                    $match: {
                        feedSourceId: "feed-source-with-latest-deployment-feed-version"
                    }
                },
                {
                    $sort: {
                        lastUpdated : -1
                    }
                },
                {
                    // At this point we will have the latest feed version from the latest deployment for a feed source.
                   $limit: 1
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
            match(
                in("feedSourceId", feedSourceId)
            ),
            // If more than one feed version for a feed source is held against a deployment the latest is used.
            sort(Sorts.descending("lastUpdated")),
            limit(1)
        );
        return getFeedVersionDeployed(stages);
    }

    private static FeedVersionDeployed getFeedVersionDeployed(List<Bson> stages) {
        Document feedVersionDocument = Persistence
            .getMongoDatabase()
            .getCollection("Project")
            .aggregate(stages)
            .first();
        return (feedVersionDocument == null) ? null : new FeedVersionDeployed(feedVersionDocument);
    }
}
