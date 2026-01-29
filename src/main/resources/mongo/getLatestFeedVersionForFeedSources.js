db.getCollection('FeedSource').aggregate([
    {
        // Match provided project id.
        $match: {
            projectId: "<projectId>"
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
        $lookup: {
            from: "FeedVersion",
            localField: "publishedVersionId",
            foreignField: "namespace",
            as: "publishedFeedVersion"
        }
    },
    {
        $unwind: "$feedVersions"
    },
    {
        $unwind: {
            path: "$publishedFeedVersion",
            preserveNullAndEmptyArrays: true
        }
    },
    {
        $sort: {
            "feedVersions.version": -1
        }
    },
    {
        $group: {
            _id: "$_id",
            publishedVersionId: { $first: "$publishedVersionId" },
            publishedFeedVersionErrorCount: { $first: "$publishedFeedVersion.validationResult.errorCount"},
            publishedFeedVersionStartDate: { $first: "$publishedFeedVersion.validationResult.firstCalendarDate"},
            publishedFeedVersionEndDate: { $first: "$publishedFeedVersion.validationResult.lastCalendarDate"},
            feedVersion: {
                $first: {
                    version: "$feedVersions.version",
                    feedVersionId: "$feedVersions._id",
                    firstCalendarDate: "$feedVersions.validationResult.firstCalendarDate",
                    lastCalendarDate: "$feedVersions.validationResult.lastCalendarDate",
                    errorCount: "$feedVersions.validationResult.errorCount",
                    processedByExternalPublisher: "$feedVersions.processedByExternalPublisher",
                    sentToExternalPublisher: "$feedVersions.sentToExternalPublisher",
                    gtfsPlusValidation: "$feedVersions.gtfsPlusValidation",
                    namespace: "$feedVersions.namespace"
                }
            }
        }
    }
])
