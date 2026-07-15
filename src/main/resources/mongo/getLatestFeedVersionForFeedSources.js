db.FeedSource.aggregate([
    { $match: { projectId: "<projectId>" } },
    {
        $lookup: {
            from: "FeedVersion",
            let: { feedSourceId: "$_id" },
            pipeline: [
                { $match: { $expr: { $eq: ["$feedSourceId", "$$feedSourceId"] } } },
                { $sort: { version: -1 } },
                { $limit: 1 },
                {
                    $project: {
                        version: 1,
                        validationResult: 1,
                        processedByExternalPublisher: 1,
                        sentToExternalPublisher: 1,
                        gtfsPlusValidation: 1,
                        namespace: 1
                    }
                }
            ],
            as: "latestFeedVersion"
        }
    },
    {
        $lookup: {
            from: "FeedVersion",
            let: { feedSourceId: "$_id", publishedVersionId: "$publishedVersionId" },
            pipeline: [
                // Filtering by feedSourceId helps by 100x when dealing with large number of feed versions.
                { $match: { $expr: { $eq: ["$feedSourceId", "$$feedSourceId"] } } },
                { $match: { $expr: { $eq: ["$namespace", "$$publishedVersionId"] } } },
                { $limit: 1 },
                { $project: { validationResult: 1 } }
            ],
            as: "publishedFeedVersion"
        }
    },
    { $unwind: { path: "$latestFeedVersion", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$publishedFeedVersion", preserveNullAndEmptyArrays: true } },
    {
        $project: {
            publishedVersionId: 1,
            publishedFeedVersionErrorCount: "$publishedFeedVersion.validationResult.errorCount",
            publishedFeedVersionStartDate: "$publishedFeedVersion.validationResult.firstCalendarDate",
            publishedFeedVersionEndDate: "$publishedFeedVersion.validationResult.lastCalendarDate",
            version: "$latestFeedVersion.version",
            feedVersionId: "$latestFeedVersion._id",
            firstCalendarDate: "$latestFeedVersion.validationResult.firstCalendarDate",
            lastCalendarDate: "$latestFeedVersion.validationResult.lastCalendarDate",
            errorCount: "$latestFeedVersion.validationResult.errorCount",
            processedByExternalPublisher: "$latestFeedVersion.processedByExternalPublisher",
            sentToExternalPublisher: "$latestFeedVersion.sentToExternalPublisher",
            gtfsPlusValidation: "$latestFeedVersion.gtfsPlusValidation",
            namespace: "$latestFeedVersion.namespace"
        }
    }
])