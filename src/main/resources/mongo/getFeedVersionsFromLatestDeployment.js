db.getCollection('Project').aggregate([
    { $match: { _id: "<projectId>" } },
    {
        $lookup: {
            from: "Deployment",
            localField: "_id",
            foreignField: "projectId",
            as: "deployment"
        }
    },
    { $unwind: "$deployment" },
    { $replaceRoot: { newRoot: "$deployment" } },
    { $sort: { lastUpdated: -1 } },
    { $limit: 1 },
    {
        $lookup: {
            from: "FeedVersion",
            let: { feedVersionIds: "$feedVersionIds" },
            pipeline: [
                {
                    $match: {
                        $expr: { $in: ["$_id", "$$feedVersionIds"] }
                    }
                },
                {
                    $project: {
                        _id: 1,
                        feedSourceId: 1,
                        "validationResult.firstCalendarDate": 1,
                        "validationResult.lastCalendarDate": 1,
                        "validationResult.errorCount": 1
                    }
                }
            ],
            as: "feedVersions"
        }
    },
    { $unwind: "$feedVersions" },
    { $replaceRoot: { newRoot: "$feedVersions" } },
    {
        $project: {
            _id: 1,
            feedSourceId: 1,
            "validationResult.firstCalendarDate": 1,
            "validationResult.lastCalendarDate": 1,
            "validationResult.errorCount": 1
        }
    }
]);