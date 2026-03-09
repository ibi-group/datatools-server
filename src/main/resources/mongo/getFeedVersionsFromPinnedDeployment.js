db.getCollection('Project').aggregate([
    { $match: { _id: "<projectId>" } },
    { $project: { pinnedDeploymentId: 1 } },
    {
        $lookup: {
            from: "Deployment",
            localField: "pinnedDeploymentId",
            foreignField: "_id",
            as: "deployment"
        }
    },
    { $unwind: "$deployment" },
    {
        $lookup: {
            from: "FeedVersion",
            let: { feedVersionIds: "$deployment.feedVersionIds" },
            pipeline: [
                { $match: { $expr: { $in: ["$_id", "$$feedVersionIds"] } } },
                { $project: {
                        _id: 1,
                        feedSourceId: 1,
                        "validationResult.firstCalendarDate": 1,
                        "validationResult.lastCalendarDate": 1,
                        "validationResult.errorCount": 1
                    }}
            ],
            as: "feedVersions"
        }
    },
    { $unwind: "$feedVersions" },
    { $replaceRoot: { newRoot: "$feedVersions" } }
])