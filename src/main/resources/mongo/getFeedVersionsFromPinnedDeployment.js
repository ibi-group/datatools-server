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
