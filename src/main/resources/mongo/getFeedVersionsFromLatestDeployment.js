db.getCollection('Project').aggregate([
    {
        // Match provided project id.
        $match: {
            _id: "<projectId>"
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
