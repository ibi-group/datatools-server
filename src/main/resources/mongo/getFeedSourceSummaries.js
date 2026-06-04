db.getCollection('FeedSource').aggregate([
    {
        // Match provided project id.
        $match: {
            projectId: "<projectId>"
        }
    },
    {
        // FeedSource fields to include
        $project: {
            "_id": 1,
            "name": 1,
            "deployable": 1,
            "isPublic": 1,
            "lastUpdated": 1,
            "labelIds": 1,
            "url": 1,
            "filename": 1,
            "noteIds": 1,
            "publishedVersionId": 1
        }
    },
    {
        $sort: {
            "name": 1
        }
    }
])
