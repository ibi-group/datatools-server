// Query to get the latest version (version summary) sent for publishing (MTC-specific)
db.getCollection('FeedVersion').aggregate([
{
    // Only keep needed fields to reduce the memory footprint of the query.
    $project: {
        _id: 1,
        feedSourceId: 1,
        namespace: 1,
        sentToExternalPublisher: 1
    }
},
{
    $match: {
        sentToExternalPublisher: { $exists: 1 }
    }
},
{
    $group: {
        _id: "$feedSourceId",
        latestSentToExternalPublisher: { $max: "$sentToExternalPublisher" },
        items: { $push: "$$ROOT" }
    }
},
{
    $unwind: "$items"
},
{
    $match: {
        $expr: { $eq: ["$items.sentToExternalPublisher", "$latestSentToExternalPublisher"] }
    }
},
{
    "$replaceRoot": {
        "newRoot": "$items"
    }
}
])
