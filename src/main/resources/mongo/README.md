To run the scripts in this folder, follow these steps:

1) Comment out the call to tearDownDeployedFeedVersion() in FeedSourceControllerTest -> tearDown().
2) Run FeedSourceControllerTest to created required objects referenced here.
3) Once complete, delete documents via MongoDB.
4) Uncomment the call to tearDownDeployedFeedVersion() in FeedSourceControllerTest -> tearDown().
5) Re-run FeedSourceControllerTest to confirm deletion of objects.