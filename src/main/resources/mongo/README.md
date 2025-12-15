## Running the Scripts in this folder using test data from FeedSourceControllerTest

1. **Comment out** the call to `tearDownDeployedFeedVersion()` in `FeedSourceControllerTest` \-> `tearDown()`.
2. **Run** `FeedSourceControllerTest` to create required objects referenced here.
3. **Delete** documents via MongoDB once complete.
4. **Uncomment** the call to `tearDownDeployedFeedVersion()` in `FeedSourceControllerTest` \-> `tearDown()`.
5. **Re-run** `FeedSourceControllerTest` to confirm deletion of objects.

## Alternative Approach

If the appropriate data has already been created (e.g., via the DT UI), the `<projectId>` tag in each script can be replaced with the actual projectId value.