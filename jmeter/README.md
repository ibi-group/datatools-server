# datatools-server jmeter tests

This folder contains various items that to run jmeter load tests on datatools-server.

## Installation

Install jmeter with this nifty script:

```sh
./install-jmeter.sh
```

## Running

The jmeter test plan can be ran from the jmeter GUI or it can be ran without a GUI.  In each of these cases, it is assumed that a datatools-server instance can be queried at http://localhost:4000.

### Starting jmeter GUI

This script starts the jmeter gui and loads the test script.

```sh
./run-gui.sh
```

### Running test plan without GUI

The test plan can be ran straight from the command line.  A helper script is provided to assist in running jmeter from the command line.  This script has 3 required and 1 optional positional arguments:

| # | argument | possible values | description |
| ---- | ---- | ---- | ---- |
| 1 | test plan mode | `batch`, `fetch`, `query` or `upload` | which test plan mode to use when running the jmeter script. (see notes below for more explanation of these test plan modes) |
| 2 | number of threads | an integer greater than 0 | The number of simultaneous threads to run at a time.  The threads will have staggered start times 1 second apart. |
| 3 | number of loops | an integer greater than 0 | the number of loops to run.  This is combined with the number of threads, so if the number of threads is 10 and the number of loops is 8, the total number of test plans to run will be 80. |
| 4 | project name or batch csv file | string of the project name or string of file path to batch csv file | This argument is required if running the script with the `batch` test plan mode, otherwise, this argument is optional.<br><br>If in `fetch` or `upload` mode, the jmeter script will create new projects with a the provided project name (or "test project" if a name is not provided) plus the current iteration number.  In `fetch` or `upload` mode, the feed url and upload file is not configurable.  In `fetch` mode, the url `http://documents.atlantaregional.com/transitdata/gtfs_ASC.zip` will be used to fetch the feed to create the feed version.  In `upload` mode, the file `fixtures/gtfs.zip` will be uploaded to create the feed version.<br><br>If in `query` mode, jmeter will try to find the project matching the provided name (as long as the project name is not "test project") or a random project will be picked if this argument is not provided.  |
| 5 | s3 bucket | string of an s3 bucket | OPTIONAL.  If provided, the script will tar up the output folder and attempt to upload to the specified s3 bucket.  This assumes that aws credentials have been setup for use by the `aws` command line tool.  If not running in batch mode and a project name has been specified, the name of this file will be `{project name}.tar.gz`.  Otherwise, the name will be `output.tar.gz`.  |

Examples:

_Run the test plan in upload mode 1 total times in 1 thread running 1 loop._
```sh
./run-tests.sh upload 1 1
```

_Run the test plan in query mode 80 total times in 10 threads each completing 8 loops._
```sh
./run-tests.sh query 10 8 my-project-name my-s3-bucket
```

_Run in batch mode.  Note that all feeds in the csv file will be processed in each loop.  So in the following command, each feed in the batch.csv file would be processed 6 times.  See the section below for documentation on the csv file and also see the fixtures folder for an example file._
```sh
./run-tests.sh batch 3 2 batch.csv my-s3-bucket
```

### Running the upload test on multiple gtfs files

As noted above, the jmeter script can be run in `batch` mode.  The provded csv file must contain the following headers and data:

| header | description |
| ---- | ---- |
| project name | name of project to be created |
| mode | Must be either `fetch` or `upload` |
| location | The path to the file if the mode is `upload` or the http address if the mode is `fetch` |

There is also a helper python script that can be used to run the jmeter script in `batch` mode using all files stored within an s3 bucket.  This script requires that aws credentials have been setup for use by the aws command line tool.

| # | argument | possible values | description |
| ---- | ---- | ---- | ---- |
| 1 | test plan mode | `fetch` or `upload` | The test plan mode to use.  This will be written to each row of the csv file described above. |
| 2 | s3 bucket of gtfs feeds  | the string of an s3 bucket | An s3 bucket that is accessbile with the credentials setup for the aws cli.  Place zip files within the bucket.  Each zip file will be downloaded to the local machine and the jmeter test plan will be ran in upload mode for each gtfs zip file. |
| 3 | s3 bucket for output reports | the string of an s3 bucket | OPTIONAL. After each test run, the script will tar up the output folder and attempt to upload to the specified s3 bucket.  |

Example:

```sh
python run-upload-tests.py fetch gtfs-test-feeds datatools-jmeter-results
```


## Test Plan

A single test plan file is used for maintainablility.  By default, the test plan runs 1 thread in 1 loop and will upload a feed and then perform various checks on the uploaded feed version.  As noted in the above section, it is possible to run different variations of the test plan.  There are 4 types of test plans that can be initiated: `batch`, `fetch`, `query` or `upload`.

### Batch Test Plan Mode Script Steps

When the test plan is run in batch mode, a csv file must be provided that contains rows of test plans of either `fetch` or `upload` types.  Each row is then ran the with specified number of threads and loops.

1.  For Each Row: Run either the `fetch` or `upload` test plan according to the configuration in the row.

### Upload Test Plan Mode Script Steps

This section is run under the `upload` test plan mode or for a feed marked for uploading in the batch csv file.

1.  Create Project
1.  Create Feedsource
1.  Upload zip to create new Feed Version
1.  Loop until job to upload feed is complete (making http requests to job status)
1.  Save a record of the amount of time it took from the completion of the feed upload until receiving a status update that the feed version processing has completed
1.  Continue to API Integrity Script Steps

### Fetch Test Plan Mode Script Steps

This section is run under the `fetch` test plan mode or for a feed marked for fetching in the batch csv file.

1.  Create Project
1.  Create Feedsource
1.  Create new Feed Version (which initiates a download of a feed from datatools-server)
1.  Loop until job to fetch and process the feed is complete (making http requests to job status)
1.  Save a record of the amount of time it took from the completion of the feed version creation request until receiving a status update that the feed version processing has completed
1.  Continue to API Integrity Script Steps

### Query Test Plan Mode Script Steps

This section is run under the `query` test plan mode.  This script assumes that each project has a feed source that has a valid feed version.

1.  Fetch all projects
1.  Pick a random project
1.  Fetch all feed sources from the selected project
1.  Pick a random feed source
1.  Fetch all feed versions from the selected feed source
1.  Pick a random feed version
1.  Continue to API Integrity Script Steps

### API Integrity Script Steps

This section is run in all test plan modes.

1.  Fetch stops and a row count of stops
1.  Make sure the number of stops matches the row count of stops
1.  Fetch all routes
1.  Pick a random route
1.  Fetch all trips on selected route
1.  Check that all trips have same route_id as route
1.  Fetch all patterns on selected route
1.  Check that all patterns have same route_id
1.  Fetch embedded stop_times from trips from a random pattern
1.  Check that all stop_times have proper trip_id
1.  Check that all stop_times in trips on pattern have same stop sequence as pattern
1.  Make a GraphQL request that contains a nested query of routes, patterns and stops
1.  Make sure that each route is present in the route within the list of patterns

## Reporting

If running this script in GUI mode, it is possible to see all results in real-time by viewing the various listeners at the end of the thread group.

When running the test plan from the command line in non-gui mode, reports will be saved to the `output` folder.  The outputs will contain a csv file of all requests made and an html report summarizing the results.  If the test plan mode was `batch`, `fetch` or `upload` than another csv file will be written that contains a list of the elapsed time for processing the creation of a new gtfs feed version.

The csv files can be loaded into a jmeter GUI to view more details.
