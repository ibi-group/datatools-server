# datatools-jmeter-test

A repo to aid with running jmeter load tests on datatools-server.

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
| 1 | test plan mode | `upload` or `fetch` | whether or not to run the script in upload or fetch mode (see notes below for more explanation of these test plan modes) |
| 2 | number of threads | an integer greater than 0 | The number of simultaneous threads to run at a time.  The threads will be have staggered start times 1 second apart. |
| 3 | number of loops | an integer greater than 0 | the number of loops of to run.  This is combined with the number of threads, so if the number of threads is 10 and the number of loops is 8, the total number of test plans to run will be 80. |
| 4 | project name | string of the project name | OPTIONAL.  If provided, when the script is ran in upload mode, the script will create new projects with this name plus the current iteration number.  Ex: "project-name #".  Otherwise the project names will be called "test project #". |
| 5 | s3 bucket | string of an s3 bucket | OPTIONAL.  If provided, the script will tar up the output folder and attempt to upload to the specified s3 bucket.  This assumes that aws credentials have been setup for use by the `aws` command line tool. |

Examples:

```sh
./run-tests.sh upload 1 1
```

```sh
./run-tests.sh fetch 10 8 my-s3-bucket
```

## Test Plan

A single test plan file is used for maintainablility.  By default, the test plan runs 1 thread in 1 loop and will upload a feed and then perform various checks on the uploaded feed version.  As noted in the above section, it is possible to run a different variation of the test plan that will not upload a feed and instead select a random project and then a random feed source from the project and then a random feed version from the feed source and then perform the same checks on the feed version.

### Upload Test Plan Mode Script Steps

This section is ran under the `upload` test plan mode.

1.  Create Project
1.  Create Feedsource
1.  Upload zip to create new Feed Version
1.  Loop until job to upload feed is complete (making http requests to job status)
1.  Save a record of the amount of time it took from the completion of the feed upload until receiving a status update that the feed version processing has completed
1.  Continue to API Integrity Script Steps

### Fetch Test Plan Mode Script Steps

This section is ran under the `fetch` test plan mode.  This script assumes that each project has a feed source that has a valid feed version.

1.  Fetch all projects
1.  Pick a random project
1.  Fetch all feed sources from the selected project
1.  Pick a random feed source
1.  Fetch all feed versions from the selected feed source
1.  Pick a random feed version
1.  Continue to API Integrity Script Steps

### API Integrity Script Steps

This section is ran in both test plan modes.

1.  Fetch all routes
1.  Pick a random route
1.  Fetch all trips on selected route
1.  Check that all trips have same route_id as route
1.  Fetch all patterns on selected route
1.  Check that all patterns have same route_id
1.  Fetch embedded stop_times from trips from a random pattern
1.  Check that all stop_times have proper trip_id
1.  Check that all stop_times in trips on pattern have same stop sequence as pattern

## Reporting

If running this script in GUI mode, it is possible to see all results in real-time by viewing the various listeners at the end of the thread group.

When running the test plan from the command line in non-gui mode, reports will be saved to the `output` folder.  The outputs will contain a csv file of all requests made, an html report summarizing the results.  If the test plan mode was `upload` than another csv file will be written that contains a list of the elapsed time from completion of uploading a gtfs zip file until the job status for that job completed.

The csv files can be loaded into a jmeter GUI listener to view more details.
