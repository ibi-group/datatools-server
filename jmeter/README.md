# datatools-jmeter-test

A repo to aid with running jmeter load tests on datatools-server.

## Installation

Install jmeter with this nifty script:

```sh
./install-jmeter.sh
```

## Running

Run the jmeter gui (and load the test script)

```sh
./run-gui.sh
```

Run the tests without the gui (and write result and report files)


```sh
./run-tests.sh
```

## Test Plans

### Data Loading Test Script Steps

1.  Create Project
1.  Create Feedsource
1.  Upload zip to create new Feed Version
1.  Loop until job to upload feed is complete (making http requests to job status)

### API Integrity Test Script Steps

1.  Fetch all routes
1.  Pick a random route
1.  Fetch all trips on selected route
1.  Check that all trips have same route_id as route
1.  Fetch all patterns on selected route
1.  Check that all patterns have same route_id
1.  Fetch embedded stop_times from trips from a random pattern
1.  Check that all stop_times have proper trip_id
1.  Check that all stop_times in trips on pattern have same stop sequence as pattern
