#!/bin/sh

# clean up old output
rm -rf result
rm -rf report
mkdir result
mkdir report

# sanity check: make sure datatools server is running
if wget -q http://localhost:4000 > /dev/null; then
  # assume we're good to go
  apache-jmeter-3.3/bin/jmeter.sh -n -t test-script.jmx -l result/result.csv -e -o report
else
  # TODO: add some more notes about how it needs auth disabled?
  echo "No server running on port 4000.  Please start up datatools-server."
fi
