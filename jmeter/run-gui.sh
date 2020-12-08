#!/bin/sh

source jmeter-version.sh

apache-jmeter-$JMETER_VER/bin/jmeter.sh -t test-script.jmx
