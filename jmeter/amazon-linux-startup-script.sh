#!/bin/bash

# install java 8
yum install java-1.8.0 -y
yum remove java-1.7.0-openjdk -y

source jmeter-version.sh

# install jmeter
./install-jmeter.sh

# TODO: update jmeter.properties file
# http://www.testingdiaries.com/jmeter-on-aws/

# start up jmeter server
apache-jmeter-$JMETER_VER/bin/jmeter-server
