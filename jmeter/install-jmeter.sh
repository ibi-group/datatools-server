#!/bin/bash

# install jmeter
wget https://archive.apache.org/dist/jmeter/binaries/apache-jmeter-3.3.zip
unzip apache-jmeter-3.3.zip
rm -rf apache-jmeter-3.3.zip

# install jmeter plugin manager
wget -O apache-jmeter-3.3/lib/ext/jmeter-plugins-manager-0.16.jar https://jmeter-plugins.org/get/

# install command line runner
wget -O apache-jmeter-3.3/lib/cmdrunner-2.0.jar http://search.maven.org/remotecontent?filepath=kg/apc/cmdrunner/2.0/cmdrunner-2.0.jar

# run jmeter to generate command line script
java -cp apache-jmeter-3.3/lib/ext/jmeter-plugins-manager-0.16.jar org.jmeterplugins.repository.PluginManagerCMDInstaller

# install jpgc-json-2
apache-jmeter-3.3/bin/PluginsManagerCMD.sh install jpgc-json
