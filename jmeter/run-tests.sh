#!/bin/sh

if [ -z $1 ]
then
  echo 'WARNING: s3 bucket not supplied, results will not be uploaded to s3'
fi

# clean up old output
rm -rf output
mkdir output
mkdir output/result
mkdir output/report

echo "Begin jmeter script"

apache-jmeter-3.3/bin/jmeter.sh -n -t test-script.jmx -l output/result/result.csv -e -o output/report

tar -czvf output.tar.gz output

if [ -z $1 ]
then
  echo 'WARNING: s3 bucket not supplied, results will not be uploaded to s3'
else
  s3location="s3://$1/dt_jmeter_run_$(date +%Y-%m-%dT%H-%M-%S-%Z).tar.gz"
  echo "Uploading to $s3location"
  aws s3 cp output.tar.gz $s3location
  echo "Uploaded to $s3location"
fi

echo "done"
