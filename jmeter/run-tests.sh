#!/bin/sh

if [ -z $1 ]
then
  >&2 echo 'Must supply "upload" or "fetch" as first argument'
  exit 1
fi

if [ -z $2 ]
then
  >&2 echo 'Must supply second argument (number of threads)'
  exit 1
fi

if [ -z $3 ]
then
  >&2 echo 'Must supply third argument (number of loops)'
  exit 1
fi

if [ -z $4 ]
then
  echo 'WARNING: name of project not supplied.  In upload mode, all projects will be named "test project #"'
fi

if [ -z $5 ]
then
  echo 'WARNING: s3 bucket not supplied, results will not be uploaded to s3'
fi

# clean up old output
rm -rf output
mkdir output
mkdir output/result
mkdir output/report

echo "starting jmeter script"

jmeter_cmd="apache-jmeter-3.3/bin/jmeter.sh -n -t test-script.jmx -l output/result/result.csv -e -o output/report -Jmode=$1 -Jthreads=$2 -Jloops=$3"

if [ -n "$4" ]
then
  jmeter_cmd="$jmeter_cmd -Jproject=$4"
fi

echo "$jmeter_cmd"
eval "$jmeter_cmd"

if [ -z $5 ]
then
  echo 'WARNING: s3 bucket not supplied, results will not be uploaded to s3'
else
  tar -czvf output.tar.gz output
  s3location="s3://$5/dt_jmeter_run_$(date +%Y-%m-%dT%H-%M-%S-%Z).tar.gz"
  echo "Uploading to $s3location"
  aws s3 cp output.tar.gz $s3location
  echo "Uploaded to $s3location"
fi

echo "done"
