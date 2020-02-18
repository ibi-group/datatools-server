#!/bin/sh

source jmeter-version.sh

if [ -z $1 ]
then
  >&2 echo 'Must supply "batch", "fetch", "query" or "upload" as first argument'
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
  if [ "$1" == "batch" ]
  then
    >&2 echo 'Must supply fourth argument (csv file) in batch mode'
    exit 1
  else
    echo 'WARNING: name of project not supplied.  In upload mode, all projects will be named "test project #"'
  fi
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

jmeter_cmd="apache-jmeter-$JMETER_VER/bin/jmeter.sh -n -t test-script.jmx -l output/result/result.csv -e -o output/report -Jmode=$1 -Jthreads=$2 -Jloops=$3"

if [ -n "$4" ]
then
  if [ "$1" == "batch" ]
  then
    jmeter_cmd="$jmeter_cmd -Jbatchfile=$4"
  else
    jmeter_cmd="$jmeter_cmd -Jproject=$4"
  fi
fi

echo "$jmeter_cmd"
eval "$jmeter_cmd"

tar_file="output.tar.gz" 
if [ -n "$4" ] && [ "$1" != "batch" ]
then
  tar_file="$4.tar.gz"
fi
tar -czf $tar_file output

if [ -z $5 ]
then
  echo 'WARNING: s3 bucket not supplied, results will not be uploaded to s3'
else
  s3location="s3://$5/dt_jmeter_run_"
  if [ -n "$4" ]
  then
    s3location=${s3location}${tar_file}_
  fi
  s3location="$s3location$(date +%Y-%m-%dT%H-%M-%S-%Z).tar.gz"
  
  echo "Uploading to $s3location"
  aws s3 cp $tar_file $s3location
  echo "Uploaded to $s3location"
fi

echo "done"
