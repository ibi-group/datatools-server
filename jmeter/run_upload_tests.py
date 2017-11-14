# a script to download all files from an s3 bucket and then run the upload jmeter test plan
# to reduce needed dependencies, this script calls the aws cli tool instead of using
# the python library for aws

import csv
import os
import re
import subprocess
import sys

# parse args
if len(sys.argv) < 3:
    print '''Usage:
    run_upload_tests.py test-plan-mode gtfs-feeds-s3-bucket [results-s3-bucket]
    
      test-plan-mode
        must be `upload` or `fetch` in terms of what datatools-server does.
        if `upload` this script will download all zip files from the s3 bucket 
        and upload them to datatools-server.  if `fetch` this script will instruct 
        datatools-server to create a feed version by fetching from a url
        
      gtfs-feeds-s3-bucket
        bucket to grab feeds from.
        If using fetch mode, these feeds must be publicly accessible from s3.
        must be a s3 bucket that is accessbile via your aws credentials
        
      resuts-s3-bucket
        Optional.
        bucket to upload results to
    '''
    sys.exit()
    
test_plan_mode = sys.argv[1]
gtfs_feeds_s3_bucket = sys.argv[2]

if test_plan_mode != 'upload' and test_plan_mode != 'fetch':
    print 'Invalid test plan mode'
    sys.exit()

# download a list of all files from an s3 bucket
output = subprocess.check_output(['aws', 's3', 'ls', gtfs_feeds_s3_bucket])

# prepare csv file headers
rows = [['project name', 'fetch or upload', 'file or http address']]

# loop through output
num_feeds_found = 0
for line in output.split('\n'):
    # determine if file in bucket is a zip file
    match = re.search('\d\s([\w-]*\.zip)', line)
    if match:
        num_feeds_found += 1
        zipfile = match.group(1)
        project_name = zipfile.rsplit('.', 1)[0]
        
        # download zip file if running in upload mode
        if test_plan_mode == 'upload':
            # create feed download dir if it doesn't exist
            try:
                os.makedirs('fixtures/feeds')
            except:
                pass
            
            # dl gtfs file
            file_or_location = 'fixtures/feeds/{0}'.format(zipfile)
            dl_args = [
                'aws', 
                's3', 
                'cp', 
                's3://{0}/{1}'.format(gtfs_feeds_s3_bucket, zipfile),
                file_or_location
            ]
            print ' '.join(dl_args)
            subprocess.check_output(dl_args)
        else:
            file_or_location = 'https://{0}.s3.amazonaws.com/{1}'.format(gtfs_feeds_s3_bucket, zipfile)
            
        # append row    
        rows.append([
            project_name, 
            test_plan_mode,
            file_or_location
            ])
            
print 'Found {0} feeds in this bucket'.format(num_feeds_found)

# write csv file
csv_filename = 'fixtures/s3-batch.csv'
with open(csv_filename, 'w') as f:
    writer = csv.writer(f)
    writer.writerows(rows)
    
# run jmeter
jmeter_args = ['./run-tests.sh', 'batch', '1', '1', csv_filename]
if len(sys.argv) == 4:
    jmeter_args.append(sys.argv[3])
    
print ' '.join(jmeter_args)
subprocess.check_output(jmeter_args)
