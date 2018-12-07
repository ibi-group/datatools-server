import sys

# start ec2 instances

# for each instance
    # install jmeter script with custom port number

    # run jmeter script on instance

# setup ssh tunnel on localhost for each ec2 instace

# ssh -L 24001:127.0.0.1:24001 \
# -R 25000:127.0.0.1:25000 \
# -L 26001:127.0.0.1:26001 -N -f <username>@<slave1IP>
