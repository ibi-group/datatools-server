#!/bin/bash

# WARNING: Deletes ALL databases for local MongoDB instance.
# Usage: ./restart-mongo-with-fresh-db.sh

sudo service mongod stop
sudo rm -rf /var/lib/mongodb/*
sudo service mongod start