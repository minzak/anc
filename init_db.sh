#!/bin/bash

#Run once to create DB
#echo "Creating database and tables..."
sqlite3 data.db '.read create_tables.sql'
