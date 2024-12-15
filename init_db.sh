#!/bin/bash

#Run once to create DB
#echo "Creating database and tables..."
sqlite3 data.db '.read create_tables.sql'

#sqlite3 data.db 'ALTER TABLE Dosar ADD COLUMN anexa INTEGER'
#sqlite3 data.db 'ALTER TABLE Dosar ADD COLUMN cminori INTEGER'