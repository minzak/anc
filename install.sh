#!/bin/bash

sudo apt-get install python3-dev python3-venv sqlite3 build-essential libssl-dev libffi-dev python3-pip python3-wheel gcc libpq-dev -y

#If work under NON-root in venv
python3 -m venv venv
source venv/bin/activate

pip3 install -r requirements.txt
