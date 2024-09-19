#!/bin/bash

#stadiu/update-pub.sh

python3 ./parse_stadiu.py
tree -L 5 -I 'venv|*.log' > tree.txt
./q.sh > q.txt

tar -cvjSf data.db.bz2 data.db
