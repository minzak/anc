#!/bin/bash

python3 ./parse_ordins_new.py
rm -f *.log
tree -L 5 -I 'venv|*.log' > tree.txt
./q.sh > q.txt
echo "Packing DB."
tar -cvjSf data.db.bz2 data.db

