#!/bin/bash

cp -f data.db /dev/shm/
python3 ./parse_ordins_new.py
mv -f /dev/shm/data.db $(pwd)/data.db
rm -f *.log
tree -L 5 -I 'venv|old|*.log' > tree.txt
./q.sh > q.txt
./raw.sh
echo "Packing DB."
tar -cvjSf data.db.bz2 data.db
