#!/bin/bash

rm -f *.log
cp -f data.db /dev/shm/
python3 ./get_stadiu.py
python3 ./parse_stadiu_all_mp.py
cp -f /dev/shm/data.db $(pwd)/data.db
tree -L 5 -I 'venv|old|*.log' > tree.txt
./q.sh > q.txt
echo "Packing DB."
tar -cvjSf data.db.bz2 data.db
