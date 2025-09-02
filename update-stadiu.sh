#!/bin/bash

rm -f *.log
mv -f data.db /dev/shm/
python3 ./get_stadiu.py
python3 ./parse_stadiu_new_mp.py
mv -f /dev/shm/data.db $(pwd)/data.db
python3 ./recompute_refuzuri.py
tree -L 5 -I 'venv|old|*.log' > tree.txt
./q.sh > q.txt
./raw.sh
echo "Packing DB."
tar -cvjSf data.db.bz2 data.db
