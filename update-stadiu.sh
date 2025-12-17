#!/bin/bash

source venv/bin/activate

rm -f *.log
mv -f data.db /dev/shm/
python3 ./get_stadiu.py
python3 ./parse_stadiu_new_mp.py
python3 ./recompute_refuzuri.py
mv -f /dev/shm/data.db $(pwd)/data.db
tree -L 5 -I 'venv|old|*.log' > tree.txt
./q.sh > q.txt
./qx.sh > qx.txt
./raw.sh
echo "Packing DB."
tar -cvjSf data.db.bz2 data.db
