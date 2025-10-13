#!/bin/bash

cp -f data.db /dev/shm/
python3 ./get_juramat_no_ssl.py
python3 ./parse_juramat_all.py
python3 ./recompute_refuzuri.py
mv -f /dev/shm/data.db $(pwd)/data.db
rm -f *.log
tree -L 5 -I 'venv|old|*.log' > tree.txt
./q.sh > q.txt
./raw.sh
echo "Packing DB."
tar -cvjSf data.db.bz2 data.db

