#!/bin/bash

source venv/bin/activate
source ./lib_db.sh

anc_shm_begin
python3 ./parse_ordins_new.py
anc_shm_end
anc_shm_teardown

#rm -f *.log
tree -L 5 -I 'venv|old|__pycache__|*.log' > tree.txt
./q.sh > q.txt
./qx.sh > qx.txt
./raw.sh
echo "Packing DB."
tar -cvjSf data.db.bz2 data.db
