#!/bin/bash

source venv/bin/activate
source ./lib_db.sh

anc_shm_begin
# Full reparse, resumable: wipe progress so all files are reprocessed, but
# parse_juramat_new marks each file as parsed -> re-run continues after a crash.
rm -f state/juramat.json
python3 ./get_juramat_no_ssl.py
python3 ./parse_juramat_new.py
python3 ./recompute_refuzuri.py
anc_shm_end
anc_shm_teardown

#rm -f *.log
tree -L 5 -I 'venv|old|__pycache__|*.log' > tree.txt
./q.sh > q.txt
./qx.sh > qx.txt
./raw.sh
echo "Packing DB."
tar -cvjSf data.db.bz2 data.db
