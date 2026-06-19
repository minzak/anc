#!/bin/bash

source venv/bin/activate
source ./lib_db.sh

anc_shm_begin
# Full reparse: forget progress so every file is reprocessed, but resumably —
# parse_minori_new marks each file as it is parsed, so a re-run after a crash
# continues from where it stopped (reads .state, skips already-done files).
rm -f state/minori.json
python3 ./get_minori_no_ssl.py
python3 ./parse_minori_new.py
anc_shm_end
anc_shm_teardown

#rm -f *.log
tree -L 5 -I 'venv|old|__pycache__|*.log' > tree.txt
./q.sh > q.txt
./qx.sh > qx.txt
./raw.sh
echo "Packing DB."
tar -cvjSf data.db.bz2 data.db
