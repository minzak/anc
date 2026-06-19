#!/bin/bash

source venv/bin/activate
source ./lib_db.sh

rm -f data.db
rm -f *.log
rm -rf state          # full rebuild: wipe incremental sidecar (re-seeded at the end)

./init_db.sh
anc_shm_begin

#python3 ./get_stadiu.py
python3 ./parse_stadiu_all_mp.py

#python3 ./get_ordins.py
python3 ./parse_ordins_all.py

#python3 ./get_juramat.py
python3 ./parse_juramat_all.py

#python3 ./get_minori.py
python3 ./parse_minori_all.py

python3 ./recompute_refuzuri.py

anc_shm_end
anc_shm_teardown

# Full rebuild parsed every file; sync the incremental sidecar so subsequent
# update-* runs don't re-parse everything.
python3 ./incremental.py seed ordins juramat stadiu minori

tree -L 5 -I 'venv|old|__pycache__|*.log' > tree.txt
./q.sh > q.txt
./qx.sh > qx.txt
./raw.sh

echo "Packing DB."
tar -cvjSf data.db.bz2 data.db
