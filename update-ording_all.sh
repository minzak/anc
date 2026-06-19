#!/bin/bash

source venv/bin/activate
source ./lib_db.sh

anc_shm_begin
# Full reparse, resumable: wipe progress so all ordins are reprocessed.
# parse_ordins_new downloads new files, then parses every unmarked file (= all,
# since state was wiped), marking each -> a re-run after a crash continues on.
rm -f state/ordins.json
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

###
#wget https://cetatenie.just.ro/storage/2023/11/Ordin-1095-din-19-06-2024-art11.pdf -O 2023-11-Ordin-1095-din-19-06-2024-art11.pdf

#Unlisted link
#wget https://cetatenie.just.ro/storage/2021/06/Ordin-nr.-1907-din-04.10.2024.pdf -O 2021-06-Ordin-nr.-1907-din-04.10.2024.pdf
