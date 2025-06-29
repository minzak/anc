#!/bin/bash

rm -f data.db
rm -f *.log

./init_db.sh
cp -f data.db /dev/shm/

python3 ./get_stadiu.py
python3 ./parse_stadiu_all_mp.py
cp -f /dev/shm/data.db $(pwd)/data.db

python3 ./get_ordins.py
python3 ./parse_ordins_all.py

python3 ./get_juramat.py
python3 ./parse_juramat_all.py

python3 ./get_minori.py
#python3 ./parse_minori_all.py

tree -L 5 -I 'venv|old|*.log' > tree.txt
./q.sh > q.txt

echo "Packing DB."
tar -cvjSf data.db.bz2 data.db
