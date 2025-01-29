#!/bin/bash

rm -f data.db
./init_db.sh

python3 ./get_stadiu.py
python3 ./parse_stadiu_silent.py

python3 ./get_ordins.py
python3 ./parse_ordins_all.py

python3 ./get_juramat.py
python3 ./parse_juramat_all.py

tree -L 5 -I 'venv|*.log' > tree.txt
./q.sh > q.txt
echo "Packing DB."
tar -cvjSf data.db.bz2 data.db

