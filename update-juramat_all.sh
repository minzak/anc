#!/bin/bash

python3 ./get_juramat.py
python3 ./parse_juramat_all.py
rm -f *.log
tree -L 5 -I 'venv|*.log' > tree.txt
./q.sh > q.txt
echo "Packing DB."
tar -cvjSf data.db.bz2 data.db
