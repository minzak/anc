#!/bin/bash

python3 ./get_stadiu.py
python3 ./parse_stadiu_all.py
#python3 ./parse_stadiu_silent.py
tree -L 5 -I 'venv|*.log' > tree.txt
./q.sh > q.txt
echo "Packing DB."
tar -cvjSf data.db.bz2 data.db
