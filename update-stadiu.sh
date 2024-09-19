#!/bin/bash

#stadiu/update-pub.sh

rm ./stadiu/pub/.gitkeep
python3 ./parse_stadiu.py
#python3 ./parse_stadiu_silent.py
touch ./stadiu/pub/.gitkeep
tree -L 5 -I 'venv|*.log' > tree.txt
./q.sh > q.txt

tar -cvjSf data.db.bz2 data.db
