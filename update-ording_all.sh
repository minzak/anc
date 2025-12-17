#!/bin/bash

source venv/bin/activate

cp -f data.db /dev/shm/
python3 ./get_ordins_no_ssl.py
python3 ./parse_ordins_all.py
mv -f /dev/shm/data.db $(pwd)/data.db
rm -f *.log
tree -L 5 -I 'venv|old|*.log' > tree.txt
./q.sh > q.txt
./qx.sh > qx.txt
./raw.sh
echo "Packing DB."
tar -cvjSf data.db.bz2 data.db

###
#wget https://cetatenie.just.ro/storage/2023/11/Ordin-1095-din-19-06-2024-art11.pdf -O 2023-11-Ordin-1095-din-19-06-2024-art11.pdf

#Unlisted link
#wget https://cetatenie.just.ro/storage/2021/06/Ordin-nr.-1907-din-04.10.2024.pdf -O 2021-06-Ordin-nr.-1907-din-04.10.2024.pdf
