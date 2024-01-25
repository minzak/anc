cd stadiu && ./update-pub.sh
cd ..
python3 ./get_ordins.py
python3 ./parse_stadiu.py
python3 ./parse_ordins.py
tree -L 5 > tree.txt

./q.sh
