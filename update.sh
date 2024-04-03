#cd stadiu && ./update-pub.sh
#cd ..
#python3 ./parse_stadiu.py

rm -f ordins/.gitkeep
rm -f *.log
python3 ./get_ordins.py
python3 ./parse_ordins.py
tree -L 5 -I 'venv|*.log' > tree.txt
./q.sh > q.txt
touch ordins/.gitkeep
