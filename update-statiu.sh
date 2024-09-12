#stadiu/update-pub.sh

#rm -f stadiu/pub/.gitkeep
python3 ./parse_stadiu.py
tree -L 5 -I 'venv|*.log' > tree.txt
./q.sh > q.txt
#touch stadiu/pub/.gitkeep

