#cd stadiu && ./update-pub.sh
#cd ..
#python3 ./parse_stadiu.py

rm -f ordins/.gitkeep
python3 ./get_ordins.py

rm -f ordins/2023-11-Ordin-920-din-25.04.2024-art-11.pdf
rm -f ordins/2023-11-Ordin-1097-P-2024-din-26-06-2024-Art-11-NPE.pdf
python3 ./parse_ordins.py
python3 ./get_ordins.py
tree -L 5 -I 'venv|*.log' > tree.txt
./q.sh > q.txt
touch ordins/.gitkeep
rm -f *.log

###
#wget https://cetatenie.just.ro/storage/2023/11/Ordin-1095-din-19-06-2024-art11.pdf -O 2023-11-Ordin-1095-din-19-06-2024-art11.pdf
#wget https://cetatenie.just.ro/storage/2023/11/Ordin-1097-P-2024-din-26-06-2024-Art-11-NPE.pdf -O 2023-11-Ordin-1097-P-2024-din-26-06-2024-Art-11-NPE.pdf
