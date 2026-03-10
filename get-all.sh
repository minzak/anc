#!/bin/bash

source venv/bin/activate

python3 ./get_minori_no_ssl.py
python3 ./get_juramat_no_ssl.py
python3 ./get_ordins_no_ssl.py
python3 ./get_stadiu_no_ssl.py
