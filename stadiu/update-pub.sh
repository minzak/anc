#!/bin/bash

mkdir pub/`date +%Y-%m-%d` 2>/dev/null
for year in `seq 2015 2024`; do
    #wget -q --show-progress -P pub/`date +%Y-%m-%d` "https://cetatenie.just.ro/wp-content/uploads/2023/11/Art.-11-$year-Redobandire.pdf"
    wget -q --show-progress -P pub/`date +%Y-%m-%d` "https://cetatenie.just.ro/storage/2024/08/Art.-11-$year-Redobandire.xlsx.pdf"
done

#https://cetatenie.just.ro/storage/2024/08/Art.-11-2023-Redobandire.xlsx.pdf
