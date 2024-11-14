#!/bin/bash

mkdir pub/`date +%Y-%m-%d` 2>/dev/null
for year in 2017 2018 2019 2020 2021 2022 2023 2024; do
    #wget -q --show-progress -P pub/`date +%Y-%m-%d` "https://cetatenie.just.ro/wp-content/uploads/2023/11/Art.-11-$year-Redobandire.pdf"
    #echo "https://cetatenie.just.ro/storage/2024/11/articol_11_"$year"_12_11_2024.pdf"
    wget -q --show-progress -P pub/`date +%Y-%m-%d` "https://cetatenie.just.ro/storage/2024/11/articol_11_"$year"_12_11_2024.pdf"
done

#https://cetatenie.just.ro/storage/2024/08/Art.-11-2023-Redobandire.xlsx.pdf
