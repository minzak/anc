#!/bin/bash

sqlite3 -echo -box data.db \
'SELECT
    CASE
        WHEN CAST(strftime("%m", termen) AS INTEGER) BETWEEN 1 AND 3 THEN strftime("%Y", termen) || "-1 триместр"
        WHEN CAST(strftime("%m", termen) AS INTEGER) BETWEEN 4 AND 6 THEN strftime("%Y", termen) || "-2 триместр"
        WHEN CAST(strftime("%m", termen) AS INTEGER) BETWEEN 7 AND 9 THEN strftime("%Y", termen) || "-3 триместр"
        WHEN CAST(strftime("%m", termen) AS INTEGER) BETWEEN 10 AND 12 THEN strftime("%Y", termen) || "-4 триместр"
    END AS semestr,
    COUNT(*) AS count
FROM Dosar GROUP BY semestr ORDER BY semestr;'


#FROM Dosar WHERE termen >= CURRENT_DATE GROUP BY semestr ORDER BY semestr;'

sqlite3 -echo -box data.db 'SELECT * FROM Dosar ORDER BY year, number;' > raw.txt
