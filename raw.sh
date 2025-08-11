#!/bin/bash

#sqlite3 -echo -box data.db \
#'SELECT
#    CASE
#        WHEN CAST(strftime("%m", termen) AS INTEGER) BETWEEN 1 AND 3 THEN strftime("%Y", termen) || "-1 триместр"
#        WHEN CAST(strftime("%m", termen) AS INTEGER) BETWEEN 4 AND 6 THEN strftime("%Y", termen) || "-2 триместр"
#        WHEN CAST(strftime("%m", termen) AS INTEGER) BETWEEN 7 AND 9 THEN strftime("%Y", termen) || "-3 триместр"
#        WHEN CAST(strftime("%m", termen) AS INTEGER) BETWEEN 10 AND 12 THEN strftime("%Y", termen) || "-4 триместр"
#    END AS semestr,
#    COUNT(*) AS count
#FROM Dosar11 GROUP BY semestr ORDER BY semestr;'


#FROM Dosar11 WHERE termen >= CURRENT_DATE GROUP BY semestr ORDER BY semestr;'

sqlite3 -echo -box data.db 'SELECT * FROM Dosar11 ORDER BY year, number;' > raw_dosar11.txt
sqlite3 -echo -box data.db 'SELECT * FROM Termen11 ORDER BY id, termen;' > raw_termen11.txt
sqlite3 -echo -box data.db 'SELECT * FROM Refuz11 ORDER BY id, ordin;' > raw_refuz11.txt

sqlite3 -echo -box data.db "SELECT strftime('%Y-%m', solutie) AS year_month, COUNT(*) AS refusals_count FROM Refuz11 WHERE solutie IS NOT NULL GROUP BY year_month ORDER BY year_month;" > raw_refuz11_by_month.txt
