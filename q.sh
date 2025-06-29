#!/bin/bash

#sqlite3 -echo -box data.db 'SELECT * FROM Dosar11 WHERE result=True AND year=2024;'
#sqlite3 -echo -box data.db 'SELECT * FROM Dosar11 WHERE result=True AND year=2023 AND number > 23000;'

#Расшифровка:
#substr(id, length(id)-3, 4) — последние 4 символа строки, это <год>.
#CAST(... AS INTEGER) приводит подстроку к числу.
#instr(id, '/') — позиция первого слэша. substr(id, 1, instr(id, '/') - 1) возвращает все символы до первого слэша, то есть <число>.
sqlite3 -echo -box data.db '
SELECT * FROM Dosar11
WHERE result = 1
AND (    CAST(substr(id, length(id)-3, 4) AS INTEGER) > 2023
    OR ( CAST(substr(id, length(id)-3, 4) AS INTEGER) = 2023
           AND
         CAST(substr(id, 1, instr(id, "/") - 1) AS INTEGER) > 40000  )
);'

#https://t.me/Yuliya_pm
sqlite3 -echo -box data.db 'SELECT * from Dosar11 where id="48275/RD/2023";'
#sqlite3 -echo -box data.db 'SELECT * from Dosar11 where depun="2023-12-14";'
sqlite3 -echo -box data.db 'SELECT COUNT(*) AS count FROM Dosar11 WHERE depun = "2023-12-14";'

#https://t.me/ProAdmin
sqlite3 -echo -box data.db 'SELECT * from Dosar11 where id="49000/RD/2023";'
#sqlite3 -echo -box data.db 'SELECT * from Dosar11 where depun="2023-12-19";'
sqlite3 -echo -box data.db 'SELECT COUNT(*) AS count FROM Dosar11 WHERE depun = "2023-12-19";'

#https://t.me/msgme1
sqlite3 -echo -box data.db 'SELECT * from Dosar11 where id="14833/RD/2024";'
#https://t.me/Olyasoroka23
sqlite3 -echo -box data.db 'SELECT * from Dosar11 where id="27030/RD/2021";'
#https://t.me/vorkos
sqlite3 -echo -box data.db 'SELECT * from Dosar11 where id="38286/RD/2021";'

sqlite3 -echo -box data.db '.read stat.sql'

#sqlite3 -echo -box data.db 'SELECT MAX(termen) AS max_termen FROM Dosar11;'
#sqlite3 -echo -box data.db 'SELECT termen, COUNT(*) AS count FROM Dosar11 WHERE termen >= CURRENT_DATE GROUP BY termen ORDER BY termen;'

