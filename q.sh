#!/bin/bash

#sqlite3 -echo -box data.db 'SELECT * FROM Dosar WHERE result=True AND year=2024;'
sqlite3 -echo -box data.db 'SELECT COUNT(2023) FROM Dosar WHERE result=True AND year=2023;'
sqlite3 -echo -box data.db 'SELECT * FROM Dosar WHERE result=True AND year=2023 AND number > 23000;'

#https://t.me/Yuliya_pm
sqlite3 -echo -box data.db 'SELECT * from Dosar where id="48275/RD/2023";'
#sqlite3 -echo -box data.db 'SELECT * from Dosar where termen="2024-03-21";'
#sqlite3 -echo -box data.db 'SELECT * from Dosar where depun="2023-12-14";'
sqlite3 -echo -box data.db 'SELECT COUNT(*) AS count FROM Dosar WHERE termen = "2024-03-21";'

#https://t.me/ProAdmin
sqlite3 -echo -box data.db 'SELECT * from Dosar where id="49000/RD/2023";'
#sqlite3 -echo -box data.db 'SELECT * from Dosar where termen="2024-04-26";'
#sqlite3 -echo -box data.db 'SELECT * from Dosar where depun="2023-12-19";'
sqlite3 -echo -box data.db 'SELECT COUNT(*) AS count FROM Dosar WHERE termen = "2024-04-26";'

#https://t.me/msgme1
sqlite3 -echo -box data.db 'SELECT * from Dosar where id="14833/RD/2024";'
#https://t.me/Olyasoroka23
sqlite3 -echo -box data.db 'SELECT * from Dosar where id="27030/RD/2021";'
#https://t.me/vorkos
sqlite3 -echo -box data.db 'SELECT * from Dosar where id="38286/RD/2021";'

sqlite3 -echo -box data.db '.read stat.sql'

#sqlite3 -echo -box data.db 'SELECT MAX(termen) AS max_termen FROM Dosar;'
sqlite3 -echo -box data.db 'SELECT termen, COUNT(*) AS count FROM Dosar WHERE termen >= CURRENT_DATE GROUP BY termen ORDER BY termen;'

