sqlite3 -echo -box data.db 'SELECT * FROM Dosar WHERE result=True AND year=2024;'
sqlite3 -echo -box data.db 'SELECT * FROM Dosar WHERE result=True AND year=2023;'

#https://t.me/Yuliya_pm
sqlite3 -echo -box data.db 'SELECT * from Dosar where id="48275/RD/2023";'
#https://t.me/ProAdmin
sqlite3 -echo -box data.db 'SELECT * from Dosar where id="49000/RD/2023";'
#https://t.me/msgme1
sqlite3 -echo -box data.db 'SELECT * from Dosar where id="14833/RD/2024";'
#https://t.me/vorkos
sqlite3 -echo -box data.db 'SELECT * from Dosar where id="38286/RD/2021";'

sqlite3 -echo -box data.db '.read stat.sql'
