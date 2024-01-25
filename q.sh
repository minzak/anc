sqlite3 -echo -box data.db 'SELECT * FROM Dosar WHERE result=True AND year=2023;'
sqlite3 -echo -box data.db 'SELECT * from Dosar where id="48275/RD/2023";'
sqlite3 -echo -box data.db 'SELECT * from Dosar where id="49000/RD/2023";'

sqlite3 -echo -box data.db '.read stat.sql'
