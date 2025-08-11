#!/usr/bin/python3

import sqlite3
import time
import sys
from datetime import datetime

DB_PATH = '/dev/shm/data.db'


connection = sqlite3.connect(DB_PATH)
db = connection.cursor()

# Всего записей с ordin для прогресса
total_ids = db.execute('SELECT COUNT(*) FROM Dosar11 WHERE ordin IS NOT NULL').fetchone()[0]
print(f"Всего записей с приказом: {total_ids}")

# Массовое выставление refuz=1 по уникальным ordin
db.execute('''
    UPDATE Dosar11
    SET refuz=1
    WHERE ordin IN (
        SELECT ordin
        FROM Dosar11
        WHERE ordin IS NOT NULL
        GROUP BY ordin
        HAVING COUNT(*) = 1
    )
''')

# Пересборка таблицы отказов
#db.execute('DELETE FROM Refuz11')
db.execute('''
    INSERT OR REPLACE INTO Refuz11 (id, ordin, depun, solutie)
    SELECT id, ordin, depun, solutie
    FROM Dosar11
    WHERE refuz=1 AND ordin IS NOT NULL
''')
connection.commit()

print("Готово.")
connection.close()


