#!/usr/bin/python3

import sys
import time
import logging
import sqlite3
from datetime import datetime

#DB_PATH = 'data.db'
DB_PATH = '/dev/shm/data.db'

# Константы
CRED    = '\033[91m'
COK     = '\033[92m'
CWARN   = '\033[93m'
CVIOLET = '\033[95m'
CEND    = '\033[0m'

# Фиксируем время начала выполнения
start_time = time.time()

# Logging setup
def setup_logger(name, log_file, level=logging.INFO, mode='w'):
    LogFormat = logging.Formatter('%(message)s')
    handler = logging.FileHandler(log_file, mode=mode)
    handler.setFormatter(LogFormat)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

SQLlogger = setup_logger('SQLlogger', 'sql-refus-'+datetime.now().strftime("%Y-%m-%d")+'.log', mode='w')

connection = sqlite3.connect(DB_PATH)
db = connection.cursor()

# Всего записей с ordin для прогресса
total_ids = db.execute('SELECT COUNT(*) FROM Dosar11 WHERE ordin IS NOT NULL').fetchone()[0]
print(f"\nTotal records with ordins: {COK}{total_ids}{CEND}")

before_changes = connection.total_changes
# Массовое выставление refuz=1 по уникальным ordin
sql_query = '''
    UPDATE Dosar11
    SET refuz=1
    WHERE ordin IN (
        SELECT ordin
        FROM Dosar11
        WHERE ordin IS NOT NULL
        GROUP BY ordin
        HAVING COUNT(*) = 1
    )
'''
#SQLlogger.info('Executing SQL: ' + sql_query)  # Or print(sql_query)
db.execute(sql_query)
after_changes = connection.total_changes
SQLlogger.info('Refuz set: ' + str(db.rowcount))
SQLlogger.info("Before changes: {} After changes: {} Total DB changes: {}".format(before_changes, after_changes, after_changes - before_changes))
print(f"Total refuz set to 1 with uniq ordin = 1: {COK}{str(db.rowcount)}{CEND}")

# Пересборка таблицы отказов
#db.execute('DELETE FROM Refuz11')
db.execute('''
    INSERT OR REPLACE INTO Refuz11 (id, ordin, depun, solutie)
    SELECT id, ordin, depun, solutie
    FROM Dosar11
    WHERE refuz=1 AND ordin IS NOT NULL
''')
SQLlogger.info('Refuz11 rebuilt: ' + str(db.rowcount))
print(f"Copyy All data to Refuz11: {COK}{str(db.rowcount)}{CEND}")

connection.commit()
connection.close()
print("Done.")

# Вычисляем время выполнения
end_time = time.time()
execution_time = end_time - start_time
print(f"\nExecution time: {COK}{execution_time:.2f}{CEND} seconds")
