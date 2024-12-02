#!/usr/bin/python3

import os
import re
import sqlite3
import logging
from datetime import datetime
import fitz # install using pip install PyMuPDF
import time

# Фиксируем время начала выполнения
start_time = time.time()

# Константы
CRED    = '\033[91m'
COK     = '\033[92m'
CWARN   = '\033[93m'
CVIOLET = '\033[95m'
CEND    = '\033[0m'

Ordins = './ordins/'
Database = './data.db'

# Параметры логирования:
# parse.log - основные логи
# sql-ГГГГ-ММ-ДД.log - лог sql-транзакций
LogFormat = logging.Formatter('%(message)s')
def setup_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)
    handler.setFormatter(LogFormat)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

# Main logger
logger = setup_logger('main_logger', 'parse-ordins.log')
# SQL logger
SQLlogger = setup_logger('SQLlogger', 'sql-'+datetime.now().strftime("%Y-%m-%d")+'.log')

connection = sqlite3.connect(Database)
connection.set_trace_callback(SQLlogger.info)
db = connection.cursor()

# Парсинг новых файлов
logger.info('Start parsing ordins at ' + datetime.now().strftime("%Y-%m-%d %M:%S"))

for filename in os.listdir(Ordins):
    if not filename.endswith(('.pdf', '.PDF')):
        continue
    date = ""
    try:
        with fitz.open(Ordins + filename) as doc:
            text = ""
            dosarcounter = 0
            print(f"{'Parsing: ' + CWARN + filename + CEND:.<205}", end="")
            for page in doc:
                text += page.get_text()
                if date == "":
                    match = re.search(r'\d{1,2}\.\d{1,2}\.20\d{2}', text)
                    if match:
                        date = match.group()
                        date = datetime.strptime(date, '%d.%m.%Y').date()
            dosars = re.findall(r'\((\d+)[\/][A-Za-z/]*(\d+)\)', text)
            logger.info('Ordin date is ' + (date.strftime('%Y-%m-%d') if date else 'unknown'))
            for dosar in dosars:
                dosarnum, dosaryear = dosar
                logger.info(f'Found dosar {dosarnum}/{dosaryear}')
                db.execute('UPDATE Dosar SET solutie = IIF(solutie IS NULL, ?, solutie), result = True WHERE id == ?',
                           (date, f'{dosarnum}/RD/{dosaryear}'))
                SQLlogger.info('Modified: ' + str(db.rowcount))
                dosarcounter += 1
            logger.info(f'In {filename} found {dosarcounter} dosars')
            print(f"{'found ' + COK + str(dosarcounter).zfill(4) + CEND + ' dosars'}")
    except Exception as e:
        logger.error(f"Error parsing file {filename}: {e}")
        print(f"{CRED}Error parsing file {filename}: {e}{CEND}")
    connection.commit()

# Помечаем result=Ture для дел, у которых номер приказа встречается больше одного раза
db.execute( 'UPDATE Dosar SET result = True WHERE result IS False AND ordin IN (SELECT ordin FROM Dosar GROUP BY ordin HAVING COUNT(*) > 1)' )
SQLlogger.info('Modified: ' + str(db.rowcount))
connection.commit()
connection.close()

# Фиксируем время окончания выполнения
end_time = time.time()
# Вычисляем время выполнения
execution_time = end_time - start_time
print(f"{'Parsing PDF time: '}{COK}{execution_time:.2f}{CEND} seconds")

quit()
