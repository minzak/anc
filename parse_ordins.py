#!/usr/bin/python3


import os
import re
import sqlite3
import logging
from datetime import datetime
import fitz # install using pip install PyMuPDF

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

logger.info( 'Start parsing ordins at ' + datetime.now().strftime("%Y-%m-%d %M:%S") )
filecounter = 1
for filename in os.listdir(Ordins):
    date=""
    # Проходимся по списку файлов приказов и выдёргиваем данные:
    # Дата приказа (date) - первое попавшаяся дата в формате "XX.YY.ZZZZ" с вероятными пробелами рядом с точками
    # Приказ - парсинг по формату "(XXXX/YYYY)"
    with fitz.open(Ordins + filename) as doc:
        text = ""
        dosarcounter=0
        for page in doc:
            text += page.get_text()
            if date == "":
                # Если дата пустая, заполняем
                date = re.search('\d*\d[ ]*\.*[ ]*\d*\d[ ]*\.*[ ]*20\d\d', text, re.MULTILINE).group()
                # Чистим дату от пробелов
                date = date.replace(" ", "")
                # Добавляем ноль в начало даты вида 9.07.2023
                if date[1] == '.':
                    date = '0' + date
                # Разбираемся с точками, кое-где пропускаую точку в дате приказа
                date = re.sub(r'(\d\d)\.*(\d\d)\.*(\d\d\d\d)', r'\1.\2.\3', date)
                # Форматируем в datetime-переменную date c датой приказа
                # TODO: брать дату приказа со страницы на сайте ANC, т.к. с даты вообще может не быть в самом приказе
                date = datetime.strptime(date, '%d.%m.%Y').date()
        dosars = re.findall(r'\((\d+)[\/][A-Za-z/]*(\d+)\)', text, re.MULTILINE)
        logger.info( 'Ordin date is ' + date.strftime('%Y-%m-%d') )
        for dosar in dosars:
            dosarnum = dosar[0]
            dosaryear = dosar[1]
            logger.info( 'Found dosar ' + dosarnum + '/' + dosaryear )
            # Апдейт базы. Дело, которое нашлось в приказе, помечаем как вышедшее с успехом (колонка result) и ставим дату выхода в приказ (колонка solutie)
            db.execute( 'UPDATE Dosar SET solutie = IIF( solutie IS NULL, ?, solutie), result = True WHERE id == ?', (date, dosarnum + '/RD/' + dosaryear) )
            SQLlogger.info('Modified: ' + str(db.rowcount))
            dosarcounter += 1
        logger.info( 'In ' + filename + ' found ' + str(dosarcounter) + ' dosars' )
        print( 'In ' + filename + ' found ' + str(dosarcounter) + ' dosars' )
    connection.commit()
    filecounter += 1
# Помечаем result=Ture для дел, у которых номер приказа встречается больше одного раза
db.execute( 'UPDATE Dosar SET result = True WHERE result IS False AND ordin IN (SELECT ordin FROM Dosar GROUP BY ordin HAVING COUNT(*) > 1)' )
SQLlogger.info('Modified: ' + str(db.rowcount))
connection.commit()
connection.close()
