#!/usr/bin/python3


import os
import requests
import re
import sqlite3
from datetime import datetime
import logging
import fitz # install using pip install PyMuPDF

Stadiu = './stadiu/pub/'
Database = './data.db'


# Функция проверки текстовой строки на валидность.
# Должна быть либо в формате ДД.ММ.ГГГ, либо None
def vali_date(date_text):
    try:
        if date_text:
            datetime.strptime(date_text, '%d.%m.%Y').date()
        return True
    except ValueError:
        return False

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
logger = setup_logger('main_logger', 'parse.log')
# SQL logger
SQLlogger = setup_logger('SQLlogger', 'sql-'+datetime.now().strftime("%Y-%m-%d")+'.log')


connection = sqlite3.connect(Database)
connection.set_trace_callback(SQLlogger.info)
db = connection.cursor()

for directory in sorted(os.listdir(Stadiu)):
    # Сохраняем дату публикации стадиу для таблицы с терменами
    STADIUPUBDATE = datetime.strptime(directory, '%Y-%m-%d').date()
    # Цикл по всем pdf в директории
    for filename in os.listdir(Stadiu+directory):
        if not filename.endswith(('.pdf','.PDF')):
            continue
        logger.info('=== Parsing file ' + Stadiu + directory + '/' + filename)
        with fitz.open(Stadiu+directory+'/'+filename) as doc:
            for page in doc:
                tabs = page.find_tables(vertical_strategy='text', min_words_vertical=2, horizontal_strategy='text')
                if len(tabs.tables)<1:
                    logger.error('FAIL: На одной из страниц файла ' + filename + ', после досара ' + ID + 'не найдено таблиц')
                    continue
                tab = tabs[0]
                for line in tab.extract():  # print cell text for each row
                    ID = line[0]
                    if ID and ID[0].isdigit():
                        # Обработка ошибок сканирования строк
                        size_line = len(line)
                        filtered_line = list(filter(None, line))
                        size_filtered_line = len(filtered_line)
                        if size_line == 5:
                            # Строка распозналась корректно, все 5 полей
                            print('Алгоритм - 5 полей', line)
                            logger.info('Алгоритм - 5 полей ' + '[ ' + '; '.join(line) + ' ]')
                            DEPUN   = line[1]
                            TERMEN  = line[2]
                            ORDIN   = line[3]
                            SOLUTIE = line[4]
                        elif size_filtered_line == 4:
                            # 4 поля, считаем, что это id, дата досара, номер приказа и дата решения
                            print('Алгоритм - 4 поля', line)
                            logger.info('Алгоритм - 4 поля ' + '[ ' + '; '.join(line) + ' ]')
                            DEPUN   = filtered_line[1]
                            TERMEN  = None
                            ORDIN   = filtered_line[2]
                            SOLUTIE = filtered_line[3]
                        elif size_filtered_line == 3:
                            string  = filtered_line[-1]
                            if '/' in string and vali_date(string.split('/')[-1]):
                                # Хак для старых стадиу, где дата решения зашита в номер приказа
                                # В старых досар могут быть такие комбинации:
                                #   id, дата досара, номер приказа (с датой)
                                #   id, дата досара, термен - этот вариант в другой ветке if
                                print('Алгоритм - 3 поля, старый стадиу', line)
                                logger.info('Алгоритм - 3 поля, старый стадиу ' + '[ ' + '; '.join(line) + ' ]')
                                DEPUN   = line[1]
                                TERMEN  = None
                                ORDIN   = string
                                SOLUTIE = string.split('/')[-1]
                            elif '/' in string and not vali_date(string.split('/')[-1]):
                                # Хак для старых стадиу, где для досара указан только номер приказа без даты
                                # Для таких дел не будет даты решения, только номер приказа
                                print('Алгоритм - 3 поля, нет даты приказа', line)
                                logger.info('Алгоритм - 3 поля, нет даты приказа ' + '[ ' + '; '.join(line) + ' ]')
                                DEPUN   = line[1]
                                TERMEN  = None
                                ORDIN   = string
                                # Последняя попытка вытащить дату решения парсингом регулярками всего текста страницы
                                regex_result = re.search(ORDIN+'\n(\d\d.\d\d.\d\d\d\d)', page.get_text(), re.MULTILINE)
                                SOLUTIE = None if not regex_result else regex_result.group(1)
                            else:
                                # 3 поля, считаем, что это id, дата досара, термен
                                print('Алгоритм - 3 поля', line)
                                logger.info('Алгоритм - 3 поля ' + '[ ' + '; '.join(line) + ' ]')
                                DEPUN   = filtered_line[1]
                                TERMEN  = filtered_line[2]
                                ORDIN   = None
                                SOLUTIE = None
                        elif size_filtered_line == 2:
                            # 2 поля, считаем, что это id и дата досара
                            logger.info('Алгоритм - 2 поля ' + '[ ' + '; '.join(line) + ' ]')
                            print('Алгоритм - 2 поля', line)
                            DEPUN   = filtered_line[1]
                            TERMEN  = None
                            ORDIN   = None
                            SOLUTIE = None
                        else:
                            logger.info('FAIL: Ошибка распознавания строки [ ' + '; '.join(line) + ' ]')
                            continue
                        # Валидация сканированных данных и пропуск, если данные некорректны
                        if not vali_date(DEPUN) or not vali_date(TERMEN) or not vali_date(SOLUTIE):
                            logger.info('FAIL: Ошибка распознавания строки [ ' + '; '.join(line) + ' ]')
                            continue
                        else:
                            output_str = '\t\t\t\t\t\t\t\t\t\t\t\t\t' + str(ID) + '\t' + str(DEPUN) + '\t' + str(TERMEN) +'\t\t' + str(ORDIN) +'\t' + str(SOLUTIE)
                            logger.info(output_str)
                            print(output_str.expandtabs())
                        # Форматирование данных и исправление других ошибок
                        YEAR    = int(ID.split("/")[-1])
                        NUMBER  = int(ID.split("/")[0])
                        DEPUN   = datetime.strptime(DEPUN, '%d.%m.%Y').date()
                        # Часто косячат с годом подачи дела. В таком случае год подачи берем из порядкового номера
                        # Пример: дело 1234/RD/2019 подано в 2023 году. Тогда меняем год в дате и ставим год=2019
                        if DEPUN.year != YEAR:
                            DEPUN = DEPUN.replace(year=YEAR)

                        if TERMEN:
                            TERMEN = datetime.strptime(TERMEN, '%d.%m.%Y').date()
                        else:
                            TERMEN = None
                        if not ORDIN:
                            ORDIN = None
                        if SOLUTIE:
                            SOLUTIE = datetime.strptime(SOLUTIE, '%d.%m.%Y').date()
                        else:
                            SOLUTIE = None

                        # Поля таблицы Dosar:
                        # id (uniq text) - номер дела
                        # year (int) - год подачи
                        # number (int) - номер досара
                        # depun (date) - дата подачи
                        # solutie (date) - дата решения
                        # ordin (text) - номер приказа
                        # result (int) - результат, true - приказ, false - отказ, null - ещё неизвестно
                        # termen (date) - дата последнего термена
                        # suplimentar (int) - флаг дозапроса, true - по делу были дозапросы, по умолчанию false - данных по дозапросу нет, считаем, что не было.

                        if ORDIN:
                            # Если есть решение, тогда есть id, год, номер, дата подачи, дата решения, номер приказа.
                            # Помечаем результат как неуспешный result=False. Корректировка результата будет на более поздних этапах: по данным приказов и по поиску неуникальных приказов.
                            # Если такой ID уже есть, то вносим данные приказа (возможно, повторно вносим)
                            # Даты Termen нет, поэтому её не вносим, остаётся старой
                            pass
                            if SOLUTIE:
                                db.execute( 'INSERT INTO Dosar (id, year, number, depun, solutie, ordin, result) VALUES (?, ?, ?, ?, ?, ?, ?) '
                                            'ON CONFLICT(id) DO UPDATE SET solutie=?, ordin=?, result=?',
                                            (ID, YEAR, NUMBER, DEPUN, SOLUTIE, ORDIN, False,
                                            SOLUTIE, ORDIN, False)
                                          )
                                SQLlogger.info('Modified: ' + str(db.rowcount))
                            else:
                                db.execute( 'INSERT INTO Dosar (id, year, number, depun, ordin, result) VALUES (?, ?, ?, ?, ?, ?) '
                                            'ON CONFLICT(id) DO UPDATE SET ordin=?, result=?',
                                            (ID, YEAR, NUMBER, DEPUN, ORDIN, False,
                                            ORDIN, False)
                                          )
                                SQLlogger.info('Modified: ' + str(db.rowcount))
                        else:
                            # Если решения нет, то id, год, номер, дата подачи и, возможно, дата термена:
                            # 1) Добавляем в таблицу Dosar новые данные
                            # 2) Если такой ID в таблице есть, то апдейтим термен
                            # 3) Добавляем в таблицу с терменами новую запись по термену, если термен указан. Если такая же пара ID+termen существует, то данные не внесутся.
                            if TERMEN:
                                pass
                                db.execute( 'INSERT INTO Dosar (id, year, number, depun, termen) VALUES (?, ?, ?, ?, ?) '
                                            'ON CONFLICT(id) DO UPDATE SET termen=excluded.termen WHERE termen<excluded.termen OR termen IS NULL', 
                                            (ID, YEAR, NUMBER, DEPUN, TERMEN)
                                          )
                                SQLlogger.info('Modified: ' + str(db.rowcount))
                                db.execute( 'INSERT OR IGNORE INTO Termen (id, termen, stadiu) VALUES (?, ?, ?)',
                                            (ID, TERMEN, STADIUPUBDATE)
                                          )
                                SQLlogger.info('Modified: ' + str(db.rowcount))
                            else:
                            # Если термен не указан, то просто добавляем в таблицу Dosar данные о новом деле
                                pass
                                db.execute( 'INSERT OR IGNORE INTO Dosar (id, year, number, depun) VALUES (?, ?, ?, ?)',
                                            (ID, YEAR, NUMBER, DEPUN)
                                          )
                                SQLlogger.info('Modified: ' + str(db.rowcount))
    connection.commit()

# Помечаем дела с неуникальным номером приказа как положительный результат, result=true
db.execute( 'UPDATE Dosar SET result=True WHERE result IS False AND ordin IN (SELECT ordin FROM Dosar GROUP BY ordin HAVING COUNT(*) > 1)' )
SQLlogger.info('Modified: ' + str(db.rowcount))
# Помечаем дела, для которых изменялся термен, как дела с дозапросом suplimentar=true
db.execute( 'UPDATE Dosar SET suplimentar=True WHERE id IN (SELECT id FROM Termen GROUP BY id HAVING COUNT(*) > 1)' )
SQLlogger.info('Modified: ' + str(db.rowcount))
# Помечаем дела, для которых термен отстоит от даты подачи больше, чем на 365 дней, как дела с дозапросом suplimentar=true
db.execute( 'UPDATE Dosar SET suplimentar=True WHERE (JULIANDAY(Termen)-JULIANDAY(depun))>365' )
SQLlogger.info('Modified: ' + str(db.rowcount))

connection.commit()
connection.close()

quit()
