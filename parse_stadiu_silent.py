#!/usr/bin/python3

import os
import requests
import re
import sqlite3
from datetime import datetime
import fitz # install using pip install PyMuPDF

Stadiu = './stadiu/'
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

connection = sqlite3.connect(Database)
db = connection.cursor()

# List all items in the directory
all_files = os.listdir(Stadiu)
# Define files to exclude (for example, hidden files starting with a dot) 
# exclude_files = {'.gitkeep', 'another_file_to_exclude.txt'}
exclude_files = {'.gitkeep'}

# Use list comprehension to filter out excluded files
dirs = [item for item in all_files if item not in exclude_files and os.path.isdir(os.path.join(Stadiu, item))]
print('Silent parsing directory:')
print(sorted(dirs))

#for directory in sorted(os.listdir(Stadiu)):
for directory in sorted(dirs):
    # Сохраняем дату публикации стадиу для таблицы с терменами
    STADIUPUBDATE = datetime.strptime(directory, '%Y-%m-%d').date()
    # Цикл по всем pdf в директории
    for filename in os.listdir(Stadiu+directory):
        if not filename.endswith(('.pdf','.PDF')):
            continue
        print('parsing file: ' + Stadiu + directory + '/' + filename)
        with fitz.open(Stadiu+directory+'/'+filename) as doc:
            for page in doc:
                tabs = page.find_tables(vertical_strategy='text', min_words_vertical=2, horizontal_strategy='text')
                if len(tabs.tables)<1:
                    print('FAIL: На одной из страниц файла ' + filename + ', после досара ' + ID + 'не найдено таблиц')
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
                            DEPUN   = line[1]
                            TERMEN  = line[2]
                            ORDIN   = line[3]
                            SOLUTIE = line[4]
                        elif size_filtered_line == 4:
                            # 4 поля, считаем, что это id, дата досара, номер приказа и дата решения
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
                                DEPUN   = line[1]
                                TERMEN  = None
                                ORDIN   = string
                                SOLUTIE = string.split('/')[-1]
                            elif '/' in string and not vali_date(string.split('/')[-1]):
                                # Хак для старых стадиу, где для досара указан только номер приказа без даты
                                # Для таких дел не будет даты решения, только номер приказа
                                DEPUN   = line[1]
                                TERMEN  = None
                                ORDIN   = string
                                # Последняя попытка вытащить дату решения парсингом регулярками всего текста страницы
                                regex_result = re.search(ORDIN+'\n(\d\d.\d\d.\d\d\d\d)', page.get_text(), re.MULTILINE)
                                SOLUTIE = None if not regex_result else regex_result.group(1)
                            else:
                                # 3 поля, считаем, что это id, дата досара, термен
                                DEPUN   = filtered_line[1]
                                TERMEN  = filtered_line[2]
                                ORDIN   = None
                                SOLUTIE = None
                        elif size_filtered_line == 2:
                            # 2 поля, считаем, что это id и дата досара
                            DEPUN   = filtered_line[1]
                            TERMEN  = None
                            ORDIN   = None
                            SOLUTIE = None
                        else:
                            print('FAIL: Ошибка распознавания строки [ ' + '; '.join(line) + ' ]')
                            continue
                        # Валидация сканированных данных и пропуск, если данные некорректны
                        if not vali_date(DEPUN) or not vali_date(TERMEN) or not vali_date(SOLUTIE):
                            print('FAIL: Ошибка распознавания строки [ ' + '; '.join(line) + ' ]')
                            continue
                        else:
                            pass
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
                            else:
                                db.execute( 'INSERT INTO Dosar (id, year, number, depun, ordin, result) VALUES (?, ?, ?, ?, ?, ?) '
                                            'ON CONFLICT(id) DO UPDATE SET ordin=?, result=?',
                                            (ID, YEAR, NUMBER, DEPUN, ORDIN, False,
                                            ORDIN, False)
                                          )
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
                                db.execute( 'INSERT OR IGNORE INTO Termen (id, termen, stadiu) VALUES (?, ?, ?)',
                                            (ID, TERMEN, STADIUPUBDATE)
                                          )
                            else:
                            # Если термен не указан, то просто добавляем в таблицу Dosar данные о новом деле
                                pass
                                db.execute( 'INSERT OR IGNORE INTO Dosar (id, year, number, depun) VALUES (?, ?, ?, ?)',
                                            (ID, YEAR, NUMBER, DEPUN)
                                          )
    connection.commit()

# Помечаем дела с неуникальным номером приказа как положительный результат, result=true
db.execute( 'UPDATE Dosar SET result=True WHERE result IS False AND ordin IN (SELECT ordin FROM Dosar GROUP BY ordin HAVING COUNT(*) > 1)' )
# Помечаем дела, для которых изменялся термен, как дела с дозапросом suplimentar=true
db.execute( 'UPDATE Dosar SET suplimentar=True WHERE id IN (SELECT id FROM Termen GROUP BY id HAVING COUNT(*) > 1)' )
# Помечаем дела, для которых термен отстоит от даты подачи больше, чем на 365 дней, как дела с дозапросом suplimentar=true
db.execute( 'UPDATE Dosar SET suplimentar=True WHERE (JULIANDAY(Termen)-JULIANDAY(depun))>365' )

connection.commit()
connection.close()

quit()
