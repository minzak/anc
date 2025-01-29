#!/usr/bin/python3

import os
import requests
import re
import sqlite3
from datetime import datetime
import fitz  # install using pip install PyMuPDF

Stadiu = './stadiu/'
Database = './data.db'

# Функция проверки текстовой строки на валидность.
# Должна быть либо в формате ДД.ММ.ГГГГ, либо None
# Если формат не соответствует, выбрасывается ошибка ValueError

def vali_date(date_text):
    try:
        if date_text:
            datetime.strptime(date_text, '%d.%m.%Y').date()
        return True
    except ValueError:
        return False

# Подключаемся к базе данных SQLite
connection = sqlite3.connect(Database)
db = connection.cursor()

# Сканируем папку Stadiu, исключая ненужные файлы
all_files = os.listdir(Stadiu)
exclude_files = {'.gitkeep'}
dirs = [item for item in all_files if item not in exclude_files and os.path.isdir(os.path.join(Stadiu, item))]

print('Silent parsing directory:')
print(sorted(dirs))

# Подсчитываем общее количество PDF-файлов во всех каталогах
# Это необходимо для отображения прогресса обработки

total_files = 0
for directory in sorted(dirs):
    total_files += len([filename for filename in os.listdir(os.path.join(Stadiu, directory)) if filename.endswith(('.pdf', '.PDF'))])

# Переменная для отслеживания текущего индекса файла
current_index = 0

# Функция для слияния разорванных частей строки

def merge_parts(parts):
    return ''.join(str(part).strip() for part in parts if part)

# Функция для корректировки ID с пробелами или некорректными частями

def fix_id_parts(line):
    if len(line) > 1 and (line[0].strip().endswith('/') or line[1].startswith('/RD')):
        line[0] = merge_parts(line[:2])
        return line[:1] + line[2:]
    return line

# Обрабатываем каждый каталог и каждый PDF в нем
for directory in sorted(dirs):
    # Преобразуем название папки в дату публикации
    STADIUPUBDATE = datetime.strptime(directory, '%Y-%m-%d').date()
    for filename in sorted(os.listdir(os.path.join(Stadiu, directory))):
        # Пропускаем файлы, которые не являются PDF
        if not filename.endswith(('.pdf', '.PDF')):
            continue

        # Увеличиваем индекс текущего файла и выводим прогресс
        current_index += 1
        print(f'parsing file [{current_index}/{total_files}]: {Stadiu}{directory}/{filename}')

        # Открываем PDF-документ с использованием библиотеки PyMuPDF
        with fitz.open(os.path.join(Stadiu, directory, filename)) as doc:
            for page in doc:
                # Ищем таблицы на странице с помощью PyMuPDF
                tabs = page.find_tables(vertical_strategy='text', min_words_vertical=2, horizontal_strategy='text')
                if len(tabs.tables) < 1:
                    print(f'FAIL: На одной из страниц файла {filename} не найдено таблиц')
                    continue
                tab = tabs[0]

                # Извлекаем строки из таблицы и обрабатываем их
                for line in tab.extract():
                    try:
                        # Исправляем ID с пробелами (например, ['10', '/RD/2012'])
                        line = fix_id_parts(line)

                        # Проверяем разрыв полей данных (например, ['67192/RD/2010', '27.10.2010', '', '8', '25/2011', '10.08.2011'])
                        if len(line) > 5:
                            line[2] = merge_parts(line[2:4])
                            line[3] = merge_parts(line[4:6])
                            line = line[:4]

                        ID = line[0]
                        if ID and ID[0].isdigit():
                            # Обработка строки с учетом различных форматов данных
                            size_line = len(line)
                            filtered_line = list(filter(None, line))
                            size_filtered_line = len(filtered_line)

                            DEPUN, TERMEN, ORDIN, SOLUTIE = None, None, None, None

                            if size_line == 5:
                            # Строка распозналась корректно, все 5 полей
                                DEPUN = line[1]
                                TERMEN = line[2]
                                ORDIN = line[3]
                                SOLUTIE = line[4]
                            elif size_filtered_line == 4:
                            # 4 поля, считаем, что это id, дата досара, номер приказа и дата решения
                                DEPUN = filtered_line[1]
                                ORDIN = filtered_line[2]
                                SOLUTIE = filtered_line[3]
                            elif size_filtered_line == 3:
                                # Хак для старых стадиу, где дата решения зашита в номер приказа
                                # В старых досар могут быть такие комбинации:
                                #   id, дата досара, номер приказа (с датой)
                                #   id, дата досара, термен - этот вариант в другой ветке if
                                DEPUN = filtered_line[1]
                                if '/' in filtered_line[2]:
                                    ORDIN = filtered_line[2]
                                    SOLUTIE = filtered_line[2].split('/')[-1] if vali_date(filtered_line[2].split('/')[-1]) else None
                                else:
                                    TERMEN = filtered_line[2]
                            elif size_filtered_line == 2:
                            # 2 поля, считаем, что это id и дата досара
                                DEPUN = filtered_line[1]
                            else:
                                raise ValueError(f'Unexpected line format: {line}')

                            # Обработка случая, когда DEPUN содержит две даты через пробел или некорректные части
                            if DEPUN:
                                if ' ' in DEPUN:
                                    parts = DEPUN.split(' ')
                                    valid_dates = [part for part in parts if vali_date(part)]
                                    if len(valid_dates) == 2:
                                        DEPUN, TERMEN = valid_dates
                                    elif len(valid_dates) == 1:
                                        DEPUN = valid_dates[0]
                                    else:
                                        raise ValueError(f'Invalid dual date format in DEPUN: {DEPUN}')
                                elif not vali_date(DEPUN):
                                    raise ValueError(f'Invalid date in DEPUN: {DEPUN}')

                            # Проверяем валидность дат
                            if DEPUN and vali_date(DEPUN):
                                DEPUN = datetime.strptime(DEPUN, '%d.%m.%Y').date()
                            else:
                                print(f'FAIL: Невалидная дата DEPUN в строке: {line}')
                                continue

                            if TERMEN and vali_date(TERMEN):
                                TERMEN = datetime.strptime(TERMEN, '%d.%m.%Y').date()
                            else:
                                TERMEN = None

                            if SOLUTIE and vali_date(SOLUTIE):
                                SOLUTIE = datetime.strptime(SOLUTIE, '%d.%m.%Y').date()
                            else:
                                SOLUTIE = None

                            YEAR = int(ID.split("/")[-1])
                            NUMBER = int(ID.split("/")[0])

                            # Часто косячат с годом подачи дела. В таком случае год подачи берем из порядкового номера
                            # Пример: дело 1234/RD/2019 подано в 2023 году. Тогда меняем год в дате и ставим год=2019
                            if DEPUN and YEAR and DEPUN.year != YEAR:
                                DEPUN = DEPUN.replace(year=YEAR)

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

                            # Вставляем данные в базу
                            if ORDIN:
                            # Если есть решение, тогда есть id, год, номер, дата подачи, дата решения, номер приказа.
                            # Помечаем результат как неуспешный result=False. Корректировка результата будет на более поздних этапах: по данным приказов и по поиску неуникальных приказов.
                            # Если такой ID уже есть, то вносим данные приказа (возможно, повторно вносим)
                            # Даты Termen нет, поэтому её не вносим, остаётся старой
                                db.execute(
                                    'INSERT INTO Dosar (id, year, number, depun, solutie, ordin, result) VALUES (?, ?, ?, ?, ?, ?, ?) '
                                    'ON CONFLICT(id) DO UPDATE SET solutie=excluded.solutie, ordin=excluded.ordin, result=excluded.result',
                                    (ID, YEAR, NUMBER, DEPUN, SOLUTIE, ORDIN, False)
                                )
                            elif TERMEN:
                                # Если решения нет, то id, год, номер, дата подачи и, возможно, дата термена:
                                # 1) Добавляем в таблицу Dosar новые данные
                                # 2) Если такой ID в таблице есть, то апдейтим термен
                                # 3) Добавляем в таблицу с терменами новую запись по термену, если термен указан. Если такая же пара ID+termen существует, то данные не внесутся.
                                db.execute(
                                    'INSERT INTO Dosar (id, year, number, depun, termen) VALUES (?, ?, ?, ?, ?) '
                                    'ON CONFLICT(id) DO UPDATE SET termen=excluded.termen WHERE termen<excluded.termen OR termen IS NULL',
                                    (ID, YEAR, NUMBER, DEPUN, TERMEN)
                                )
                                db.execute(
                                    'INSERT OR IGNORE INTO Termen (id, termen, stadiu) VALUES (?, ?, ?)',
                                    (ID, TERMEN, STADIUPUBDATE)
                                )
                            else:
                                # Если термен не указан, то просто добавляем в таблицу Dosar данные о новом деле
                                db.execute(
                                    'INSERT OR IGNORE INTO Dosar (id, year, number, depun) VALUES (?, ?, ?, ?)',
                                    (ID, YEAR, NUMBER, DEPUN)
                                )
                    except Exception as e:
                        print(f'FAIL: Ошибка обработки строки {line}: {e}')
# Сохраняем изменения в базе данных
connection.commit()

# Обновляем данные в таблицах для расчета дополнительных полей
# Обновление статуса "result" для дел с неуникальными приказами
db.execute(    'UPDATE Dosar SET result=True WHERE result IS False AND ordin IN (SELECT ordin FROM Dosar GROUP BY ordin HAVING COUNT(*) > 1)')

# Обновление поля "suplimentar" для дел с несколькими терменами
db.execute(    'UPDATE Dosar SET suplimentar=True WHERE id IN (SELECT id FROM Termen GROUP BY id HAVING COUNT(*) > 1)')

# Обновление поля "suplimentar" для дел с терменами более чем на год после подачи
db.execute(    'UPDATE Dosar SET suplimentar=True WHERE (JULIANDAY(Termen)-JULIANDAY(depun))>365')

# Сохраняем и закрываем соединение с базой данных
connection.commit()
connection.close()

quit()
