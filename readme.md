# Install

Используйте sqlite3 совместимую с `box` параметром.
```
$ sqlite3 --version
3.34.1 2021-01-20 14:10:07 10e20c0b43500cfb9bbc0eaa061c57514f715d87238f4d835880cd846b9ealt1
```
Иначе не используйте его в праметрах запросов.

# Структура БД:

Таблица Dosar
- id TEXT - номер дела в формате <номер>/RD/<год>
- year INTEGER - год подачи
- number INTEGER - номер дела
- depun DATE - дата подачи документов
- solutie DATE DEFAULT NULL - дата решения по делу, или NULL, если решение ещё не принято
- ordin TEXT - номер приказа о гражданстве, по которому принято решение
- result INTEGER DEFAULT NULL - результат рассмотрения дела. 0/False == отказ, 1/True == положительное решение, NULL - решение ещё не принято
- termen DATE DEFAULT NULL - последняя известная дата проведения комиссии по делу
- suplimentar INTEGER DEFAULT False - был ли дозапрос документов по данному делу

Таблица Termen
 - id TEXT - номер дела в формате <номер>/RD/<год>
 - termen DATE - назначенная дата рассмотрения дела.
 - stadiu DATE - дата stadiu, в котором изменился Termen

Эта таблица создаётся с правилом UNIQUE(id, termen), которое принудительно сохраняет только уникальные пары id+termen.

# Структура

Структура каталогов и PDF файлы, список находится в `tree.txt` доступен через www на приватном сайте, доступ к которому по IP списку.

# Результаты

Пример запросов в `q.sh` файле или вот пример:



```
SELECT * FROM Dosar WHERE result=True AND year=2023;
┌───────────────┬──────┬────────┬────────────┬────────────┬─────────────┬────────┬────────────┬─────────────┐
│      id       │ year │ number │   depun    │  solutie   │    ordin    │ result │   termen   │ suplimentar │
├───────────────┼──────┼────────┼────────────┼────────────┼─────────────┼────────┼────────────┼─────────────┤
│ 13071/RD/2023 │ 2023 │ 13071  │ 2023-04-10 │ 2023-09-29 │ 1619/P/2023 │ 1      │ 2023-07-20 │ 0           │
└───────────────┴──────┴────────┴────────────┴────────────┴─────────────┴────────┴────────────┴─────────────┘
```
```
SELECT * from Dosar where id="48275/RD/2023";
┌───────────────┬──────┬────────┬────────────┬─────────┬───────┬────────┬────────────┬─────────────┐
│      id       │ year │ number │   depun    │ solutie │ ordin │ result │   termen   │ suplimentar │
├───────────────┼──────┼────────┼────────────┼─────────┼───────┼────────┼────────────┼─────────────┤
│ 48275/RD/2023 │ 2023 │ 48275  │ 2023-12-14 │         │       │        │ 2024-03-21 │ 0           │
└───────────────┴──────┴────────┴────────────┴─────────┴───────┴────────┴────────────┴─────────────┘
```
```
SELECT * from Dosar where id="49000/RD/2023";
┌───────────────┬──────┬────────┬────────────┬─────────┬───────┬────────┬────────────┬─────────────┐
│      id       │ year │ number │   depun    │ solutie │ ordin │ result │   termen   │ suplimentar │
├───────────────┼──────┼────────┼────────────┼─────────┼───────┼────────┼────────────┼─────────────┤
│ 49000/RD/2023 │ 2023 │ 49000  │ 2023-12-19 │         │       │        │ 2024-04-26 │ 0           │
└───────────────┴──────┴────────┴────────────┴─────────┴───────┴────────┴────────────┴─────────────┘
```
```
SELECT * from Dosar where id="14833/RD/2024";
┌───────────────┬──────┬────────┬────────────┬─────────┬───────┬────────┬────────┬─────────────┐
│      id       │ year │ number │   depun    │ solutie │ ordin │ result │ termen │ suplimentar │
├───────────────┼──────┼────────┼────────────┼─────────┼───────┼────────┼────────┼─────────────┤
│ 14833/RD/2024 │ 2024 │ 14833  │ 2024-03-26 │         │       │        │        │ 0           │
└───────────────┴──────┴────────┴────────────┴─────────┴───────┴────────┴────────┴─────────────┘
```
```
SELECT * from Dosar where id="38286/RD/2021";
┌───────────────┬──────┬────────┬────────────┬─────────┬───────┬────────┬────────────┬─────────────┐
│      id       │ year │ number │   depun    │ solutie │ ordin │ result │   termen   │ suplimentar │
├───────────────┼──────┼────────┼────────────┼─────────┼───────┼────────┼────────────┼─────────────┤
│ 38286/RD/2021 │ 2021 │ 38286  │ 2021-11-03 │         │       │        │ 2021-02-17 │ 0           │
└───────────────┴──────┴────────┴────────────┴─────────┴───────┴────────┴────────────┴─────────────┘
```

Статистика решений за прошедший КВАРТАЛ по месяцу подачи:
```
┌──────────────┬──────┬────────┬─────────┬─────────────────────┬────────────────────┬──────────────────────────┬─────────────────────────┐
│ Месяц подачи │ Дел  │ Решено │ Решений │ Приказов в решениях │ Отказов в решениях │ Приказов после дозапроса │ Отказов после дозапроса │
├──────────────┼──────┼────────┼─────────┼─────────────────────┼────────────────────┼──────────────────────────┼─────────────────────────┤
│ 2019-01      │ 8152 │ 84.9 % │ 16      │ 16                  │ 0                  │ 16                       │ 0                       │
│ 2019-02      │ 8579 │ 83.0 % │ 17      │ 14                  │ 3                  │ 14                       │ 3                       │
│ 2019-03      │ 9221 │ 85.3 % │ 22      │ 21                  │ 1                  │ 21                       │ 1                       │
│ 2019-04      │ 7595 │ 82.9 % │ 16      │ 16                  │ 0                  │ 16                       │ 0                       │
│ 2019-05      │ 8398 │ 81.2 % │ 36      │ 33                  │ 3                  │ 32                       │ 1                       │
│ 2019-06      │ 7295 │ 80.6 % │ 64      │ 51                  │ 13                 │ 10                       │ 1                       │
│ 2019-07      │ 8242 │ 77.7 % │ 181     │ 156                 │ 25                 │ 24                       │ 3                       │
│ 2019-08      │ 7268 │ 72.6 % │ 56      │ 53                  │ 3                  │ 7                        │ 0                       │
│ 2019-09      │ 7683 │ 72.0 % │ 27      │ 27                  │ 0                  │ 26                       │ 0                       │
│ 2019-10      │ 8849 │ 70.4 % │ 38      │ 22                  │ 16                 │ 12                       │ 1                       │
│ 2019-11      │ 7836 │ 74.5 % │ 104     │ 88                  │ 16                 │ 18                       │ 4                       │
│ 2019-12      │ 7075 │ 70.4 % │ 117     │ 117                 │ 0                  │ 40                       │ 0                       │
│ 2020-01      │ 7604 │ 63.8 % │ 615     │ 609                 │ 6                  │ 31                       │ 4                       │
│ 2020-02      │ 6836 │ 55.7 % │ 469     │ 469                 │ 0                  │ 22                       │ 0                       │
│ 2020-03      │ 2836 │ 40.8 % │ 70      │ 70                  │ 0                  │ 4                        │ 0                       │
│ 2020-04      │ 154  │ 53.9 % │         │                     │                    │                          │                         │
│ 2020-05      │ 30   │ 13.3 % │         │                     │                    │                          │                         │
│ 2020-06      │ 259  │ 72.6 % │         │                     │                    │                          │                         │
│ 2020-07      │ 79   │ 48.1 % │         │                     │                    │                          │                         │
│ 2020-08      │ 147  │ 22.4 % │         │                     │                    │                          │                         │
│ 2020-09      │ 347  │ 13.0 % │ 1       │ 1                   │ 0                  │ 0                        │ 0                       │
│ 2020-10      │ 459  │ 47.7 % │         │                     │                    │                          │                         │
│ 2020-11      │ 837  │ 32.1 % │ 2       │ 1                   │ 1                  │ 0                        │ 1                       │
│ 2020-12      │ 1299 │ 55.3 % │ 61      │ 60                  │ 1                  │ 3                        │ 1                       │
│ 2021-01      │ 442  │ 31.2 % │ 1       │ 1                   │ 0                  │ 0                        │ 0                       │
│ 2021-02      │ 1177 │ 26.8 % │ 12      │ 11                  │ 1                  │ 0                        │ 1                       │
│ 2021-03      │ 1121 │ 32.4 % │ 28      │ 28                  │ 0                  │ 0                        │ 0                       │
│ 2021-04      │ 1306 │ 39.8 % │ 116     │ 115                 │ 1                  │ 1                        │ 1                       │
│ 2021-05      │ 1658 │ 38.6 % │ 31      │ 28                  │ 3                  │ 1                        │ 2                       │
│ 2021-06      │ 5078 │ 41.4 % │ 302     │ 293                 │ 9                  │ 0                        │ 7                       │
│ 2021-07      │ 5706 │ 18.8 % │ 28      │ 24                  │ 4                  │ 1                        │ 4                       │
│ 2021-08      │ 5762 │ 2.8 %  │ 58      │ 58                  │ 0                  │ 0                        │ 0                       │
│ 2021-09      │ 5001 │ 1.8 %  │ 4       │ 3                   │ 1                  │ 1                        │ 1                       │
│ 2021-10      │ 4682 │ 1.5 %  │ 4       │ 4                   │ 0                  │ 0                        │ 0                       │
│ 2021-11      │ 4666 │ 1.8 %  │ 7       │ 6                   │ 1                  │ 0                        │ 1                       │
│ 2021-12      │ 2116 │ 2.5 %  │ 7       │ 6                   │ 1                  │ 0                        │ 0                       │
│ 2022-01      │ 1935 │ 2.3 %  │ 3       │ 2                   │ 1                  │ 0                        │ 0                       │
│ 2022-02      │ 2531 │ 1.8 %  │ 6       │ 5                   │ 1                  │ 0                        │ 0                       │
│ 2022-03      │ 2639 │ 1.4 %  │ 8       │ 5                   │ 3                  │ 0                        │ 0                       │
│ 2022-04      │ 1661 │ 1.4 %  │ 4       │ 4                   │ 0                  │ 0                        │ 0                       │
│ 2022-05      │ 1858 │ 2.0 %  │ 9       │ 7                   │ 2                  │ 0                        │ 0                       │
│ 2022-06      │ 2412 │ 1.3 %  │ 5       │ 5                   │ 0                  │ 0                        │ 0                       │
│ 2022-07      │ 3056 │ 1.0 %  │ 10      │ 10                  │ 0                  │ 0                        │ 0                       │
│ 2022-08      │ 3244 │ 0.8 %  │ 12      │ 11                  │ 1                  │ 0                        │ 0                       │
│ 2022-09      │ 3206 │ 0.6 %  │ 6       │ 5                   │ 1                  │ 0                        │ 0                       │
│ 2022-10      │ 3363 │ 0.1 %  │ 3       │ 3                   │ 0                  │ 0                        │ 0                       │
│ 2022-11      │ 3512 │ 0.1 %  │ 3       │ 2                   │ 1                  │ 0                        │ 0                       │
│ 2022-12      │ 1936 │ 0.1 %  │         │                     │                    │                          │                         │
│ 2023-01      │ 2682 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2023-02      │ 3258 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2023-03      │ 3535 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2023-04      │ 2475 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2023-05      │ 3017 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2023-06      │ 2724 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2023-07      │ 3452 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2023-08      │ 3404 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2023-09      │ 3516 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2023-10      │ 3550 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2023-11      │ 3921 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2023-12      │ 3441 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2024-01      │ 3855 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2024-02      │ 4152 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2024-03      │ 4195 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2024-04      │ 3264 │ 0.0 %  │         │                     │                    │                          │                         │
│ 2024-11      │ 2    │ 0.0 %  │         │                     │                    │                          │                         │
│ 2024-12      │ 60   │ 0.0 %  │         │                     │                    │                          │                         │
└──────────────┴──────┴────────┴─────────┴─────────────────────┴────────────────────┴──────────────────────────┴─────────────────────────┘
```
