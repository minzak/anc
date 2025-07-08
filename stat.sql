SELECT
    Months.DepunMonth                   as 'Месяц подачи',
    Months.Total                        as 'Дел',
--    Months.TotalDes                   as 'Решено дел',
    PRINTF('%1.1f %%', Months.Procent)  as 'Решено',

    Results.Decisions                   as 'Решений',
    Results.Ordins                      as 'Приказов в решениях',
    Results.Rejects                     as 'Отказов в решениях',
    Results.SupOrdins                   as 'Приказов после дозапроса',
    Results.SupRefuzuri                 as 'Отказов после дозапроса'
/*  Вариант запроса, который пригодится, если делать графики - NULL заменяется нулями
    COALESCE(Results.Decisions, 0)      as 'Решений',
    COALESCE(Results.Ordins, 0)         as 'Приказов в решениях',
    COALESCE(Results.Rejects, 0)        as 'Отказов в решениях',
    COALESCE(Results.ReOrdins, 0)       as 'Приказ после дозапроса'
*/

FROM
(
SELECT
    strftime("%Y-%m", depun) as DepunMonth,
    count(*) as Total,
    ROUND(100.0*SUM(CASE WHEN solutie IS NOT NULL THEN 1 ELSE 0 END)/count(*), 1) as Procent
FROM Dosar11
GROUP BY strftime("%Y-%m", depun)
) Months
LEFT JOIN
(
    SELECT strftime("%Y-%m", depun) as DepunMonth,
        SUM(CASE WHEN solutie IS NOT NULL THEN 1 ELSE 0 END) as Decisions,
        SUM(CASE WHEN result=True THEN 1 ELSE 0 END) as Ordins,
        SUM(CASE WHEN result=False THEN 1 ELSE 0 END) as Rejects,
        SUM(CASE WHEN result=True AND suplimentar=True THEN 1 ELSE 0 END) as SupOrdins,
        SUM(CASE WHEN result=False AND suplimentar=True THEN 1 ELSE 0 END) as SupRefuzuri
    FROM Dosar11 WHERE solutie BETWEEN DATE('now', '-1 month') AND DATE('now')
    GROUP BY strftime("%Y-%m", depun)
) Results
ON Months.DepunMonth = Results.DepunMonth
WHERE Months.DepunMonth>='2019-01'
ORDER BY Months.DepunMonth;


SELECT
    Months.DepunMonth                   as 'Месяц подачи',
    Months.Total                        as 'Дел',
--    Months.TotalDes                   as 'Решено дел',
    PRINTF('%1.1f %%', Months.Procent)  as 'Решено',

    Results.Decisions                   as 'Решений',
    Results.Ordins                      as 'Приказов в решениях',
    Results.Rejects                     as 'Отказов в решениях',
    Results.SupOrdins                   as 'Приказов после дозапроса',
    Results.SupRefuzuri                 as 'Отказов после дозапроса'
/*  Вариант запроса, который пригодится, если делать графики - NULL заменяется нулями
    COALESCE(Results.Decisions, 0)      as 'Решений',
    COALESCE(Results.Ordins, 0)         as 'Приказов в решениях',
    COALESCE(Results.Rejects, 0)        as 'Отказов в решениях',
    COALESCE(Results.ReOrdins, 0)       as 'Приказ после дозапроса'
*/
FROM
(
SELECT
    strftime("%Y-%m", depun) as DepunMonth,
    count(*) as Total,
    ROUND(100.0*SUM(CASE WHEN solutie IS NOT NULL THEN 1 ELSE 0 END)/count(*), 1) as Procent
FROM Dosar11
GROUP BY strftime("%Y-%m", depun)
) Months
LEFT JOIN
(
    SELECT strftime("%Y-%m", depun) as DepunMonth,
        SUM(CASE WHEN solutie IS NOT NULL THEN 1 ELSE 0 END) as Decisions,
        SUM(CASE WHEN result=True THEN 1 ELSE 0 END) as Ordins,
        SUM(CASE WHEN result=False THEN 1 ELSE 0 END) as Rejects,
        SUM(CASE WHEN result=True AND suplimentar=True THEN 1 ELSE 0 END) as SupOrdins,
        SUM(CASE WHEN result=False AND suplimentar=True THEN 1 ELSE 0 END) as SupRefuzuri
    FROM Dosar11 WHERE solutie BETWEEN DATE('now', '-3 month') AND DATE('now')
    GROUP BY strftime("%Y-%m", depun)
) Results
ON Months.DepunMonth = Results.DepunMonth
WHERE Months.DepunMonth>='2019-01'
ORDER BY Months.DepunMonth;
