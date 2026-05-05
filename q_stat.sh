#!/bin/bash

sqlite3 -echo -box data.db '
SELECT 
    depun,
    COUNT(*) as total_dosare,
    COUNT(CASE WHEN solutie IS NOT NULL THEN 1 END) as cu_solutie,
    COUNT(CASE WHEN ordin IS NOT NULL THEN 1 END) as cu_ordin,
    COUNT(CASE WHEN anexa IS NOT NULL THEN 1 END) as cu_anexa,
    COUNT(CASE WHEN cminori IS NOT NULL THEN 1 END) as cu_cminori,
    COUNT(CASE WHEN result = 1 THEN 1 END) as cu_result,
    COUNT(CASE WHEN juramat IS NOT NULL THEN 1 END) as cu_juramat,
    ROUND(COUNT(CASE WHEN solutie IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as procent_solutie,
    ROUND(COUNT(CASE WHEN result = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as procent_result
FROM Dosar11 
WHERE depun = "2023-12-19"
GROUP BY depun
ORDER BY depun;'


sqlite3 -echo -box data.db '
SELECT 
    'solutie' as field_name,
    COUNT(CASE WHEN solutie IS NOT NULL THEN 1 END) as filled,
    COUNT(*) as total,
    ROUND(COUNT(CASE WHEN solutie IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as procent
FROM Dosar11 
WHERE depun = "2023-12-19"

UNION ALL

SELECT 
    'ordin' as field_name,
    COUNT(CASE WHEN ordin IS NOT NULL THEN 1 END) as filled,
    COUNT(*) as total,
    ROUND(COUNT(CASE WHEN ordin IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as procent
FROM Dosar11 
WHERE depun = "2023-12-19"

UNION ALL

SELECT 
    'anexa' as field_name,
    COUNT(CASE WHEN anexa IS NOT NULL THEN 1 END) as filled,
    COUNT(*) as total,
    ROUND(COUNT(CASE WHEN anexa IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as procent
FROM Dosar11 
WHERE depun = "2023-12-19"

UNION ALL

SELECT 
    'cminori' as field_name,
    COUNT(CASE WHEN cminori IS NOT NULL THEN 1 END) as filled,
    COUNT(*) as total,
    ROUND(COUNT(CASE WHEN cminori IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as procent
FROM Dosar11 
WHERE depun = "2023-12-19"

UNION ALL

SELECT 
    'result' as field_name,
    COUNT(CASE WHEN result = 1 THEN 1 END) as filled,
    COUNT(*) as total,
    ROUND(COUNT(CASE WHEN result = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as procent
FROM Dosar11 
WHERE depun = "2023-12-19"

UNION ALL

SELECT 
    'juramat' as field_name,
    COUNT(CASE WHEN juramat IS NOT NULL THEN 1 END) as filled,
    COUNT(*) as total,
    ROUND(COUNT(CASE WHEN juramat IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as procent
FROM Dosar11 
WHERE depun = "2023-12-19";
'

sqlite3 -echo -box data.db '
SELECT 
    id,
    depun,
    CASE WHEN solutie IS NOT NULL THEN '✓' ELSE '✗' END as solutie,
    CASE WHEN ordin IS NOT NULL THEN '✓' ELSE '✗' END as ordin,
    CASE WHEN anexa IS NOT NULL THEN '✓' ELSE '✗' END as anexa,
    CASE WHEN cminori IS NOT NULL THEN '✓' ELSE '✗' END as cminori,
    CASE WHEN result = 1 THEN '✓' ELSE '✗' END as result,
    CASE WHEN juramat IS NOT NULL THEN '✓' ELSE '✗' END as juramat,
    termen
FROM Dosar11 
WHERE depun = "2023-12-19"
ORDER BY id;
'

sqlite3 -echo -box data.db '
SELECT 
    depun,
    COUNT(*) as total_dosare,
    COUNT(CASE WHEN solutie IS NOT NULL THEN 1 END) as cu_solutie,
    COUNT(CASE WHEN ordin IS NOT NULL THEN 1 END) as cu_ordin,
    COUNT(CASE WHEN result = 1 THEN 1 END) as cu_result,
    ROUND(COUNT(CASE WHEN result = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as procent_complet
FROM Dosar11 
WHERE depun = "2023-12-19"
GROUP BY depun
ORDER BY depun DESC
LIMIT 20;
'

sqlite3 -echo -box data.db '
SELECT 
    id,
    depun,
    termen,
    CASE 
        WHEN solutie IS NULL AND ordin IS NULL THEN 'Нет решения'
        WHEN solutie IS NOT NULL AND ordin IS NULL THEN 'Есть решение, нет приказа'
        WHEN solutie IS NULL AND ordin IS NOT NULL THEN 'Есть приказ, нет решения'
        ELSE 'Обработано'
    END as status
FROM Dosar11 
WHERE depun = "2023-12-19"
AND (solutie IS NULL OR ordin IS NULL OR result != 1)
ORDER BY id;
'


sqlite3 -echo -box data.db 'SELECT *FROM Termen11 WHERE id = "10051/RD/2025" ORDER BY id;'

