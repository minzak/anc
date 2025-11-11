-- Детальный список всех дел из ваших пачек (только с правильным годом в номере)
SELECT 
  d.id,
  d.year,
  d.number,
  d.depun,
  d.solutie,
  d.ordin,
  d.result,
  d.refuz,
  d.termen,
  CASE 
    WHEN d.id IN ('48275/RD/2023', '49000/RD/2023') THEN '++++++'
    ELSE ''
  END as marker,
  CASE
    WHEN d.solutie IS NOT NULL THEN 'Решено'
    WHEN d.refuz > 0 THEN 'Отказ'
    WHEN d.result = 1 THEN 'Одобрено'
    ELSE 'В обработке'
  END as status
FROM Dosar11 d
WHERE (d.depun = '2023-12-14' AND d.year = 2023)
   OR (d.depun = '2023-12-19' AND d.year = 2023)
ORDER BY d.depun, d.number;
