-- Последние изменения в ваших пачках
SELECT 
  d.depun as batch_date,
  COUNT(*) as cases_updated,
  MAX(d.solutie) as latest_solution_date,
  GROUP_CONCAT(DISTINCT d.ordin) as recent_ordins
FROM Dosar11 d
WHERE d.depun IN ('2023-12-14', '2023-12-19')
  AND d.solutie >= date('now', '-120 days')  -- изменения за последние 120 дней
GROUP BY d.depun;
