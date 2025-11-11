-- Статистика по вашим пачкам (только дела с правильным годом)
WITH your_cases AS (
  SELECT id, depun, year, '+++++' as marker
  FROM Dosar11
  WHERE id IN ('48275/RD/2023', '49000/RD/2023')
),

batch_stats AS (
  SELECT 
    d.depun,
    d.year,
    COUNT(*) as total_in_batch,
    COUNT(d.solutie) as with_solution,
    COUNT(d.ordin) as with_ordin,
    COUNT(CASE WHEN d.result = 1 THEN 1 END) as approved,
    COUNT(CASE WHEN d.refuz > 0 THEN 1 END) as with_refuz_flag,
    MIN(d.solutie) as first_solution,
    MAX(d.solutie) as last_solution
  FROM Dosar11 d
  WHERE (d.depun, d.year) IN (
    SELECT depun, year FROM your_cases
  )
  GROUP BY d.depun, d.year
),

refusals AS (
  SELECT 
    d.depun,
    d.year,
    COUNT(r.id) as refusals_count
  FROM Dosar11 d
  LEFT JOIN Refuz11 r ON d.id = r.id
  WHERE (d.depun, d.year) IN (
    SELECT depun, year FROM your_cases
  )
  GROUP BY d.depun, d.year
)

SELECT 
  yc.id as your_case_id,
  yc.depun as batch_date,
  bs.total_in_batch,
  bs.with_solution,
  bs.with_ordin,
  bs.approved,
  r.refusals_count,
  bs.first_solution,
  bs.last_solution,
  ROUND(CAST(bs.with_solution AS FLOAT) / bs.total_in_batch * 100, 1) as solution_percent,
  ROUND(CAST(bs.approved AS FLOAT) / bs.total_in_batch * 100, 1) as approved_percent
FROM your_cases yc
JOIN batch_stats bs ON yc.depun = bs.depun AND yc.year = bs.year
LEFT JOIN refusals r ON yc.depun = r.depun AND yc.year = r.year
ORDER BY yc.depun;
