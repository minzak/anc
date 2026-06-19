-- ============================================================================
-- Minori11 — example queries. Run e.g.:  sqlite3 -box data.db '.read q_minori.sql'
-- Replace the example dossier number/year/id with your own.
-- ============================================================================

-- 1) DID MY CASE COME OUT? Look up by dossier number + year (segment-agnostic:
--    minors may be listed under /M/, /A/, /RD/ or a bare number/year).
SELECT id, segment, ordin, ordin_date, cminori
FROM Minori11
WHERE number = 48275 AND year = 2023;

-- 2) Exact dossier id (when you know the full form, e.g. a /M/ minor dossier).
SELECT * FROM Minori11 WHERE id = '9791/M/2022';

-- 3) Link your main /RD/ case (Dosar11) to any minori approval by number+year.
--    Shows your case's own status next to the minors' ordinance.
SELECT d.id              AS dosar,
       d.solutie         AS dosar_solutie,
       d.ordin           AS dosar_ordin,
       m.ordin           AS minori_ordin,
       m.ordin_date      AS minori_date,
       m.cminori         AS minors
FROM Dosar11 d
JOIN Minori11 m ON m.number = d.number AND m.year = d.year
WHERE d.id = '48275/RD/2023';

-- 4) Everyone in a given minori ordinance.
SELECT id, segment, cminori
FROM Minori11
WHERE ordin = '1822/P/2020'
ORDER BY number;

-- 5) Most recent minori ordinances with how many dossiers / minors each.
SELECT ordin, ordin_date,
       COUNT(*)        AS dossiers,
       SUM(cminori)    AS minors
FROM Minori11
GROUP BY ordin, ordin_date
ORDER BY ordin_date DESC
LIMIT 20;

-- 6) Minors approved per month (activity trend).
SELECT strftime('%Y-%m', ordin_date) AS year_month,
       COUNT(*)     AS dossiers,
       SUM(cminori) AS minors
FROM Minori11
WHERE ordin_date IS NOT NULL
GROUP BY year_month
ORDER BY year_month DESC;
