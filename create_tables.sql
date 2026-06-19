CREATE TABLE Dosar11(id TEXT NOT NULL PRIMARY KEY, year INTEGER NOT NULL, number INTEGER NOT NULL, depun DATE DEFAULT NULL, solutie DATE DEFAULT NULL, ordin TEXT DEFAULT NULL, anexa INTEGER DEFAULT NULL, cminori INTEGER DEFAULT NULL, result INTEGER DEFAULT NULL, termen DATE DEFAULT NULL, suplimentar INTEGER DEFAULT False, juramat DATE DEFAULT NULL, refuz INTEGER DEFAULT 0);
CREATE TABLE Termen11(id TEXT, termen DATE, stadiu DATE, UNIQUE(id, termen) );
CREATE TABLE Refuz11(id TEXT PRIMARY KEY, ordin TEXT NOT NULL, depun DATE, solutie DATE);
CREATE INDEX IF NOT EXISTS idx_refuz11_ordin ON Refuz11(ordin);
CREATE INDEX IF NOT EXISTS idx_dosar11_ordin ON Dosar11(ordin) WHERE ordin IS NOT NULL;

-- Minori: citizenship ordinances for minors. One row per (dossier, ordinance).
-- id is stored exactly as printed (minors have their own /M/ or /A/ numbering;
-- older lists use the parent /RD/ dossier or a bare number/year).
CREATE TABLE IF NOT EXISTS Minori11(
    id TEXT NOT NULL,              -- e.g. 9791/M/2022, 87829/A/2019, 11436/2018
    number INTEGER,
    year INTEGER,
    segment TEXT DEFAULT NULL,     -- M / A / RD / NULL
    ordin TEXT NOT NULL,           -- minori ordinance, e.g. 1822/P/2020
    ordin_date DATE DEFAULT NULL,  -- ordinance date
    cminori INTEGER DEFAULT 1,     -- minors for this dossier in this ordinance
    PRIMARY KEY (id, ordin)
);
CREATE INDEX IF NOT EXISTS idx_minori11_numyear ON Minori11(number, year);
CREATE INDEX IF NOT EXISTS idx_minori11_ordin ON Minori11(ordin);

