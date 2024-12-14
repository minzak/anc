CREATE TABLE Dosar(id TEXT NOT NULL PRIMARY KEY, year INTEGER NOT NULL, number INTEGER NOT NULL, depun DATE DEFAULT NULL, solutie DATE DEFAULT NULL, ordin TEXT DEFAULT NULL, result INTEGER DEFAULT NULL, termen DATE DEFAULT NULL, suplimentar INTEGER DEFAULT False, juramat DATE DEFAULT NULL);
CREATE TABLE Termen( id TEXT, termen DATE, stadiu DATE, UNIQUE(id, termen) );
