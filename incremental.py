#!/usr/bin/env python3
"""Shared helpers for the ANC pipeline.

Two responsibilities:
  1. db_path()/log_dir() -- resolve where the working DB and logs live, so that
     parsers stop hardcoding '/dev/shm' (Linux-only) and instead read the path
     prepared per-OS by lib_db.sh (env ANC_DB / ANC_LOG_DIR).
  2. FileState -- a sidecar-file record of which document files have already been
     parsed into the DB, so each parser can process only new/changed files.

A file is considered "new" if its relative path is absent from the state, or if
its size+mtime changed (and, when enabled, its sha1 differs). Marks are written
to a per-doc-type JSON under state/ and can be flushed after every successful
file so a crash mid-run is recoverable on the next invocation.
"""

import os
import re
import json
import glob
import hashlib
import logging
import sqlite3
import datetime
import unicodedata
from collections import Counter

import sys
sys.dont_write_bytecode = True

# Silence Python 3.12+ "default date adapter is deprecated" warning, keeping the
# exact same stored text format. Registered here (imported by every parser before
# it opens a connection), so it applies process-wide.
sqlite3.register_adapter(datetime.date, datetime.date.isoformat)
sqlite3.register_adapter(datetime.datetime, lambda v: v.strftime('%Y-%m-%d %H:%M:%S'))

# Directory holding the per-doc-type sidecar state files.
STATE_DIR = os.environ.get('ANC_STATE_DIR', 'state')


def db_path():
    """Working SQLite DB path. Set by lib_db.sh; falls back to local file."""
    return os.environ.get('ANC_DB', './data.db')


def log_dir():
    """Directory for run logs. Set by lib_db.sh; falls back to cwd."""
    return os.environ.get('ANC_LOG_DIR', '.')


def debug_enabled():
    """Global logging switch, read uniformly by every parser.

    Controlled by the ANC_DEBUG env var (set by lib_db.sh). Off by default: no
    .log files are written and SQL tracing is disabled, which is much faster and
    avoids filling storage. Turn on with `ANC_DEBUG=1 ./update-*.sh`.
    """
    return os.environ.get('ANC_DEBUG', '0').strip().lower() in ('1', 'true', 'yes', 'on')


def quiet_enabled():
    """Console progress switch, separate from file logging.

    Off by default: the live per-file/per-row progress is printed to the console
    as before. Set ANC_SILENT=1 to suppress that console output (e.g. for cron).
    Independent of ANC_DEBUG (which only controls .log files on disk).
    """
    return os.environ.get('ANC_SILENT', '0').strip().lower() in ('1', 'true', 'yes', 'on')


def cprint(*args, **kwargs):
    """print() that honors ANC_SILENT — use for live console progress lines."""
    if not quiet_enabled():
        print(*args, **kwargs)


def setup_logger(name, filename, level=logging.INFO, mode='w'):
    """Shared file-logger factory honoring ANC_DEBUG (uniform across all parsers).

    When debug is on, logs to <log_dir>/<filename> (or to `filename` as-is if it
    already contains a path). When off, the logger is disabled — calls become
    no-ops and no file is created.
    """
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(level)
    logger.propagate = False
    if debug_enabled():
        logger.disabled = False
        path = filename if (os.path.isabs(filename) or os.sep in filename) \
            else os.path.join(log_dir(), filename)
        handler = logging.FileHandler(path, mode=mode)
        handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(handler)
    else:
        logger.addHandler(logging.NullHandler())
        logger.disabled = True
    return logger


def setup_issue_logger(name, filename):
    """Always-on diagnostics logger for ANOMALIES (skips, empty files, missing
    dates, errors) — independent of ANC_DEBUG.

    Uses delay=True so the .log file is created only when something is actually
    written: a clean run (no anomalies) leaves no file, but if anything is skipped
    or off, the file exists for later inspection.
    """
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.disabled = False
    path = filename if (os.path.isabs(filename) or os.sep in filename) \
        else os.path.join(log_dir(), filename)
    handler = logging.FileHandler(path, mode='w', delay=True)  # file created on first write
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    return logger


def id_pattern(raw):
    """Token pattern of a slash-separated id, e.g. '1439/P/2022' -> 'D/L/D'.

    Shared by the parsers so skip logs use one consistent pattern notation.
    """
    toks = []
    for p in re.split(r'\s*/\s*', str(raw).strip()):
        if re.fullmatch(r'\d+', p):
            toks.append('D')
        elif re.fullmatch(r'[A-Za-z]+', p):
            toks.append('L' * len(p))
        else:
            toks.append('?')
    return '/'.join(toks)


# A number is an institutional reference (data-protection law "Legii nr. 677/2001",
# EU regulation, official gazette, operator id) -- not a dossier -- when one of
# these words sits IMMEDIATELY before it (only "nr."-type glue, no digits/slashes
# in between, so dossiers that merely sit near a page footer are not affected).
_REF_PRE_RE = re.compile(r'(legii|legea|regulament|operator|monitorul|oficial)[^0-9/]{0,12}$', re.I)


def is_reference(pre_context):
    """True if the text immediately preceding a matched number marks it as a
    law/regulation/official reference rather than a dossier."""
    return bool(_REF_PRE_RE.search(pre_context or ''))


def is_boilerplate(line):
    """True if a whole line is header/footer boilerplate (data-protection notice,
    operator id, address, phone, website, page number, law/regulation citation).
    Used by line-oriented parsers (e.g. stadiu) that don't go through document_body."""
    return bool(_BOILER_RE.search(_ascii_lower(line or '')))


def _ascii_lower(s):
    return unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii').lower()


# Boilerplate header/footer lines (the "шапка/тапочки" template around the list):
# data-protection notice, operator id, postal address, phone/email, website,
# authority name, page numbers, law/regulation citations.
_BOILER_RE = re.compile(
    r'(confiden|date cu caracter|operator de date|smardan|fax|e-?mail'
    r'|cetatenie\.just|cetatenie@|www\.|autoritatea nationala'
    r'|pagina\s+\d+\s+din|regulament|leg(ii|ea)\s+nr|cod postal|sector\s+\d)', re.I)

# A line carrying a dossier-style id (number[/seg]/year). Such lines are DATA,
# never the running header/footer band, even if they happen to repeat (a dossier
# can legitimately be listed on more than one page). Law refs like "677/2001"
# also match this, but they sit inside sentences caught by _BOILER_RE instead.
_ID_LINE_RE = re.compile(r'\d{2,7}\s*/\s*(?:[A-Za-z]{1,6}\s*/\s*)?\d{4}')


def document_body(pages, repeat_ratio=0.6, start_markers=()):
    """Split PDF pages into the dossier 'body', stripping the header/footer
    boilerplate (шапка/тапочки) so only the meat (the dossier list) remains.

    Three complementary cuts, because the boilerplate varies slightly between files:
      1. Header/preamble: if start_markers are given (e.g. 'anexa', 'lista'),
         everything up to and including the first marker line is the document
         header (title + legal preamble such as "Legea ... nr. 21/1991") and is
         dropped. Skipped when no marker is present anywhere (kept as-is).
      2. Running band: lines that repeat on most pages (the per-page header/footer)
         — byte-identical across pages within one file, so exact-match counting
         finds them regardless of exact wording.
      3. Markers: lines matching known boilerplate markers (covers single-page
         documents and any band that did not repeat).

    Returns a list of (page_number, [body_line, ...]); page numbers are kept so
    callers can report document:page for anything odd in the body.
    """
    page_lines = [[ln.strip() for ln in p.split('\n') if ln.strip()] for p in pages]

    boiler = set()
    if len(page_lines) >= 2:
        counts = Counter()
        for pl in page_lines:
            for ln in set(pl):          # count each line once per page
                counts[ln] += 1
        thresh = max(2, int(round(len(page_lines) * repeat_ratio)))
        # Repeated non-id lines are the running band; repeated id-bearing lines
        # are real dossiers listed on several pages -> keep them.
        boiler = {ln for ln, n in counts.items()
                  if n >= thresh and not _ID_LINE_RE.search(ln)}

    # Body starts at the first list marker. Mirrors parse_ordins_all's
    # keywords ["ANEX", "LISTA", "1. "]: a substring marker (ANEXA/LISTA) OR a
    # numbered first item ("1." / "1)" / "(1."). The marker line is KEPT (the
    # numbered "1." line carries dossier #1), only the preamble before it is cut.
    markers = tuple(m.lower() for m in start_markers)

    def _is_start(line):
        low = _ascii_lower(line)
        if any(mk in low for mk in markers):
            return True
        return bool(re.match(r'\(?\s*1\s*[.)]', line.strip()))

    has_marker = bool(markers) and any(_is_start(ln) for pl in page_lines for ln in pl)
    started = not has_marker   # if no marker anywhere, keep from the start

    out = []
    for pi, pl in enumerate(page_lines, start=1):
        keep = []
        for ln in pl:
            if not started:
                if _is_start(ln):
                    started = True   # keep this line (it may carry dossier #1)
                else:
                    continue         # drop header/preamble
            if ln in boiler or _BOILER_RE.search(_ascii_lower(ln)):
                continue
            keep.append(ln)
        out.append((pi, keep))
    return out


def _sha1(path, chunk=1 << 20):
    h = hashlib.sha1()
    with open(path, 'rb') as f:
        for blk in iter(lambda: f.read(chunk), b''):
            h.update(blk)
    return h.hexdigest()


def _stat(path):
    st = os.stat(path)
    return st.st_size, int(st.st_mtime)


class FileState:
    """Tracks files already parsed into the DB, keyed by relative path.

    Record per file: {'size', 'mtime', 'sha1'(optional)}.

    The size+mtime pair is a cheap fast-path: for immutable documents
    (ordins/juramat/minori) an already-marked file is skipped without reading
    its bytes. Only when size/mtime differ do we fall back to hashing (when
    use_hash=True) to confirm the content actually changed.
    """

    def __init__(self, doc_type, state_dir=STATE_DIR, use_hash=True):
        self.doc_type = doc_type
        self.use_hash = use_hash
        os.makedirs(state_dir, exist_ok=True)
        self.path = os.path.join(state_dir, f'{doc_type}.json')
        self._state = {}
        if os.path.isfile(self.path):
            try:
                with open(self.path, 'r') as f:
                    self._state = json.load(f)
            except Exception:
                self._state = {}

    def _key(self, filepath):
        return os.path.relpath(filepath)

    def is_new(self, filepath):
        rec = self._state.get(self._key(filepath))
        if rec is None:
            return True
        try:
            size, mtime = _stat(filepath)
        except OSError:
            return True
        if rec.get('size') == size and rec.get('mtime') == mtime:
            return False  # unchanged: skip without hashing
        if self.use_hash:
            try:
                return _sha1(filepath) != rec.get('sha1')
            except OSError:
                return True
        return True

    def new_files(self, candidates):
        """Return the subset of candidate paths that need (re)parsing."""
        return [c for c in candidates if self.is_new(c)]

    def mark(self, filepath, save=True):
        """Record filepath as processed. Pass save=False inside tight loops and
        call save() once at the end to avoid rewriting the JSON per file."""
        try:
            size, mtime = _stat(filepath)
        except OSError:
            return
        rec = {'size': size, 'mtime': mtime}
        if self.use_hash:
            try:
                rec['sha1'] = _sha1(filepath)
            except OSError:
                pass
        self._state[self._key(filepath)] = rec
        if save:
            self.save()

    def save(self):
        tmp = self.path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(self._state, f, indent=0, sort_keys=True)
        os.replace(tmp, self.path)  # atomic


# Where each document type's PDFs live (used by seed()).
DOC_GLOBS = {
    'ordins':  './ordins/*.pdf',
    'juramat': './juramat/*.pdf',
    'minori':  './minori/*.pdf',
    'stadiu':  './stadiu/*/*.pdf',   # dated snapshot subdirectories
}


def seed(doc_type):
    """Mark every currently-present file of doc_type as processed.

    Called after a full (`*_all`) rebuild so the incremental sidecar state stays
    consistent with the freshly rebuilt DB and the next incremental run does not
    re-parse everything.
    """
    pattern = DOC_GLOBS.get(doc_type)
    if not pattern:
        raise SystemExit(f"unknown doc_type: {doc_type}")
    st = FileState(doc_type, use_hash=(doc_type != 'stadiu'))
    files = sorted(glob.glob(pattern))
    for f in files:
        st.mark(f, save=False)
    st.save()
    return len(files)


if __name__ == '__main__':
    argv = sys.argv[1:]
    if argv and argv[0] == 'seed':
        for dt in (argv[1:] or list(DOC_GLOBS)):
            print(f"seeded {dt}: {seed(dt)} files")
    else:
        print("usage: incremental.py seed [doc_type ...]")
