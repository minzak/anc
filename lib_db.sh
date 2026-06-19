#!/bin/bash
# Shared helpers for fast in-memory DB storage, portable across Linux and macOS.
#
# Source from update-*.sh:
#     source ./lib_db.sh
#     anc_shm_begin            # pick fast storage, export ANC_DB, copy data.db there
#     ... run parsers (they read $ANC_DB / $ANC_LOG_DIR) ...
#     anc_shm_end              # move the DB back to the project dir
#     anc_shm_teardown         # release the RAM disk (macOS only)
#
# Linux : uses /dev/shm (tmpfs, already in RAM).
# macOS : has no /dev/shm, so we create a small HFS+ RAM disk on demand.
# Other : falls back to $TMPDIR.

ANC_PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# RAM disk holds ONLY the DB (~140MB) + sqlite temp/WAL; logs go to disk (below),
# so this does not need to be huge. 2GB gives comfortable headroom on macOS.
ANC_SHM_SIZE_MB="${ANC_SHM_SIZE_MB:-2048}"
ANC_MAC_VOL="/Volumes/anc_shm"

_anc_os="$(uname -s)"

# Detach every mounted anc_shm RAM disk by device (handles "/Volumes/anc_shm"
_anc_detach_all() {
    mount | grep -i 'anc_shm' | awk '{print $1}' | while read -r d; do
        hdiutil detach "$d" -force >/dev/null 2>&1
    done
}

# Resolve fast storage and export ANC_DB / ANC_LOG_DIR. Returns non-zero on failure.
# IMPORTANT: only the DB lives on the RAM disk. The parsers' verbose logs (stadiu
# writes per-word + per-SQL trace -> can reach GBs) go to ANC_LOG_DIR on the
# regular disk, otherwise they overflow the RAM disk ("database or disk is full").
anc_shm_init() {
    if [ "$_anc_os" = "Linux" ]; then
        SHM="/dev/shm"
    elif [ "$_anc_os" = "Darwin" ]; then
        # Always start from a fresh RAM disk: detach any leftover mounts first
        # (a previous crashed run may have left a full one, or a duplicate).
        _anc_detach_all
        local sectors=$(( ANC_SHM_SIZE_MB * 2048 ))   # 1 sector = 512 bytes
        local dev
        dev="$(hdiutil attach -nomount ram://${sectors})" || {
            echo "lib_db: RAM disk attach failed" >&2; return 1; }
        dev="$(echo "$dev" | tr -d '[:space:]')"
        diskutil erasevolume HFS+ "anc_shm" "$dev" >/dev/null || {
            echo "lib_db: RAM disk format failed" >&2; return 1; }
        SHM="$ANC_MAC_VOL"
    else
        SHM="${TMPDIR:-/tmp}"
    fi
    export SHM
    export ANC_DB="$SHM/data.db"
    # Logs on the regular disk (project dir); cleaned by the scripts' `rm -f *.log`.
    export ANC_LOG_DIR="$ANC_PROJECT_DIR"
    # Global switches read by every parser:
    #   ANC_DEBUG  — write .log files on disk + SQL trace (off = fast). ANC_DEBUG=1 ./update-*.sh
    #   ANC_SILENT — suppress console progress (off = process shown, as before).
    export ANC_DEBUG="${ANC_DEBUG:-0}"
    export ANC_SILENT="${ANC_SILENT:-0}"
}

# Prepare fast storage and copy the project DB into it.
anc_shm_begin() {
    anc_shm_init || exit 1
    if [ -f "$ANC_PROJECT_DIR/data.db" ]; then
        # Incremental run: work on a copy of the existing DB.
        cp -f "$ANC_PROJECT_DIR/data.db" "$ANC_DB"
    elif [ -f "$ANC_PROJECT_DIR/create_tables.sql" ]; then
        # Fresh build (data.db was removed): create the schema directly in fast
        # storage from create_tables.sql — no local data.db / init_db.sh needed.
        sqlite3 "$ANC_DB" ".read $ANC_PROJECT_DIR/create_tables.sql"
    fi
}

# Move the worked-on DB back to the project directory.
anc_shm_end() {
    mv -f "$ANC_DB" "$ANC_PROJECT_DIR/data.db"
}

# Release the macOS RAM disk (no-op on Linux/other).
anc_shm_teardown() {
    if [ "$_anc_os" = "Darwin" ]; then
        _anc_detach_all
    fi
}
