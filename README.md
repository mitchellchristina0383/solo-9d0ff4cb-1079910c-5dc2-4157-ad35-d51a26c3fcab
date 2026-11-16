# Job Log Service

FastAPI + SQLite service for an append-only job log with crash-safe backup and
verified restore.  All key flows are usable from the interactive docs at
`/docs`.

## Run

```bash
docker compose up --build
```

The API listens on `http://localhost:8000` (`JOBLOG_PORT` overrides the port),
docs at `http://localhost:8000/docs`.  State lives in the `joblog-data` volume:
source database `/data/joblogs/source.db`, backups `/data/backups`, restored
targets `/data/restores`.

Without Docker (Python 3.11+):

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
JOBLOG_DATA_DIR=./data uvicorn joblog.main:app --port 8000
```

## API

| Method | Path | Purpose |
| ------ | ---- | ------- |
| `POST` | `/logs` | Append a log entry; returns only after the commit is durable |
| `GET`  | `/logs` | Query committed logs (`job_id`, `after_id`, `limit`) |
| `POST` | `/backups` | Create, verify and publish a backup; returns its backup id |
| `GET`  | `/backups` | List published backups |
| `GET`  | `/backups/{id}` | Get one published backup (id, SHA-256s, snapshot row count) |
| `GET`  | `/backups/{id}/verify` | Re-check manifest, SHA-256 and SQLite integrity |
| `GET`  | `/backups/{id}/download` | Download the verified `.db` file |
| `POST` | `/backups/{id}/restore` | Restore into an independent target (`target_name`, `overwrite`) |
| `GET`  | `/restores/{target_name}/logs` | Read logs from a restored target (read-only) |
| `GET`  | `/health` | Liveness/readiness |

### Typical flow (`/docs` order)

1. `POST /logs` a few entries.
2. `POST /backups` — the response contains the published `id`, the database
   SHA-256 and the manifest SHA-256.
3. `POST /logs` more entries (they must not appear in the backup).
4. `GET /backups/{id}/verify` — confirms the manifest still matches.
5. `POST /backups/{id}/restore` with `{"target_name": "incident-2026.db"}`.
6. `GET /restores/incident-2026.db/logs` — shows the logs committed before the
   backup and none appended afterwards.

## Guarantees and how they are achieved

* **Consistent snapshot under concurrent appends.**  A backup opens a
  read-only connection, starts a read transaction (`BEGIN` + a `SELECT`, which
  pins one committed WAL snapshot) and copies with SQLite's online-backup API.
  Writers keep appending on `BEGIN IMMEDIATE` transactions; rows committed
  after the snapshot are excluded, rows committed before are included.  The
  copy's `COUNT(*)`/`MAX(id)` are cross-checked against the frozen snapshot.
* **Backup id published only after verification.**  The copy is staged under
  `backups/.staging/`, checked with `PRAGMA integrity_check`, a
  `manifest.json` with SHA-256 digests is written, and the whole directory is
  re-verified from disk.  Only then is it atomically renamed to
  `backups/<id>` and the database row flipped to `ready`.  The API only ever
  exposes `ready` backups.
* **Interruption / restart safety.**  A backup row is inserted as `staging`
  before any copying.  Startup runs `recover_incomplete()`: staging directories
  are deleted, non-`ready` rows become `failed`, and `ready` rows whose files
  vanished are demoted to `failed`.  A partial or interrupted backup can never
  be listed, downloaded or restored.
* **Published backups are immutable.**  Backup files are made read-only
  (`0444`), stored outside the source database, and never touched by later
  appends.  The source is a normal WAL database and stays writable.
* **Verified restore.**  `POST /backups/{id}/restore` re-verifies the manifest
  hash, the database hash and `integrity_check` *before* touching the target.
  The restored copy is prepared and re-verified in a private staging
  directory, then published with a single atomic `rename(2)`.  The existing
  target is never moved aside or unmapped: readers see either the old
  complete version or the new one, and any failure before the rename leaves
  the previous target byte-identical.
* **Concurrent restores to the same target.**  Publishes to one target are
  serialised on a per-target lock and each successful response corresponds to
  exactly one atomic, complete version swap.  A restore that fails — even
  after a concurrent restore to the same target has already returned success —
  only deletes its own private staging directory; it can never delete or roll
  back a version another request published.  Overwrite therefore never opens
  a window in which the target is missing or serves mixed log content.
  Orphaned `.restore-*` staging directories (only possible after a hard
  crash) are removed at startup.
* **Independent target.**  Restore writes a separate database file under
  `/data/restores`; later appends to the source and later writes to the target
  are isolated from each other.  The restore endpoint never opens the source
  for writing.

## Tests

```bash
pip install -r requirements.txt pytest httpx
python -m pytest
```

The suite covers concurrent appends during a backup, frozen-snapshot
behaviour, immutable published backups, independent restore, corrupt-backup
restore with the target preserved, interrupted backups after restart,
`ready`-row demotion, backup failure, restart persistence, `/docs`, and
same-target restore concurrency: an interleaved restore failure cannot roll
back a version another restore already published, readers never see a missing
or mixed target while overwrites race, a no-overwrite race publishes exactly
once, and orphaned restore staging directories are cleaned at startup.

## Notes / limits

* The service is a single process.  `recover_incomplete()` and the restore
  staging cleanup assume they are the only writer to the backup/restore
  directories at startup, and restore serialisation uses in-process locks;
  do not run several API instances against the same `/data` volume.
* Backup files are hash-checked on every verify/download/restore, which is
  O(backup size).  Fine for job-log volumes; for very large databases this
  should be cached.
* Restore takes an explicit `target_name` (a filename under `/data/restores`);
  absolute host paths are deliberately not accepted through the API.
