import hashlib
import json
import random
import sqlite3
import time
import uuid
from contextlib import contextmanager
from .workflow_log import emit

TYPES = {'Appointment', 'MedicationRefill', 'TelephoneTriage'}


class Conflict(Exception):
    pass


class Store:
    def __init__(self, path='encounters.db', clock=time.time):
        self.path, self.clock = path, clock
        with self.tx() as db:
            db.executescript('''
            CREATE TABLE IF NOT EXISTS encounters (
              id TEXT PRIMARY KEY, patient TEXT NOT NULL, type TEXT NOT NULL,
              current_version INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS versions (
              encounter TEXT NOT NULL REFERENCES encounters(id), version INTEGER NOT NULL,
              transcription TEXT NOT NULL, digest TEXT NOT NULL, accepted REAL NOT NULL,
              PRIMARY KEY(encounter,version));
            CREATE TABLE IF NOT EXISTS events (
              id TEXT PRIMARY KEY, digest TEXT NOT NULL,
              encounter TEXT NOT NULL, version INTEGER NOT NULL,
              FOREIGN KEY(encounter,version) REFERENCES versions(encounter,version));
            CREATE TABLE IF NOT EXISTS jobs (
              encounter TEXT NOT NULL, version INTEGER NOT NULL,
              status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
              due REAL NOT NULL, token TEXT, lease REAL, started REAL, finished REAL,
              summary TEXT, error TEXT,
              PRIMARY KEY(encounter,version),
              FOREIGN KEY(encounter,version) REFERENCES versions(encounter,version));
            CREATE INDEX IF NOT EXISTS jobs_due ON jobs(status,due);
            ''')

    @contextmanager
    def tx(self):
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA synchronous=FULL')
        db.execute('BEGIN IMMEDIATE')
        try:
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def ingest(self, event):
        for key in ('event_id', 'encounter_id', 'patient_id'):
            if not isinstance(event.get(key), str) or not event[key].strip():
                raise ValueError('IDs must be nonempty strings')
        v = event.get('version')
        if type(v) is not int or v < 1 or event.get('encounter_type') not in TYPES:
            raise ValueError('invalid version or encounter type')
        if not isinstance(event.get('payload'), dict) or not isinstance(event['payload'].get('transcription'), str) or not event['payload']['transcription'].strip():
            raise ValueError('transcription must be a nonempty string')
        logical = {k: event[k] for k in ('encounter_id', 'patient_id', 'encounter_type', 'version', 'payload')}
        digest = hashlib.sha256(json.dumps(logical, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        eid, enc = event['event_id'], event['encounter_id']
        with self.tx() as db:
            old = db.execute('SELECT * FROM events WHERE id=?', (eid,)).fetchone()
            if old and old['digest'] != digest:
                raise Conflict('event ID content conflict')
            current = db.execute('SELECT * FROM encounters WHERE id=?', (enc,)).fetchone()
            if current and (current['patient'] != event['patient_id'] or current['type'] != event['encounter_type']):
                raise Conflict('encounter identity conflict')
            version = db.execute('SELECT * FROM versions WHERE encounter=? AND version=?', (enc, v)).fetchone()
            if version and version['digest'] != digest:
                raise Conflict('encounter version content conflict')
            outcome = 'duplicate' if version else ('stale' if current and v < current['current_version'] else 'accepted_current')
            now = self.clock()
            if not current:
                db.execute('INSERT INTO encounters VALUES (?,?,?,?)', (enc, event['patient_id'], event['encounter_type'], v))
            if not version:
                db.execute('INSERT INTO versions VALUES (?,?,?,?,?)', (enc, v, event['payload']['transcription'], digest, now))
            if not old:
                db.execute('INSERT INTO events VALUES (?,?,?,?)', (eid, digest, enc, v))
            if outcome == 'accepted_current':
                db.execute('UPDATE encounters SET current_version=? WHERE id=?', (v, enc))
                db.execute("INSERT INTO jobs(encounter,version,status,due) VALUES (?,?,'queued',?)", (enc, v, now))
            cv = db.execute('SELECT current_version FROM encounters WHERE id=?', (enc,)).fetchone()[0]
            return {'outcome': outcome, 'encounter_id': enc, 'version': v, 'current_version': cv}

    def progress(self, enc):
        with self.tx() as db:
            row = db.execute('''SELECT e.*, j.*, v.accepted FROM encounters e
                JOIN jobs j ON j.encounter=e.id AND j.version=e.current_version
                JOIN versions v ON v.encounter=j.encounter AND v.version=j.version
                WHERE e.id=?''', (enc,)).fetchone()
            if not row:
                raise KeyError(enc)
            r = dict(row)
            return dict(encounter_id=enc, patient_id=r['patient'], encounter_type=r['type'],
                        version=r['current_version'], status=r['status'], summary=r['summary'],
                        attempts=r['attempts'], accepted_at=r['accepted'], started_at=r['started'],
                        finished_at=r['finished'], next_attempt_at=r['due'] if r['status']=='queued' else None,
                        error=r['error'], sla_breached=(r['finished'] or self.clock()) - r['accepted'] > 10)

    def claim(self, max_attempts=6, lease_seconds=45):
        now = self.clock()
        with self.tx() as db:
            db.execute("UPDATE jobs SET status='queued', token=NULL WHERE status='processing' AND lease<=?", (now,))
            db.execute("""UPDATE jobs SET status='superseded', finished=? WHERE status='queued'
                AND version < (SELECT current_version FROM encounters WHERE id=jobs.encounter)""", (now,))
            db.execute("UPDATE jobs SET status='failed', finished=?, error='attempt_budget_exhausted' WHERE status='queued' AND attempts>=?", (now, max_attempts))
            r = db.execute("SELECT * FROM jobs WHERE status='queued' AND due<=? ORDER BY due LIMIT 1", (now,)).fetchone()
            if not r:
                return None
            token = uuid.uuid4().hex
            db.execute("UPDATE jobs SET status='processing', attempts=attempts+1, token=?, lease=?, started=COALESCE(started,?) WHERE encounter=? AND version=?", (token, now+lease_seconds, now, r['encounter'], r['version']))
            transcript = db.execute('SELECT transcription FROM versions WHERE encounter=? AND version=?', (r['encounter'], r['version'])).fetchone()[0]
            job = dict(encounter=r['encounter'], version=r['version'], token=token, transcription=transcript)
            attempt = r['attempts'] + 1
        emit('job_claimed', encounter_id=job['encounter'], version=job['version'],
             claim_id=token, attempt=attempt, previous_state='queued', state='processing',
             lease_expires_at=now+lease_seconds, input_source='immutable_version_snapshot')
        return job

    def complete(self, job, summary=None, error=None, max_attempts=6, permanent=False):
        now = self.clock()
        with self.tx() as db:
            r = db.execute("SELECT * FROM jobs WHERE encounter=? AND version=? AND token=? AND status='processing'", (job['encounter'], job['version'], job['token'])).fetchone()
            if not r:
                rejected = True
            else:
                rejected = False
            if rejected:
                emit('completion_rejected', encounter_id=job['encounter'], version=job['version'],
                     claim_id=job['token'], reason='claim_no_longer_owns_processing_job')
                return False
            status = 'ready' if error is None else ('failed' if permanent or r['attempts'] >= max_attempts else 'queued')
            delay = random.uniform(.5, 1) * min(300, 2 ** r['attempts'])
            db.execute('UPDATE jobs SET status=?, summary=?, error=?, due=?, finished=?, token=NULL WHERE encounter=? AND version=?',
                       (status, summary, error, now+delay, now if status in ('ready','failed') else None, r['encounter'], r['version']))
            current_version = db.execute('SELECT current_version FROM encounters WHERE id=?',
                                         (job['encounter'],)).fetchone()[0]
        emit('job_state_committed', encounter_id=job['encounter'], version=job['version'],
             claim_id=job['token'], attempt=r['attempts'], previous_state='processing',
             state=status, error=error, next_attempt_at=now+delay if status=='queued' else None,
             summary_saved=status=='ready', current_version=current_version,
             is_current=job['version']==current_version)
        return True

    def redrive(self, enc, version):
        with self.tx() as db:
            result = db.execute("""UPDATE jobs SET status='queued', attempts=0, due=?, finished=NULL, error=NULL
                WHERE encounter=? AND version=? AND status='failed'
                AND version=(SELECT current_version FROM encounters WHERE id=?)""", (self.clock(), enc, version, enc))
            return result.rowcount == 1

    def history(self, enc, version):
        with self.tx() as db:
            r = db.execute('SELECT version,status,attempts,started,finished,summary,error FROM jobs WHERE encounter=? AND version=?', (enc, version)).fetchone()
            if not r:
                raise KeyError(enc)
            return dict(r)
