import concurrent.futures
import tempfile
import unittest
from encounter_service.store import Conflict, Store


def event(v=1, eid=None, **kw):
    return dict(event_id=eid or f'e{v}', encounter_id='enc', patient_id='pat', encounter_type='Appointment', version=v, payload={'transcription': f'Patient: version {v}'}, **kw)


class ReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.now = 100
        self.store = Store(self.tmp.name+'/db', lambda: self.now)

    def tearDown(self):
        self.tmp.cleanup()

    def test_concurrent_duplicate(self):
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            results = list(pool.map(lambda _: self.store.ingest(event()), range(16)))
        self.assertEqual(sum(r['outcome']=='accepted_current' for r in results), 1)
        self.assertEqual(self.store.ingest(event(eid='alias'))['outcome'], 'duplicate')
        with self.store.tx() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM jobs').fetchone()[0], 1)

    def test_gaps_and_stale(self):
        self.store.ingest(event(10))
        self.store.ingest(event(12))
        self.assertEqual(self.store.ingest(event(11))['outcome'], 'stale')
        job = self.store.claim()
        self.assertEqual(job['version'], 12)
        self.assertEqual(job['transcription'], 'Patient: version 12')

    def test_restart_and_fencing(self):
        self.store.ingest(event())
        restarted = Store(self.store.path, lambda: self.now)
        first = restarted.claim()
        self.now += 46
        second = restarted.claim()
        self.assertFalse(restarted.complete(first, 'wrong'))
        self.assertTrue(restarted.complete(second, 'saved'))
        self.assertFalse(restarted.complete(second, 'repeated'))
        self.assertIsNone(restarted.claim())
        self.assertEqual(restarted.progress('enc')['summary'], 'saved')

    def test_out_of_order_results(self):
        self.store.ingest(event(12))
        old = self.store.claim()
        self.store.ingest(event(13))
        new = self.store.claim()
        self.store.complete(new, 'new')
        self.store.complete(old, 'old')
        self.assertEqual(self.store.progress('enc')['summary'], 'new')
        self.assertEqual(self.store.history('enc', 12)['summary'], 'old')

    def test_conflicts_atomic(self):
        self.store.ingest(event())
        for key, value in [('patient_id','other'), ('encounter_type','TelephoneTriage'), ('payload', {'transcription':'different'})]:
            changed = event(eid='alias')
            changed[key] = value
            with self.assertRaises(Conflict):
                self.store.ingest(changed)
        changed = event(2, eid='e1')
        with self.assertRaises(Conflict):
            self.store.ingest(changed)
        self.assertEqual(self.store.progress('enc')['version'], 1)

    def test_retry_exhaustion_sla_redrive(self):
        self.store.ingest(event())
        for _ in range(6):
            job = self.store.claim()
            self.store.complete(job, error='provider_call_failed')
            self.now += 301
        result = self.store.progress('enc')
        self.assertEqual(result['status'], 'failed')
        self.assertTrue(result['sla_breached'])
        self.assertTrue(self.store.redrive('enc', 1))
        job = self.store.claim()
        self.now += 11
        self.assertEqual(self.store.progress('enc')['status'], 'processing')
        self.store.complete(job, 'recovered')
        self.assertEqual(self.store.progress('enc')['status'], 'ready')


if __name__ == '__main__':
    unittest.main()
