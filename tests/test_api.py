"""Exercise real HTTP parsing and serialization without binding a network port."""
import io
import json
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from encounter_service.api import handler
from encounter_service.store import Store
from test_store import event


class Connection:
    def __init__(self, request):
        self.input = io.BytesIO(request)
        self.output = bytearray()

    def makefile(self, *args, **kwargs):
        return self.input

    def sendall(self, data):
        self.output.extend(data)


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(self.tmp.name + '/db', lambda: 100)

    def request(self, method, path, body=None, size=None):
        body = b'' if body is None else body
        raw = (f'{method} {path} HTTP/1.0\r\nHost: localhost\r\n'
               f'Content-Length: {len(body) if size is None else size}\r\n\r\n').encode() + body
        connection = Connection(raw)
        handler(self.store)(connection, ('127.0.0.1', 1234), SimpleNamespace())
        headers, payload = bytes(connection.output).split(b'\r\n\r\n', 1)
        self.assertIn(b'Content-Type: application/json', headers)
        self.assertIn(f'Content-Length: {len(payload)}'.encode(), headers)
        return int(headers.split()[1]), json.loads(payload)

    def post(self, data):
        return self.request('POST', '/events', json.dumps(data).encode())

    def test_ingestion_outcomes_and_conflict_codes(self):
        self.assertEqual(self.post(event(12))[0], 202)
        status, body = self.post(event(12))
        self.assertEqual((status, body['outcome']), (200, 'duplicate'))
        status, body = self.post(event(11))
        self.assertEqual((status, body['outcome'], body['current_version']), (200, 'stale', 12))
        changed = event(13)
        changed['patient_id'] = 'different'
        self.assertEqual(self.post(changed)[0], 409)

    def test_progress_lifecycle_and_version_history(self):
        self.post(event())
        path = '/encounters/enc/summary'
        self.assertEqual(self.request('GET', path)[1]['status'], 'queued')
        job = self.store.claim()
        self.assertEqual(self.request('GET', path)[1]['status'], 'processing')
        self.store.complete(job, 'Synthetic summary')
        status, result = self.request('GET', path)
        self.assertEqual((status, result['status'], result['summary']), (200, 'ready', 'Synthetic summary'))
        self.post(event(2))
        self.assertIsNone(self.request('GET', path)[1]['summary'])
        self.assertEqual(self.request('GET', '/encounters/enc/versions/1')[1]['summary'], 'Synthetic summary')
        self.store.complete(self.store.claim(), error='permanent', permanent=True)
        status, result = self.request('GET', path)
        self.assertEqual((status, result['status']), (200, 'failed'))
        self.assertEqual(result['transcription'], event(2)['payload']['transcription'])

    def test_malformed_requests_and_size_limits(self):
        for body in (b'{', b'[]', b'null', b'{}', b'{"version":true}'):
            with self.subTest(body=body):
                self.assertEqual(self.request('POST', '/events', body)[0], 400)
        for size in (0, -1, 2_000_001):
            self.assertEqual(self.request('POST', '/events', size=size)[0], 413)

    def test_missing_routes_resources_and_invalid_versions(self):
        for method, path in (('GET', '/unknown'), ('POST', '/unknown'),
                             ('GET', '/encounters/missing/summary'),
                             ('GET', '/encounters/missing/versions/1')):
            with self.subTest(method=method, path=path):
                self.assertEqual(self.request(method, path)[0], 404)
        self.assertEqual(self.request('GET', '/encounters/enc/versions/not-an-int')[0], 400)

    def test_storage_failures_are_sanitized_and_retryable(self):
        for method, path, target, body in (
            ('POST', '/events', 'ingest', json.dumps(event()).encode()),
            ('GET', '/encounters/enc/summary', 'progress', None),
        ):
            with self.subTest(method=method), patch.object(self.store, target, side_effect=RuntimeError('secret details')):
                self.assertEqual(self.request(method, path, body), (503, {'error': 'storage_unavailable'}))

    def test_health(self):
        self.assertEqual(self.request('GET', '/health'), (200, {'status': 'ok'}))


    def test_transcription_and_summary_always_match_current_version(self):
        first = event(12)
        first['payload']['transcription'] = 'Nurse: Symptoms?\nPatient: ठीक है.\n' * 1000
        self.post(first)
        old_job = self.store.claim()
        second = event(13)
        self.post(second)
        path = '/encounters/enc/summary'
        status, result = self.request('GET', path)
        self.assertEqual(status, 200)
        self.assertEqual(result['version'], 13)
        self.assertEqual(result['transcription'], second['payload']['transcription'])
        self.assertIsNone(result['summary'])
        self.store.complete(self.store.claim(), 'Version 13 summary')
        self.store.complete(old_job, 'Version 12 summary')
        result = self.request('GET', path)[1]
        self.assertEqual(result['transcription'], second['payload']['transcription'])
        self.assertEqual(result['summary'], 'Version 13 summary')

    def test_full_transcription_is_returned_unchanged_while_processing_and_ready(self):
        data = event()
        data['payload']['transcription'] = 'Nurse: Symptoms?\nPatient: ठीक है.\n' * 1000
        self.post(data)
        path = '/encounters/enc/summary'
        self.assertEqual(self.request('GET', path)[1]['transcription'], data['payload']['transcription'])
        job = self.store.claim()
        self.assertEqual(self.request('GET', path)[1]['transcription'], data['payload']['transcription'])
        self.store.complete(job, 'Summary')
        self.assertEqual(self.request('GET', path)[1]['transcription'], data['payload']['transcription'])
