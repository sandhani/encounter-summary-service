import io
import json
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from encounter_service.summary_adapters import (
    DEFAULT_MODEL, HttpSummaryAdapter, MockSummaryAdapter, OpenAISummaryAdapter,
    build_adapter,
)
from encounter_service.store import Store
from encounter_service.worker import tick
from test_store import event


def response(text='A synthetic summary.'):
    return dict(status='completed', output=[{'type':'reasoning'},
        {'type':'message','role':'assistant','status':'completed',
         'content':[{'type':'output_text','text':text}]}])


class SummaryAdapterTests(unittest.TestCase):
    def test_selection_and_precedence(self):
        env={'USE_LLM':'1','OPENAI_API_KEY':'test-key','SUMMARY_URL':'http://unused', 'SUMMARY_MOCK':'1'}
        self.assertIsInstance(build_adapter(environ=env), OpenAISummaryAdapter)
        self.assertEqual(build_adapter(environ=env).model, DEFAULT_MODEL)
        self.assertIsInstance(build_adapter(False,env),HttpSummaryAdapter)
        self.assertIsInstance(build_adapter(environ={'SUMMARY_MOCK':'1'}),MockSummaryAdapter)

    def test_cli_override_enables_llm(self):
        adapter=build_adapter(True,{'USE_LLM':'0','OPENAI_API_KEY':'test-key','LLM_MODEL':'gpt-6-luna'})
        self.assertIsInstance(adapter,OpenAISummaryAdapter)
        self.assertEqual(adapter.model,'gpt-6-luna')

    def test_invalid_configuration_fails_before_claim(self):
        for env in ({}, {'USE_LLM':'1'}, {'USE_LLM':'maybe'},
                    {'USE_LLM':'1','OPENAI_API_KEY':'test','LLM_MODEL':''},
                    {'USE_LLM':'1','OPENAI_API_KEY':'test','LLM_REASONING_EFFORT':'invalid'}):
            with self.subTest(env=env),self.assertRaises(ValueError):
                build_adapter(environ=env)

    def test_exact_request_and_response_parsing(self):
        transcript='Nurse: Dose?\nPatient: 500 mg twice daily. ठीक है.\n' * 1000
        adapter=OpenAISummaryAdapter('secret-test-key')
        with patch('urllib.request.urlopen', return_value=io.BytesIO(json.dumps(response()).encode())) as call:
            self.assertEqual(adapter(transcript),'A synthetic summary.')
        call.assert_called_once()
        request=call.call_args[0][0]
        body=json.loads(request.data)
        self.assertEqual(request.full_url,'https://api.openai.com/v1/responses')
        self.assertEqual(request.get_header('Authorization'),'Bearer secret-test-key')
        self.assertEqual(body['input'],transcript)
        self.assertFalse(body['store'])
        self.assertEqual(body['reasoning'],{'effort':'low'})
        self.assertNotIn('secret-test-key',repr(adapter))

    def test_partial_refused_empty_or_malformed_output_never_saved(self):
        bad=[{}, [], {'status':'incomplete','output':[]}, response(' '),
             {'status':'completed','output':[]}, {'status':'completed','output':[None]}]
        refused=response()
        refused['output'][1]['content']=[{'type':'refusal','refusal':'no'}]
        bad.append(refused)
        for value in bad:
            with self.subTest(value=value),patch('urllib.request.urlopen',return_value=io.BytesIO(json.dumps(value).encode())):
                with self.assertRaises(ValueError):
                    OpenAISummaryAdapter('test')('transcript')

    def test_http_adapter_preserves_contract_and_validates_missing_summary(self):
        with patch('urllib.request.urlopen',return_value=io.BytesIO(b'{"summary":"ok"}')):
            self.assertEqual(HttpSummaryAdapter('http://localhost/summary')('input'),'ok')
        with patch('urllib.request.urlopen',return_value=io.BytesIO(b'{}')):
            with self.assertRaises(ValueError):
                HttpSummaryAdapter('http://localhost/summary')('input')

    def test_paid_errors_are_not_retried_inside_adapter(self):
        for code in (401,429,503):
            with self.subTest(code=code),patch('urllib.request.urlopen',side_effect=HTTPError('https://api.openai.com',code,'synthetic',{},None)) as call:
                with self.assertRaises(HTTPError):
                    OpenAISummaryAdapter('test')('input')
                self.assertEqual(call.call_count,1)

    def test_worker_persists_llm_result_and_rejects_partial_response(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(tmp+'/db')
            store.ingest(event())
            with patch('urllib.request.urlopen',return_value=io.BytesIO(json.dumps(response()).encode())):
                tick(store,OpenAISummaryAdapter('test'))
            self.assertEqual(store.progress('enc')['summary'],'A synthetic summary.')
            store.ingest(event(2))
            with patch('urllib.request.urlopen',return_value=io.BytesIO(b'{"status":"incomplete"}')):
                tick(store,OpenAISummaryAdapter('test'))
            self.assertEqual(store.progress('enc')['status'],'failed')
            self.assertIsNone(store.progress('enc')['summary'])

    def test_worker_retries_429_but_fails_401(self):
        for code,expected in ((429,'queued'),(401,'failed')):
            with self.subTest(code=code),tempfile.TemporaryDirectory() as tmp:
                store=Store(tmp+'/db')
                store.ingest(event())
                with patch('urllib.request.urlopen',side_effect=HTTPError('https://api.openai.com',code,'secret message',{},None)):
                    tick(store,OpenAISummaryAdapter('test'))
                self.assertEqual(store.progress('enc')['status'],expected)
                self.assertEqual(store.progress('enc')['error'],f'provider_http_{code}')


    def test_word_limit_boundary_including_unicode_whitespace(self):
        for count in (99, 100, 101):
            text = '\n'.join(['शब्द'] * count)
            with self.subTest(count=count), patch('urllib.request.urlopen', return_value=io.BytesIO(json.dumps(response(text)).encode())):
                adapter = OpenAISummaryAdapter('test')
                if count > 100:
                    with self.assertRaisesRegex(ValueError, 'summary_word_limit_exceeded'):
                        adapter('input')
                else:
                    self.assertEqual(adapter('input'), text)

    def test_custom_limit_is_validated_and_in_prompt(self):
        env = {'USE_LLM': '1', 'OPENAI_API_KEY': 'test', 'SUMMARY_MAX_WORDS': '50'}
        adapter = build_adapter(environ=env)
        self.assertEqual(adapter.max_words, 50)
        with patch('urllib.request.urlopen', return_value=io.BytesIO(json.dumps(response()).encode())) as call:
            adapter('input')
        self.assertIn('no more than 50 words', json.loads(call.call_args[0][0].data)['instructions'])
        for value in ('0', '-1', '', '1.5', 'invalid'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                build_adapter(environ=dict(env, SUMMARY_MAX_WORDS=value))

    def test_over_limit_summary_is_not_persisted_or_automatically_retried(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(tmp+'/db')
            store.ingest(event())
            with patch('urllib.request.urlopen', return_value=io.BytesIO(json.dumps(response('word ' * 101)).encode())) as call:
                tick(store, OpenAISummaryAdapter('test'))
            self.assertEqual(call.call_count, 1)
            result = store.progress('enc')
            self.assertEqual(result['status'], 'failed')
            self.assertIsNone(result['summary'])
            self.assertIsNone(store.claim())
