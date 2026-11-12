import subprocess
import sys
import unittest
from unittest.mock import patch

from tools.incident_e2e import verify, digest
from test_store import event


class IncidentVerificationTests(unittest.TestCase):
    def row(self, scenario='permanent'):
        return {'incident_id':'incident','scenario':scenario,'event':event()}

    def result(self, summary='Patient reports a sore throat.', status='ready'):
        return {'version':13, 'status':status, 'summary':summary,
                'patient_id':'pat','encounter_type':'Appointment',
                'sla_breached':False,'error':None}

    def test_llm_does_not_expect_synthetic_permanent_failure(self):
        with patch('tools.incident_e2e.request',return_value=(200,self.result())):
            self.assertEqual(verify([self.row()], 'http://unused', float('inf'), use_llm=True),
                             {'ready':1,'sla_breached':0})

    def test_llm_rejects_synthetic_empty_and_overlong_output(self):
        for summary in ('',digest('input'),'Synthetic demo summary; not for clinical use.','word '*101):
            with self.subTest(summary=summary[:25]), patch('tools.incident_e2e.request',return_value=(200,self.result(summary))):
                with self.assertRaisesRegex(AssertionError,'invalid LLM summary'):
                    verify([self.row()], 'http://unused', float('inf'), use_llm=True)

    def test_llm_failure_fails_campaign(self):
        with patch('tools.incident_e2e.request',return_value=(200,self.result(None,'failed'))):
            with self.assertRaisesRegex(AssertionError,'unexpected terminal status'):
                verify([self.row()], 'http://unused', float('inf'), use_llm=True)

    def test_synthetic_exact_hash_verification_stays_enabled(self):
        row=self.row('normal')
        result=self.result(digest(row['event']['payload']['transcription']+'\nsnapshot=13'))
        with patch('tools.incident_e2e.request',return_value=(200,result)):
            self.assertEqual(verify([row], 'http://unused', float('inf')),{'ready':1,'sla_breached':0})

    def test_llm_cli_fails_early_without_key(self):
        import os
        env=dict(os.environ)
        env.pop('OPENAI_API_KEY',None)
        run=subprocess.run([sys.executable,'tools/incident_e2e.py','run','--use-llm','--limit','1'],
                           env=env,capture_output=True,text=True)
        self.assertEqual(run.returncode,2)
        self.assertIn('OPENAI_API_KEY is required',run.stderr)
