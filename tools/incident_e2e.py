"""Real HTTP ingestion and worker processing for synthetic incident groups."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TYPES = ('Appointment', 'MedicationRefill', 'TelephoneTriage')


def generate(path, incidents, patients):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w') as out:
        for i in range(incidents):
            for p in range(patients):
                for t, kind in enumerate(TYPES):
                    index = (i * patients + p) * 3 + t
                    scenario = ('normal', 'transient', 'permanent', 'slow')[index % 4]
                    enc = f'inc-{i:04d}-pat-{p:02d}-type-{t}'
                    transcript = '\n'.join(f'{"Nurse" if n%2==0 else "Patient"}: Synthetic incident {i}, patient {p}, {kind}, dialogue line {n}.' for n in range(40))
                    event = dict(event_id=enc+'-v12', encounter_id=enc, patient_id=f'inc-{i:04d}-patient-{p:02d}',
                                 encounter_type=kind, version=12,
                                 payload={'transcription': f'SYNTHETIC:{scenario}:{enc}\n'+transcript})
                    out.write(json.dumps({'incident_id': f'inc-{i:04d}', 'scenario':scenario, 'event':event})+'\n')


def request(base, method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base+path, data=data, method=method,
                                 headers={'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


def digest(text):
    return 'Synthetic summary sha256='+hashlib.sha256(text.encode()).hexdigest()


class Provider(BaseHTTPRequestHandler):
    calls = Counter()
    lock = threading.Lock()
    def log_message(self, *args):
        pass
    def do_POST(self):
        text = json.loads(self.rfile.read(int(self.headers['Content-Length'])))['transcription']
        mode = text.split(':',2)[1]
        with self.lock:
            self.calls[text] += 1
            attempt = self.calls[text]
        code = 400 if mode=='permanent' else (503 if mode=='transient' and attempt==1 else 200)
        if mode=='slow':
            time.sleep(.03 if 'snapshot=13' in text else .15)
        body = json.dumps({'summary': digest(text)} if code==200 else {'error':'synthetic fault'}).encode()
        self.send_response(code)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except BrokenPipeError:
            pass


def load(rows, base, clients):
    """Deliver updates through HTTP only, including simultaneous duplicate requests."""
    counts = Counter()
    lock = threading.Lock()
    def checked(event, codes, outcomes=None):
        status, body = request(base,'POST','/events',event)
        if status not in codes or (outcomes and body.get('outcome') not in outcomes):
            raise AssertionError(f"unexpected response for {event['encounter_id']}: {status}, {body}")
        with lock:
            counts[str(status)+':'+body.get('outcome',body.get('error','unknown'))] += 1
    def deliver(row):
        e = row['event']
        # Complete snapshots, deliberately skipping version 11.
        checked(e, {202}, {'accepted_current'})
        alias = json.loads(json.dumps(e))
        alias['event_id'] += '-alias'
        with ThreadPoolExecutor(3) as duplicates:
            list(duplicates.map(lambda x: checked(x,{200},{'duplicate'}), [e,e,alias]))
        late = json.loads(json.dumps(e))
        late.update(event_id=e['encounter_id']+'-v11',version=11)
        late['payload']['transcription'] += '\nsnapshot=11'
        checked(late,{200},{'stale'})
        newer = json.loads(json.dumps(e))
        newer.update(event_id=e['encounter_id']+'-v13',version=13)
        newer['payload']['transcription'] += '\nsnapshot=13'
        checked(newer,{202},{'accepted_current'})
        bad = json.loads(json.dumps(newer))
        bad['event_id'] += '-bad-patient'
        bad['patient_id'] += '-wrong'
        checked(bad,{409})
        bad = json.loads(json.dumps(newer))
        bad['event_id'] += '-bad-type'
        bad['encounter_type'] = TYPES[(TYPES.index(e['encounter_type'])+1)%3]
        checked(bad,{409})
        bad = json.loads(json.dumps(newer))
        bad['event_id'] += '-bad-payload'
        bad['payload']['transcription'] += '\nconflict'
        checked(bad,{409})
        bad = json.loads(json.dumps(newer))
        bad['version'] = 14  # Existing global event ID reused inconsistently.
        checked(bad,{409})
    with ThreadPoolExecutor(clients) as pool:
        for n,_ in enumerate(pool.map(deliver,rows),1):
            if n%1000==0:
                print(f'HTTP ingestion: {n} encounters',flush=True)
    return dict(counts)


def verify(rows, base, deadline, use_llm=False, max_words=100):
    pending = {r['event']['encounter_id']: r for r in rows}
    counts = Counter()
    while pending:
        if time.monotonic()>deadline:
            raise TimeoutError(f'{len(pending)} encounters unfinished')
        for enc,row in list(pending.items()):
            code,result = request(base,'GET',f'/encounters/{enc}/summary')
            if code!=200 or result['version']!=13:
                raise AssertionError(f'current version mismatch: {enc}')
            if result['status'] not in ('ready','failed'):
                continue
            expected = 'failed' if not use_llm and row['scenario']=='permanent' else 'ready'
            if result['status']!=expected:
                raise AssertionError(f'unexpected terminal status: {enc}, {result["status"]}, error={result.get("error")}')
            if expected=='ready':
                text = row['event']['payload']['transcription']+'\nsnapshot=13'
                if use_llm:
                    summary = result.get('summary')
                    if (not isinstance(summary, str) or not summary.strip()
                            or summary.startswith('Synthetic summary sha256=')
                            or summary == 'Synthetic demo summary; not for clinical use.'
                            or len(summary.split()) > max_words):
                        raise AssertionError(f'invalid LLM summary: {enc}')
                elif result['summary']!=digest(text):
                    raise AssertionError(f'wrong-version summary: {enc}')
            elif result['error']!='provider_http_400':
                raise AssertionError(f'wrong error: {enc}')
            if result['patient_id']!=row['event']['patient_id'] or result['encounter_type']!=row['event']['encounter_type']:
                raise AssertionError(f'identity changed: {enc}')
            counts[expected]+=1
            counts['sla_breached']+=int(result['sla_breached'])
            del pending[enc]
        if pending:
            time.sleep(.2)
    return dict(counts)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('command',choices=('generate','run','load'))
    p.add_argument('--data',type=Path,default=ROOT/'test_data/incidents-1000.jsonl')
    p.add_argument('--incidents',type=int,default=1000)
    p.add_argument('--patients',type=int,default=3)
    p.add_argument('--clients',type=int,default=8)
    p.add_argument('--workers',type=int,default=8)
    p.add_argument('--timeout',type=int,default=3600)
    p.add_argument('--api',default='http://127.0.0.1:8000')
    p.add_argument('--use-llm', action='store_true', help='Use paid OpenAI summaries instead of the synthetic provider (run only)')
    p.add_argument('--limit', type=int, default=100, help='Encounter rows to ingest (default: 100; use 9000 for the full dataset)')
    p.add_argument('--output',type=Path,default=ROOT/'test_data/e2e-run')
    args=p.parse_args()
    if min(args.incidents,args.patients,args.clients,args.workers,args.timeout)<1:
        p.error('counts and timeout must be positive')
    if args.limit is not None and args.limit < 1:
        p.error('limit must be positive')
    if args.use_llm and args.command != 'run':
        p.error('--use-llm is available only with run; load uses your existing worker configuration')
    adapter = None
    if args.use_llm:
        from encounter_service.summary_adapters import build_adapter
        try:
            adapter = build_adapter(True)
        except ValueError as exc:
            p.error(str(exc))
    if args.command=='generate':
        generate(args.data,args.incidents,args.patients)
        print(f'Generated {args.incidents*args.patients*3} encounters at {args.data}')
        return
    from itertools import islice
    with args.data.open() as data:
        rows=[json.loads(line) for line in islice(data, args.limit)]
    if not rows:
        p.error('dataset is empty')
    if args.command=='load':
        print(json.dumps(load(rows,args.api,args.clients),indent=2))
        return
    # An exclusive fresh directory prevents accidental reuse of a prior test database.
    args.output.mkdir(parents=True,exist_ok=False)
    db=args.output/'encounters.db'
    report={'encounters':len(rows),'incidents':len({r['incident_id'] for r in rows}),
            'patients':len({r['event']['patient_id'] for r in rows}), 'passed':False,
            'provider_mode':'openai' if args.use_llm else 'synthetic',
            'model':getattr(adapter,'model',None),
            'summary_max_words':getattr(adapter,'max_words',None),
            'verification':'version/identity and nonempty bounded LLM output; not clinical quality' if args.use_llm else 'exact synthetic transcript hash'}
    print(f"Mode: {report['provider_mode']}; encounters: {len(rows)}; workers: {args.workers}", flush=True)
    if args.use_llm:
        print('Paid OpenAI calls enabled. Both v12 and v13 may be processed; retries may incur additional charges.', flush=True)
    processes=[]
    server=None
    log=(args.output/'process.log').open('w')
    started=time.monotonic()
    try:
        if not args.use_llm:
            Provider.calls.clear()
            server=ThreadingHTTPServer(('127.0.0.1',0),Provider)
            threading.Thread(target=server.serve_forever,daemon=True).start()
        # Reserve the API port; the API process binds after the reservation is released.
        import socket
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0))
            port=sock.getsockname()[1]
        base=f'http://127.0.0.1:{port}'
        env=dict(os.environ, USE_LLM='1' if args.use_llm else '0')
        if args.use_llm:
            env.pop('SUMMARY_URL',None)
            env.pop('SUMMARY_MOCK',None)
        else:
            env['SUMMARY_URL']=f'http://127.0.0.1:{server.server_port}/summary'
        def start(module, extra=()):
            process=subprocess.Popen([sys.executable,'-m',module,'--db',str(db),*extra],cwd=ROOT,env=env,stdout=log,stderr=log)
            processes.append(process)
            return process
        api=start('encounter_service.api',('--port',str(port)))
        for _ in range(100):
            if api.poll() is not None:
                raise RuntimeError('API exited; inspect process.log')
            try:
                if request(base,'GET','/health')[0]==200:
                    break
            except OSError:
                time.sleep(.1)
        else:
            raise TimeoutError('API startup timeout')
        workers=[start('encounter_service.worker') for _ in range(args.workers)]
        report['http_outcomes']=load(rows,base,args.clients)
        # Restart a worker process while backlog exists. Depending on timing, it may be idle
        # or processing; deterministic lease-fencing is separately tested by unit scenarios.
        if not args.use_llm:
            workers[0].kill()
            workers[0].wait()
            start('encounter_service.worker')
        # Do not deliberately interrupt paid calls or synthesize provider errors in LLM mode.
        report['worker_restart_tested']=not args.use_llm
        report['results']=verify(rows,base,time.monotonic()+args.timeout,
                                 use_llm=args.use_llm, max_words=getattr(adapter,'max_words',100))
        report['provider_invocations']=None if args.use_llm else sum(Provider.calls.values())
        report['passed']=True
    except Exception as exc:
        report['error']=f'{type(exc).__name__}: {exc}'
        raise
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if server is not None:
            server.shutdown()
            server.server_close()
        log.close()
        report['duration_seconds']=round(time.monotonic()-started,3)
        (args.output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps(report,indent=2))


if __name__=='__main__':
    main()
