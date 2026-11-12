import argparse
import json
import logging
import os
import time
import urllib.request
import urllib.error
from .store import Store
from .workflow_log import emit


def generate_summary(transcription):
    """Configured HTTP adapter; mock mode is explicitly for local orchestration demos."""
    endpoint = os.environ.get('SUMMARY_URL')
    if not endpoint:
        if os.environ.get('SUMMARY_MOCK') != '1':
            raise RuntimeError('provider_not_configured')
        return 'Synthetic demo summary; not for clinical use.'
    request = urllib.request.Request(endpoint, data=json.dumps({'transcription': transcription}).encode(),
                                     headers={'Content-Type': 'application/json'}, method='POST')
    with urllib.request.urlopen(request, timeout=20) as response:
        result = json.load(response)['summary']
        if not isinstance(result, str) or not result:
            raise ValueError('invalid_provider_response')
        return result


def tick(store, provider=generate_summary):
    job = store.claim()
    if not job:
        return False
    fields = dict(encounter_id=job['encounter'], version=job['version'], claim_id=job['token'])
    emit('summary_generation_started', **fields, state='processing',
         transcription_characters=len(job['transcription']))
    started = time.monotonic()
    try:
        summary = provider(job['transcription'])
    except urllib.error.HTTPError as exc:
        emit('summary_generation_failed', **fields, error=f'provider_http_{exc.code}',
             duration_seconds=round(time.monotonic()-started, 3))
        store.complete(job, error=f'provider_http_{exc.code}',
                       permanent=400 <= exc.code < 500 and exc.code not in (408, 429))
    except ValueError:
        emit('summary_generation_failed', **fields, error='invalid_provider_response',
             duration_seconds=round(time.monotonic()-started, 3))
        store.complete(job, error='invalid_provider_response', permanent=True)
    except Exception:
        emit('summary_generation_failed', **fields, error='provider_call_failed',
             duration_seconds=round(time.monotonic()-started, 3))
        # Never persist exception messages: provider errors may contain patient content.
        store.complete(job, error='provider_call_failed')
    else:
        emit('summary_generation_finished', **fields, state='processing',
             duration_seconds=round(time.monotonic()-started, 3))
        store.complete(job, summary=summary)
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='encounters.db')
    parser.add_argument('--log-file', help='Append JSON workflow events to this file (default: stderr)')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s', filename=args.log_file)
    emit('worker_started', database=args.db)
    if not os.environ.get('SUMMARY_URL') and os.environ.get('SUMMARY_MOCK') != '1':
        parser.error('set SUMMARY_URL or explicitly enable SUMMARY_MOCK=1')
    store = Store(args.db)
    while True:
        if not tick(store):
            time.sleep(.2)


if __name__ == '__main__':
    main()
