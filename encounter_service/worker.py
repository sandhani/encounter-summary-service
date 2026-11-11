import argparse
import json
import os
import time
import urllib.request
import urllib.error
from .store import Store


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
    try:
        summary = provider(job['transcription'])
    except urllib.error.HTTPError as exc:
        store.complete(job, error=f'provider_http_{exc.code}',
                       permanent=400 <= exc.code < 500 and exc.code not in (408, 429))
    except ValueError:
        store.complete(job, error='invalid_provider_response', permanent=True)
    except Exception:
        # Never persist exception messages: provider errors may contain patient content.
        store.complete(job, error='provider_call_failed')
    else:
        store.complete(job, summary=summary)
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='encounters.db')
    args = parser.parse_args()
    if not os.environ.get('SUMMARY_URL') and os.environ.get('SUMMARY_MOCK') != '1':
        parser.error('set SUMMARY_URL or explicitly enable SUMMARY_MOCK=1')
    store = Store(args.db)
    while True:
        if not tick(store):
            time.sleep(.2)


if __name__ == '__main__':
    main()
