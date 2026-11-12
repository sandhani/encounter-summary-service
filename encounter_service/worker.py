import argparse
import logging
import time
import urllib.error
from .store import Store
from .workflow_log import emit
from .summary_adapters import build_adapter, PROMPT_VERSION


def generate_summary(transcription):
    """Compatibility entry point; worker startup configures one reusable adapter."""
    return build_adapter()(transcription)


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
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--use-llm', dest='use_llm', action='store_true', help='Use the OpenAI LLM adapter')
    mode.add_argument('--no-use-llm', dest='use_llm', action='store_false', help='Disable LLM; use HTTP/mock configuration')
    parser.set_defaults(use_llm=None)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s', filename=args.log_file)
    try:
        adapter = build_adapter(args.use_llm)
    except ValueError as exc:
        parser.error(str(exc))
    emit('worker_started', database=args.db, adapter=type(adapter).__name__,
         model=getattr(adapter, 'model', None), prompt_version=PROMPT_VERSION,
         summary_max_words=getattr(adapter, 'max_words', None))
    store = Store(args.db)
    while True:
        if not tick(store, adapter):
            time.sleep(.2)


if __name__ == '__main__':
    main()
