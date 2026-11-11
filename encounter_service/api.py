import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit
from .store import Conflict, Store


def handler(store):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass  # Request paths may contain identifiers; no default access log.

        def respond(self, status, body):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            if self.path != '/events':
                return self.respond(404, {'error': 'not_found'})
            try:
                size = int(self.headers.get('Content-Length', '0'))
                if size < 1 or size > 2_000_000:
                    return self.respond(413, {'error': 'body_size_limit'})
                event = json.loads(self.rfile.read(size))
                if not isinstance(event, dict):
                    raise ValueError()
                result = store.ingest(event)
                self.respond(202 if result['outcome']=='accepted_current' else 200, result)
            except Conflict as exc:
                self.respond(409, {'error': str(exc)})
            except (ValueError, KeyError, TypeError):
                self.respond(400, {'error': 'invalid_request'})
            except Exception:
                self.respond(503, {'error': 'storage_unavailable'})

        def do_GET(self):
            parts = urlsplit(self.path).path.strip('/').split('/')
            try:
                if parts == ['health']:
                    with store.tx() as db:
                        db.execute('SELECT 1')
                    return self.respond(200, {'status': 'ok'})
                if len(parts)==3 and parts[0]=='encounters' and parts[2]=='summary':
                    return self.respond(200, store.progress(unquote(parts[1])))
                if len(parts)==4 and parts[0]=='encounters' and parts[2]=='versions':
                    return self.respond(200, store.history(unquote(parts[1]), int(parts[3])))
                self.respond(404, {'error': 'not_found'})
            except KeyError:
                self.respond(404, {'error': 'not_found'})
            except ValueError:
                self.respond(400, {'error': 'invalid_version'})
            except Exception:
                self.respond(503, {'error': 'storage_unavailable'})
    return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='encounters.db')
    parser.add_argument('--port', type=int, default=8000)
    args = parser.parse_args()
    ThreadingHTTPServer(('127.0.0.1', args.port), handler(Store(args.db))).serve_forever()


if __name__ == '__main__':
    main()
