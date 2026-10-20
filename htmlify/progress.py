import fcntl
import json
import os
from pathlib import Path
import time


PROGRESS_PATH_ENV = 'HTMLIFY_PROGRESS_PATH'
PROGRESS_INTERVAL_ENV = 'HTMLIFY_PROGRESS_INTERVAL'


def format_duration(seconds):
    seconds = max(0, int(round(seconds)))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    parts = []
    if days:
        parts.append(f'{days}d')
    if hours or days:
        parts.append(f'{hours}h')
    if minutes or hours or days:
        parts.append(f'{minutes}m')
    parts.append(f'{seconds}s')
    return ' '.join(parts)


def configure_progress(path, interval=5):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    os.environ[PROGRESS_PATH_ENV] = str(path)
    os.environ[PROGRESS_INTERVAL_ENV] = str(interval)


def clear_progress():
    path = os.environ.pop(PROGRESS_PATH_ENV, None)
    os.environ.pop(PROGRESS_INTERVAL_ENV, None)
    if path:
        Path(path).unlink(missing_ok=True)


def _update_state(callback):
    path_value = os.getenv(PROGRESS_PATH_ENV)
    if not path_value:
        return None
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+', encoding='utf-8') as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.seek(0)
        try:
            state = json.load(handle)
        except (json.JSONDecodeError, ValueError):
            state = {'sites': {}}
        result = callback(state)
        handle.seek(0)
        handle.truncate()
        json.dump(state, handle, separators=(',', ':'))
        handle.flush()
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    return result


def _progress_message(domain, site, now):
    completed = min(site['completed'], site['total'])
    total = site['total']
    elapsed = max(0, now - site['started_at'])
    percentage = (completed / total * 100) if total else 100
    processed = site.get('processed', 0)
    rate = processed / elapsed if processed and elapsed > 0 else 0
    remaining = max(0, total - completed)
    eta = format_duration(remaining / rate) if rate else 'calculating…'
    return (
        f'[htmlify] {domain}: {completed:,}/{total:,} pages '
        f'({percentage:.1f}%) | elapsed {format_duration(elapsed)} '
        f'| ETA {eta}'
        + (f' | {rate:.2f} pages/s' if rate else '')
    )


def initialise_site_progress(domain, total, *, now=None):
    now = time.monotonic() if now is None else now

    def initialise(state):
        sites = state.setdefault('sites', {})
        if domain in sites:
            sites[domain]['total'] = total
            return None
        site = {
            'total': total,
            'completed': 0,
            'processed': 0,
            'started_at': now,
            'last_report_at': now,
        }
        sites[domain] = site
        return _progress_message(domain, site, now)

    if message := _update_state(initialise):
        print(message, flush=True)


def record_page_completion(domain, *, processed, now=None):
    now = time.monotonic() if now is None else now
    try:
        interval = max(0, float(os.getenv(PROGRESS_INTERVAL_ENV, '5')))
    except ValueError:
        interval = 5

    def complete(state):
        site = state.setdefault('sites', {}).get(domain)
        if not site:
            return None
        site['completed'] += 1
        if processed:
            site['processed'] = site.get('processed', 0) + 1
        should_report = (
            site['completed'] >= site['total']
            or now - site.get('last_report_at', 0) >= interval
        )
        if not should_report:
            return None
        site['last_report_at'] = now
        return _progress_message(domain, site, now)

    if message := _update_state(complete):
        print(message, flush=True)


def _batch_progress_message(name, batch, now):
    completed = min(batch['completed'], batch['total'])
    total = batch['total']
    elapsed = max(0, now - batch['started_at'])
    percentage = (completed / total * 100) if total else 100
    processed = batch.get('processed', 0)
    rate = processed / elapsed if processed and elapsed > 0 else 0
    remaining = max(0, total - completed)
    eta = format_duration(remaining / rate) if rate else 'calculating…'
    return (
        f'[htmlify] {name}: {completed:,}/{total:,} sites '
        f'({percentage:.1f}%) | {batch["succeeded"]:,} succeeded '
        f'| {batch["failed"]:,} failed '
        f'| elapsed {format_duration(elapsed)} | ETA {eta}'
    )


def initialise_batch_progress(name, total, *, now=None):
    now = time.monotonic() if now is None else now

    def initialise(state):
        batches = state.setdefault('batches', {})
        if name in batches:
            batches[name]['total'] = total
            return None
        batch = {
            'total': total,
            'completed': 0,
            'processed': 0,
            'succeeded': 0,
            'failed': 0,
            'failures': [],
            'started_at': now,
            'last_report_at': now,
        }
        batches[name] = batch
        return _batch_progress_message(name, batch, now)

    if message := _update_state(initialise):
        print(message, flush=True)


def record_batch_completion(
    name,
    *,
    processed,
    failed=False,
    item=None,
    error=None,
    now=None,
):
    now = time.monotonic() if now is None else now
    try:
        interval = max(0, float(os.getenv(PROGRESS_INTERVAL_ENV, '5')))
    except ValueError:
        interval = 5

    def complete(state):
        batch = state.setdefault('batches', {}).get(name)
        if not batch:
            return None
        batch['completed'] += 1
        batch['failed' if failed else 'succeeded'] += 1
        if failed and item:
            failure = {
                'item': str(item),
                'error': str(error or 'Unknown error'),
            }
            if failure not in batch.setdefault('failures', []):
                batch['failures'].append(failure)
        if processed:
            batch['processed'] += 1
        should_report = (
            batch['completed'] >= batch['total']
            or now - batch.get('last_report_at', 0) >= interval
        )
        if not should_report:
            return None
        batch['last_report_at'] = now
        return _batch_progress_message(name, batch, now)

    if message := _update_state(complete):
        print(message, flush=True)


def get_batch_failures(name):
    def read(state):
        batch = state.setdefault('batches', {}).get(name, {})
        return list(batch.get('failures', []))

    return _update_state(read) or []
