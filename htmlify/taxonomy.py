import json
from functools import lru_cache
from pathlib import Path
from tempfile import NamedTemporaryFile

import phpserialize

from htmlify import config
from htmlify.db import db_manager


TAXONOMY_LOOKUP_VERSION = 1
TAXONOMY_LOOKUP_DIR = config.PROCESSING_DATA_DIR / 'taxonomy-lookups'


def taxonomy_lookup_path(domain):
    return TAXONOMY_LOOKUP_DIR / f'{domain}.json'


@lru_cache(maxsize=None)
def _read_taxonomy_lookup(domain):
    """Read the shared lookup prepared before page workers are launched."""
    path = taxonomy_lookup_path(domain)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError, TypeError):
        return None
    if payload.get('version') != TAXONOMY_LOOKUP_VERSION:
        return None
    if not isinstance(payload.get('terms'), dict):
        return None
    return payload


def prepare_taxonomy_lookup(domain, *, refresh_after=None):
    """Write one term lookup for reuse by every page-worker process.

    Luigi creates a new process for each PageTask, so Python's in-memory
    caches cannot prevent every taxonomy page from reloading the complete
    term table.  This small JSON file makes that site-level query happen once
    per sitemap instead.
    """
    path = taxonomy_lookup_path(domain)
    if path.is_file():
        current = _read_taxonomy_lookup(domain)
        fresh_enough = (
            refresh_after is None
            or path.stat().st_mtime >= float(refresh_after)
        )
        if current is not None and fresh_enough:
            return current

    biological_vids = get_biological_vids(domain)
    rows = db_manager.fetch(
        domain,
        'SELECT tid, vid FROM taxonomy_term_data',
    )
    payload = {
        'version': TAXONOMY_LOOKUP_VERSION,
        'biological_vids': [int(vid) for vid in biological_vids],
        'terms': {
            str(int(tid)): int(vid)
            for tid, vid in rows
        },
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with NamedTemporaryFile(
            mode='w',
            encoding='utf-8',
            dir=path.parent,
            prefix=f'.{path.name}.',
            delete=False,
        ) as temporary:
            json.dump(
                payload,
                temporary,
                ensure_ascii=False,
                separators=(',', ':'),
            )
            temporary_path = Path(temporary.name)
        temporary_path.replace(path)
    finally:
        if temporary_path and temporary_path.exists():
            temporary_path.unlink()

    _read_taxonomy_lookup.cache_clear()
    get_biological_terms.cache_clear()
    get_term_vocabularies.cache_clear()
    return payload


@lru_cache(maxsize=None)
def get_biological_vids(domain):
    """Return the vocabulary IDs configured as biological classifications."""
    row = db_manager.fetch_one(
        domain,
        'SELECT value FROM variable WHERE name="biological_vids"',
    )
    if not row or not row[0]:
        return tuple()

    values = phpserialize.loads(row[0])
    return tuple(
        int(vid)
        for vid, enabled in values.items()
        if enabled
    )


@lru_cache(maxsize=None)
def get_biological_terms(domain):
    """Map biological term IDs to their vocabulary IDs for one site."""
    if lookup := _read_taxonomy_lookup(domain):
        biological_vids = {
            int(vid) for vid in lookup.get('biological_vids', [])
        }
        return {
            int(tid): int(vid)
            for tid, vid in lookup['terms'].items()
            if int(vid) in biological_vids
        }

    vids = get_biological_vids(domain)
    if not vids:
        return {}
    placeholders = ', '.join(str(int(vid)) for vid in vids)
    rows = db_manager.fetch(
        domain,
        f'''
            SELECT tid, vid
            FROM taxonomy_term_data
            WHERE vid IN ({placeholders})
        ''',
    )
    return {int(tid): int(vid) for tid, vid in rows}


def biological_vid_for_term(domain, tid):
    return get_biological_terms(domain).get(int(tid))


@lru_cache(maxsize=None)
def get_term_vocabularies(domain):
    """Map every term ID to its vocabulary ID for one site."""
    if lookup := _read_taxonomy_lookup(domain):
        return {
            int(tid): int(vid)
            for tid, vid in lookup['terms'].items()
        }

    rows = db_manager.fetch(
        domain,
        '''
            SELECT tid, vid
            FROM taxonomy_term_data
        ''',
    )
    return {int(tid): int(vid) for tid, vid in rows}


def vocabulary_for_term(domain, tid):
    return get_term_vocabularies(domain).get(int(tid))


def taxonomy_asset_url(vid):
    return f'/assets/taxonomy/{int(vid)}.json'


def write_taxonomy_index(domain, vid, destination):
    """Write a compact, shared hierarchy index for a vocabulary."""
    vocabulary = db_manager.fetch_one(
        domain,
        f'SELECT name FROM taxonomy_vocabulary WHERE vid = {int(vid)}',
    )
    rows = db_manager.fetch(
        domain,
        f'''
            SELECT td.tid, td.name, COALESCE(th.parent, 0), td.weight
            FROM taxonomy_term_data td
            LEFT JOIN taxonomy_term_hierarchy th ON th.tid = td.tid
            WHERE td.vid = {int(vid)}
            ORDER BY td.weight, td.name, td.tid, th.parent
        ''',
    )
    payload = {
        'version': 1,
        'vid': int(vid),
        'title': vocabulary[0] if vocabulary else 'Taxonomy',
        'nodes': [
            {
                'id': int(tid),
                'parent': int(parent or 0),
                'name': name,
            }
            for tid, name, parent, _weight in rows
        ],
    }

    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    # These indexes are public web assets. NamedTemporaryFile deliberately
    # starts with mode 0600, so explicitly make both the directory and final
    # file readable by the Apache worker after the atomic replacement.
    destination.parent.chmod(0o755)
    with NamedTemporaryFile(
        mode='w',
        encoding='utf-8',
        dir=destination.parent,
        prefix=f'.{destination.name}.',
        delete=False,
    ) as temporary:
        json.dump(
            payload,
            temporary,
            ensure_ascii=False,
            separators=(',', ':'),
        )
        temporary_path = Path(temporary.name)
    temporary_path.replace(destination)
    destination.chmod(0o644)
    return payload
