import json
from functools import lru_cache
from pathlib import Path
from tempfile import NamedTemporaryFile

import phpserialize

from htmlify.db import db_manager


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
