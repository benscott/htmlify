import hashlib
import json
import re
from collections import OrderedDict
from functools import lru_cache
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

import phpserialize
import requests

from htmlify import config
from htmlify.config import logger
from htmlify.db import db_manager


FACET_PARAMETER = re.compile(r'^f\[\d+\]$')
SOLR_FIELD = re.compile(r'^[A-Za-z0-9_]+$')
RESULT_PATHS = (
    (re.compile(r'^/?node/(\d+)(?:/|$)'), 'node'),
    (re.compile(r'^/?file(?:-colorboxed)?/(\d+)(?:/|$)'), 'file'),
    (re.compile(r'^/?taxonomy/term/(\d+)(?:/|$)'), 'taxonomy_term'),
)

LICENCE_LABELS = {
    '1': 'All Rights Reserved',
    '2': 'Attribution CC BY',
    '3': 'Attribution, Share Alike CC BY-SA',
    '4': 'Attribution, No Derivative Works CC BY-ND',
    '5': 'Attribution, Non-Commercial CC BY-NC',
    '6': 'Attribution, Non-Commercial, Share Alike CC BY-NC-SA',
    '7': 'Attribution, Non-Commercial, No Derivative Works CC BY-NC-ND',
    '8': 'CC0',
    '9': 'Public Domain',
}

DATABASE_LABEL_FIELDS = {
    'tid': ('taxonomy_term_data', 'tid', 'name'),
    'im_field_taxonomic_name': ('taxonomy_term_data', 'tid', 'name'),
    'im_field_imaging_technique': ('taxonomy_term_data', 'tid', 'name'),
    'im_field_keywords': ('taxonomy_term_data', 'tid', 'name'),
    'im_field_preparation_technique': ('taxonomy_term_data', 'tid', 'name'),
    'im_field_smg_galleries': ('node', 'nid', 'title'),
    'is_uid': ('users', 'uid', 'name'),
    'im_biblio_contributor': ('biblio_contributor_data', 'cid', 'name'),
    'im_biblio_keyword': ('biblio_keyword_data', 'kid', 'word'),
    'im_biblio_secondary_title': (
        'biblio_secondary_title',
        'bstid',
        'secondary_title',
    ),
    'tus_biblio_type': ('biblio_types', 'tid', 'name'),
}

# A numeric value in one of these fields is only meaningful while its
# taxonomy term still exists. Drupal does not enforce referential integrity
# on Field API tables, so old records can retain tids for deleted terms.
TAXONOMY_LABEL_FIELDS = {
    field
    for field, (table, _id_column, _label_column) in DATABASE_LABEL_FIELDS.items()
    if table == 'taxonomy_term_data'
}

FACET_LABEL_FIELDS = {
    'taxonomic name': 'im_field_taxonomic_name',
    'terms': 'tid',
    'author': 'im_biblio_contributor',
    'publication year': 'tus_biblio_year',
    'publication type': 'tus_biblio_type',
    'journal keyword': 'im_biblio_keyword',
    'journal': 'im_biblio_secondary_title',
    'filter by contributor': 'is_uid',
    'contributor': 'is_uid',
    'gallery': 'im_field_smg_galleries',
    'licence': 'im_field_cc_licence',
    'media type': 'tus_type_name',
    'creator': 'ss_field_creator',
    'imaging technique': 'im_field_imaging_technique',
    'image keywords': 'im_field_keywords',
    'preparation technique': 'im_field_preparation_technique',
}

FACET_FIELD_ALIASES = {
    # Facet API calls Drupal's core node-author facet ``author``, while the
    # corresponding field stored in Solr is ``is_uid``.
    'author': 'is_uid',
}


def _normalize_facet_field(field):
    # Scratchpads registers hidden ``*_global`` variants of its block facets.
    # Both variants query the same underlying Solr field.
    if field.endswith('_global'):
        field = field[:-len('_global')]
    return FACET_FIELD_ALIASES.get(field, field)


def _facet_parts(href):
    query = parse_qs(urlparse(href).query)
    for key, values in query.items():
        if not FACET_PARAMETER.match(key):
            continue
        for value in values:
            field, separator, raw_value = value.partition(':')
            if separator and SOLR_FIELD.match(field) and raw_value:
                yield field, raw_value


def _link_label(link):
    label = link.get_text(' ', strip=True)
    for hidden in link.select('.element-invisible'):
        hidden_text = hidden.get_text(' ', strip=True)
        if hidden_text:
            label = label.replace(hidden_text, '')
    return re.sub(r'\s*\([\d,]+\)\s*$', '', label).strip()


def _facet_delta_map(domain, fetch):
    if not domain or not fetch:
        return {}
    try:
        rows = fetch(
            domain,
            "SELECT data FROM cache WHERE cid = 'facetapi:delta_map' LIMIT 1",
        )
    except Exception as error:
        logger.warning(f'Could not read Facet API delta map for {domain}: {error}')
        return {}
    if not rows:
        return {}
    decoded = _decode_php(rows[0][0])
    if not isinstance(decoded, dict):
        return {}
    return {
        str(delta).lower(): str(raw_delta).rsplit(':', 1)[-1]
        for delta, raw_delta in decoded.items()
    }


def _field_from_block(block, block_label, delta_map):
    block_id = block.get('id', '')
    prefix = 'block-facetapi-'
    if block_id.startswith(prefix):
        delta = block_id[len(prefix):].lower()
        if field := delta_map.get(delta):
            return _normalize_facet_field(field)
    normalized_label = re.sub(r'\s+', ' ', block_label).strip(' :').lower()
    return FACET_LABEL_FIELDS.get(normalized_label)


def extract_facet_definitions(soup, domain=None, fetch=None):
    """Read the enabled Facet API fields and visible value labels from HTML."""
    fetch = fetch or (db_manager.fetch if domain else None)
    delta_map = None
    facets = OrderedDict()
    for block in soup.select('.block-facetapi'):
        heading = block.select_one('.block-title, h2, h3')
        block_label = heading.get_text(' ', strip=True) if heading else ''
        block_fields = set()
        for link in block.find_all('a', href=True):
            for field, value in _facet_parts(link['href']):
                block_fields.add(field)
                facet = facets.setdefault(field, {
                    'field': field,
                    'label': block_label or field,
                    'observed_values': {},
                })
                label = _link_label(link)
                if label:
                    facet['observed_values'][str(value)] = label
        if block_fields:
            continue

        # Some large facets render an autocomplete form before (or instead of)
        # their value links. Its input name is the Solr field identifier.
        for input_element in block.select('form input[name]'):
            field = _normalize_facet_field(input_element.get('name', ''))
            if SOLR_FIELD.match(field) and field.startswith(('im_', 'is_', 'ss_', 'tus_')):
                facets.setdefault(field, {
                    'field': field,
                    'label': block_label or field,
                    'observed_values': {},
                })
                break

        # Empty Facet API blocks do not expose their field in their markup.
        # Resolve Drupal's hashed block delta back to the configured field.
        if delta_map is None:
            delta_map = _facet_delta_map(domain, fetch)
        field = _field_from_block(block, block_label, delta_map)
        if field and SOLR_FIELD.match(field):
            facets.setdefault(field, {
                'field': field,
                'label': block_label or field,
                'observed_values': {},
            })
    return list(facets.values())


def _context_path_matches(page_path, patterns, front_page='node'):
    """Match Drupal Context path patterns, including exclusions."""
    route = str(page_path or '').strip('/') or str(front_page).strip('/')
    matched = False
    positive_patterns = 0
    negative_patterns = 0
    for raw_pattern in patterns or []:
        pattern = str(raw_pattern or '').strip()
        if not pattern:
            continue
        negative = pattern.startswith('~')
        if negative:
            negative_patterns += 1
            pattern = pattern[1:]
        else:
            positive_patterns += 1
        pattern = pattern.replace('<front>', str(front_page).strip('/'))
        expression = re.escape(pattern).replace(r'\*', '.*')
        if re.fullmatch(expression, route):
            if negative:
                return False
            matched = True
    return matched or (positive_patterns == 0 and negative_patterns > 0)


@lru_cache(maxsize=None)
def _drupal_facet_configuration(path):
    try:
        payload = json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError, TypeError) as error:
        logger.warning(f'Could not read Drupal facet configuration {path}: {error}')
        return {}
    return payload if isinstance(payload, dict) else {}


def configured_facet_definitions(page_path, config_path=None):
    """Return facets assigned to this route by Drupal Context configuration."""
    config_path = config_path or config.DRUPAL_FACET_CONFIG_PATH
    if not config_path:
        return []
    payload = _drupal_facet_configuration(str(config_path))
    configured = []
    sequence = 0
    for context in payload.get('contexts', []):
        if not isinstance(context, dict):
            continue
        paths = context.get('paths', [])
        if not paths or not _context_path_matches(page_path, paths):
            continue

        # OR-mode Contexts become active when their path condition matches.
        # For an AND-mode context, path alone is sufficient only when it is
        # the sole condition. Other conditions require a live Drupal request.
        conditions = context.get('conditions', [])
        if context.get('condition_mode') and set(conditions) != {'path'}:
            continue

        for facet in context.get('facets', []):
            if not isinstance(facet, dict):
                continue
            field = _normalize_facet_field(str(facet.get('field', '')))
            if not field or not SOLR_FIELD.fullmatch(field):
                continue
            configured.append((int(facet.get('weight', 0)), sequence, field, facet))
            sequence += 1

    facets = OrderedDict()
    for _weight, _sequence, field, facet in sorted(configured):
        facets.setdefault(field, {
            'field': field,
            'label': str(facet.get('label') or field),
            'observed_values': {},
        })
    return list(facets.values())


def merge_facet_definitions(*groups):
    """Merge configured and rendered facets without losing visible labels."""
    facets = OrderedDict()
    for group in groups:
        for facet in group or []:
            field = _normalize_facet_field(str(facet.get('field', '')))
            if not field or not SOLR_FIELD.fullmatch(field):
                continue
            if field not in facets:
                facets[field] = {
                    'field': field,
                    'label': str(facet.get('label') or field),
                    'observed_values': {},
                }
            elif facet.get('label'):
                # Rendered block labels are passed last and reflect any
                # route-specific hook_block_view_alter() customisation.
                facets[field]['label'] = str(facet['label'])
            facets[field]['observed_values'].update({
                str(value): str(label)
                for value, label in facet.get('observed_values', {}).items()
            })
    return list(facets.values())


def facet_configuration_fingerprint(page_path, config_path=None):
    """Return a stable marker for the effective configured facets on a route."""
    facets = configured_facet_definitions(page_path, config_path)
    if not facets:
        return None
    canonical = [
        {'field': facet['field'], 'label': facet['label']}
        for facet in facets
    ]
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        separators=(',', ':'),
    ).encode('utf-8')
    return hashlib.sha1(encoded).hexdigest()[:12]


def extract_result_hints(soup):
    """Return entity type/id pairs from the current page of search results."""
    content = (
        soup.select_one('#block-system-main')
        or soup.select_one('#region-content')
        or soup.select_one('main')
        or soup.select_one('[role="main"]')
    )
    if not content:
        return []

    hints = []
    seen = set()
    for link in content.find_all('a', href=True):
        path = urlparse(link['href']).path
        for pattern, entity_type in RESULT_PATHS:
            match = pattern.match(path)
            if not match:
                continue
            hint = (entity_type, match.group(1))
            if hint not in seen:
                seen.add(hint)
                hints.append(hint)
            break
        if len(hints) >= 30:
            break
    return hints


def facet_asset_name(page_path):
    path = '/' + str(page_path or '').strip('/')
    readable = re.sub(r'[^a-z0-9]+', '-', path.lower()).strip('-') or 'front'
    digest = hashlib.sha1(path.encode('utf-8')).hexdigest()[:10]
    return f'{readable[:80]}-{digest}.json'


def facet_asset_url(page_path):
    return f'/assets/facets/{facet_asset_name(page_path)}'


def _decode_php(value):
    if value is None:
        return None
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, str):
        value = value.encode('utf-8')
    try:
        return phpserialize.loads(value, decode_strings=True)
    except (TypeError, ValueError):
        return value.decode('utf-8') if isinstance(value, bytes) else value


def _list_values(value):
    if isinstance(value, dict):
        return list(value.values())
    if isinstance(value, (list, tuple, set)):
        return list(value)
    if value in (None, ''):
        return []
    return [value]


def _search_page_rows(domain, fetch):
    try:
        return fetch(
            domain,
            'SELECT search_path, env_id, settings '
            'FROM apachesolr_search_page',
        )
    except Exception as error:
        logger.warning(f'Could not read Solr search pages for {domain}: {error}')
        return []


@lru_cache(maxsize=None)
def _cached_search_page_rows(domain):
    return tuple(_search_page_rows(domain, db_manager.fetch))


def _route_matches(pattern, page_path):
    pattern = str(pattern or '').strip('/')
    route = str(page_path or '').strip('/')
    expression = re.escape(pattern).replace('%', r'[^/]+')
    return bool(re.fullmatch(expression, route))


def is_configured_facet_page(domain, page_path, fetch=None):
    route = str(page_path or '').strip('/')
    if route == 'search' or route.startswith('search/'):
        return False
    rows = (
        _search_page_rows(domain, fetch)
        if fetch else _cached_search_page_rows(domain)
    )
    return any(
        _route_matches(search_path, route)
        for search_path, _env_id, _settings in rows
    )


def _search_page_settings(domain, page_path, fetch):
    route = str(page_path or '').strip('/')
    rows = _search_page_rows(domain, fetch)
    for search_path, env_id, serialized_settings in rows:
        if not _route_matches(search_path, route):
            continue
        settings = _decode_php(serialized_settings)
        settings = settings if isinstance(settings, dict) else {}
        filters = [
            str(value)
            for value in _list_values(settings.get('fq'))
            if value not in (None, '')
        ]
        return filters, env_id
    return [], None


def _site_hash(domain, fetch):
    rows = fetch(
        domain,
        "SELECT value FROM variable "
        "WHERE name = 'apachesolr_site_hash' LIMIT 1",
    )
    if not rows:
        return None
    value = _decode_php(rows[0][0])
    return str(value) if value else None


def _solr_url(domain, env_id, fetch, override=None):
    url = override or config.SOLR_BASE_URL
    if not url:
        condition = ''
        if env_id:
            safe_env_id = str(env_id).replace("'", "''")
            condition = f" WHERE env_id = '{safe_env_id}'"
        rows = fetch(
            domain,
            f'SELECT url FROM apachesolr_environment{condition} LIMIT 1',
        )
        url = rows[0][0] if rows else None
    if not url:
        raise RuntimeError(
            f'No Solr URL is configured for faceted page on {domain}. '
            'Set SOLR_BASE_URL or use --solr-url.'
        )
    url = str(url).rstrip('/')
    return url if url.endswith('/select') else f'{url}/select'


def _solr_json(solr_get, url, params):
    response = solr_get(url, params=params, timeout=config.REQUEST_TIMEOUT)
    if isinstance(response, dict):
        return response
    response.raise_for_status()
    return response.json()


def _infer_filters(solr_get, solr_url, filters, result_hints):
    grouped = OrderedDict()
    for entity_type, entity_id in result_hints:
        grouped.setdefault(entity_type, []).append(str(entity_id))
    if not grouped:
        return []

    clauses = []
    for entity_type, entity_ids in grouped.items():
        ids = ' '.join(entity_ids)
        clauses.append(f'(entity_type:{entity_type} AND entity_id:({ids}))')
    payload = _solr_json(solr_get, solr_url, {
        'q': ' OR '.join(clauses),
        'fq': filters,
        'fl': 'entity_type,bundle',
        'rows': 100,
        'wt': 'json',
    })
    docs = payload.get('response', {}).get('docs', [])
    entity_types = sorted({str(doc['entity_type']) for doc in docs if doc.get('entity_type')})
    bundles = sorted({str(doc['bundle']) for doc in docs if doc.get('bundle')})
    inferred = []
    if entity_types:
        inferred.append('entity_type:(' + ' OR '.join(entity_types) + ')')
    if bundles:
        inferred.append('bundle:(' + ' OR '.join(bundles) + ')')
    return inferred


def _fetch_documents(solr_get, solr_url, filters, facet_fields):
    fields = ['id', 'entity_id', 'entity_type', 'bundle', 'label', 'path', 'url']
    fields.extend(field for field in facet_fields if field not in fields)
    documents = []
    start = 0
    rows = 2000
    total = None

    while total is None or start < total:
        payload = _solr_json(solr_get, solr_url, {
            'q': '*:*',
            'fq': filters,
            'fl': ','.join(fields),
            'sort': 'id asc',
            'start': start,
            'rows': rows,
            'wt': 'json',
        })
        response = payload.get('response', {})
        batch = response.get('docs', [])
        total = int(response.get('numFound', len(batch)))
        if total > config.FACET_INDEX_MAX_RECORDS:
            raise RuntimeError(
                f'Faceted page contains {total} records, exceeding '
                f'FACET_INDEX_MAX_RECORDS={config.FACET_INDEX_MAX_RECORDS}'
            )
        documents.extend(batch)
        if not batch:
            break
        start += len(batch)
    return documents


def _filter_terms(filters, field):
    values = []
    prefix = f'{field}:'
    for filter_value in filters:
        value = str(filter_value).strip()
        if value.startswith('-') or not value.startswith(prefix):
            continue
        expression = value[len(prefix):].strip()
        if expression.startswith('(') and expression.endswith(')'):
            expression = expression[1:-1]
        for term in re.split(r'\s+OR\s+', expression):
            term = term.strip().strip('"\'')
            if re.fullmatch(r'[A-Za-z0-9_]+', term):
                values.append(term)
    return list(dict.fromkeys(values))


def _id_chunks(values, size=500):
    values = sorted({int(value) for value in values})
    for offset in range(0, len(values), size):
        yield values[offset:offset + size]


def _append_values(documents, rows, field):
    for entity_id, value in rows:
        document = documents.get(int(entity_id))
        if not document or value in (None, ''):
            continue
        document.setdefault(field, []).append(value)


def _drupal_field_name(solr_field):
    for prefix in ('im_', 'is_', 'sm_', 'ss_', 'tm_', 'ts_', 'tus_'):
        if solr_field.startswith(prefix):
            field_name = solr_field[len(prefix):]
            return field_name if field_name.startswith('field_') else None
    return None


def _drupal_field_value_column(domain, field_name, fetch):
    """Discover a Drupal 7 Field API storage column without field hard-coding."""
    if not SOLR_FIELD.fullmatch(field_name):
        return None
    table = f'field_data_{field_name}'
    try:
        rows = fetch(domain, f'SHOW COLUMNS FROM {table}')
    except Exception:
        return None
    columns = [str(row[0]) for row in rows]
    base_columns = {
        'entity_type', 'bundle', 'deleted', 'entity_id', 'revision_id',
        'language', 'delta',
    }
    value_columns = [column for column in columns if column not in base_columns]
    suffix_priority = (
        '_tid', '_target_id', '_nid', '_fid', '_licence', '_value',
    )
    for suffix in suffix_priority:
        for column in value_columns:
            if column.endswith(suffix):
                return column
    return value_columns[0] if value_columns else None


def _enrich_drupal_fields(
    domain,
    documents,
    entity_type,
    facet_fields,
    fetch,
    *,
    skip=(),
):
    """Read configured Field API facets directly from their storage tables."""
    if not documents:
        return
    skip = set(skip)
    for facet_field in facet_fields:
        if facet_field in skip:
            continue
        field_name = _drupal_field_name(facet_field)
        if not field_name:
            continue
        value_column = _drupal_field_value_column(domain, field_name, fetch)
        if not value_column or not SOLR_FIELD.fullmatch(value_column):
            continue
        table = f'field_data_{field_name}'
        for chunk in _id_chunks(documents):
            identifiers = ','.join(str(identifier) for identifier in chunk)
            try:
                rows = fetch(
                    domain,
                    f'SELECT entity_id, {value_column} FROM {table} '
                    "WHERE deleted = 0 "
                    f"AND entity_type = '{entity_type}' "
                    f'AND entity_id IN ({identifiers})',
                )
                _append_values(documents, rows, facet_field)
            except Exception as error:
                logger.warning(
                    f'Could not read Drupal field facet {facet_field} '
                    f'for {domain}: {error}'
                )
                break


def _enrich_node_documents(domain, documents, facet_fields, fetch):
    if not documents:
        return
    ids = list(documents)
    for chunk in _id_chunks(ids):
        id_list = ','.join(str(identifier) for identifier in chunk)
        if any(field.startswith(('tus_biblio_', 'im_biblio_')) for field in facet_fields):
            try:
                rows = fetch(
                    domain,
                    'SELECT nid, biblio_year, biblio_type, '
                    'biblio_secondary_title FROM biblio '
                    f'WHERE nid IN ({id_list})',
                )
                for nid, year, biblio_type, secondary_title in rows:
                    document = documents.get(int(nid))
                    if not document:
                        continue
                    if 'tus_biblio_year' in facet_fields and year not in (None, ''):
                        document['tus_biblio_year'] = year
                    if 'tus_biblio_type' in facet_fields and biblio_type not in (None, ''):
                        document['tus_biblio_type'] = biblio_type
                    if (
                        'im_biblio_secondary_title' in facet_fields
                        and secondary_title not in (None, '')
                    ):
                        document['im_biblio_secondary_title'] = secondary_title
            except Exception as error:
                logger.warning(f'Could not read biblio records for {domain}: {error}')

        relation_fields = (
            ('im_biblio_contributor', 'biblio_contributor', 'cid'),
            ('im_biblio_keyword', 'biblio_keyword', 'kid'),
        )
        for field, table, value_column in relation_fields:
            if field not in facet_fields:
                continue
            try:
                rows = fetch(
                    domain,
                    f'SELECT nid, {value_column} FROM {table} '
                    f'WHERE nid IN ({id_list})',
                )
                _append_values(documents, rows, field)
            except Exception as error:
                logger.warning(f'Could not read {field} values for {domain}: {error}')

        if 'im_field_taxonomic_name' in facet_fields:
            try:
                rows = fetch(
                    domain,
                    'SELECT entity_id, field_taxonomic_name_tid '
                    'FROM field_data_field_taxonomic_name '
                    "WHERE deleted = 0 AND entity_type = 'node' "
                    f'AND entity_id IN ({id_list})',
                )
                if 'im_field_taxonomic_name' in facet_fields:
                    _append_values(documents, rows, 'im_field_taxonomic_name')
            except Exception as error:
                logger.warning(
                    f'Could not read taxonomic names for {domain}: {error}'
                )


def _database_documents(domain, filters, result_hints, facet_fields, fetch):
    """Build basic Solr-shaped documents directly from Drupal storage."""
    entity_types = _filter_terms(filters, 'entity_type')
    bundles = _filter_terms(filters, 'bundle')
    hinted = OrderedDict()
    for entity_type, entity_id in result_hints:
        if str(entity_id).isdigit():
            hinted.setdefault(entity_type, []).append(int(entity_id))

    documents = []
    if 'node' in entity_types or (not entity_types and hinted.get('node')):
        conditions = ['status = 1']
        safe_bundles = [bundle for bundle in bundles if SOLR_FIELD.match(bundle)]
        if safe_bundles:
            quoted = ','.join(f"'{bundle}'" for bundle in safe_bundles)
            conditions.append(f'type IN ({quoted})')
        elif hinted.get('node'):
            ids = ','.join(str(identifier) for identifier in hinted['node'])
            conditions.append(f'nid IN ({ids})')
        try:
            rows = fetch(
                domain,
                'SELECT nid, title, uid, type FROM node WHERE '
                + ' AND '.join(conditions),
            )
            node_documents = {
                int(nid): {
                    'id': f'database/node/{nid}',
                    'entity_id': int(nid),
                    'entity_type': 'node',
                    'bundle': node_type,
                    'label': title,
                    'path': f'node/{nid}',
                    'is_uid': uid,
                }
                for nid, title, uid, node_type in rows
            }
            _enrich_node_documents(
                domain,
                node_documents,
                facet_fields,
                fetch,
            )
            _enrich_drupal_fields(
                domain,
                node_documents,
                'node',
                facet_fields,
                fetch,
                skip={'im_field_taxonomic_name'},
            )
            documents.extend(node_documents.values())
        except Exception as error:
            logger.warning(f'Could not export node records for {domain}: {error}')

    if 'file' in entity_types or (not entity_types and hinted.get('file')):
        conditions = ['status = 1']
        safe_bundles = [bundle for bundle in bundles if SOLR_FIELD.match(bundle)]
        if safe_bundles:
            quoted = ','.join(f"'{bundle}'" for bundle in safe_bundles)
            conditions.append(f'type IN ({quoted})')
        elif hinted.get('file'):
            ids = ','.join(str(identifier) for identifier in hinted['file'])
            conditions.append(f'fid IN ({ids})')
        try:
            rows = fetch(
                domain,
                'SELECT fid, filename, uid, type, uri FROM file_managed WHERE '
                + ' AND '.join(conditions),
            )
            file_documents = {}
            for fid, filename, uid, file_type, uri in rows:
                file_documents[int(fid)] = {
                    'id': f'database/file/{fid}',
                    'entity_id': int(fid),
                    'entity_type': 'file',
                    'bundle': file_type,
                    'label': filename,
                    'path': f'file/{fid}',
                    'is_uid': uid,
                    'tus_type_name': file_type,
                }
                thumbnail = _file_thumbnail_url(domain, uri, file_type, filename)
                if thumbnail:
                    file_documents[int(fid)]['thumbnail'] = thumbnail
            _enrich_drupal_fields(
                domain,
                file_documents,
                'file',
                facet_fields,
                fetch,
            )
            documents.extend(file_documents.values())
        except Exception as error:
            logger.warning(f'Could not export file records for {domain}: {error}')

    if len(documents) > config.FACET_INDEX_MAX_RECORDS:
        raise RuntimeError(
            f'Faceted page contains {len(documents)} database records, exceeding '
            f'FACET_INDEX_MAX_RECORDS={config.FACET_INDEX_MAX_RECORDS}'
        )
    return documents


def _merge_documents(solr_documents, database_documents, facet_fields):
    merged = OrderedDict()
    for document in solr_documents + database_documents:
        key = (
            str(document.get('entity_type') or ''),
            str(document.get('entity_id') or ''),
        )
        if key not in merged:
            merged[key] = document
            continue
        existing = merged[key]
        for field in [*facet_fields, 'thumbnail']:
            if not _list_values(existing.get(field)) and _list_values(document.get(field)):
                existing[field] = document[field]
    return list(merged.values())


def _file_thumbnail_url(domain, uri, file_type, filename):
    """Return an archived public image URL suitable for a thumbnail card."""
    image_extensions = {'.avif', '.gif', '.jpeg', '.jpg', '.png', '.webp'}
    is_image = (
        str(file_type).lower() == 'image'
        or Path(str(filename)).suffix.lower() in image_extensions
    )
    uri = str(uri or '')
    if not is_image:
        return None
    if uri.startswith('public://'):
        relative_path = (
            'public/'
            + quote(uri[len('public://'):].lstrip('/'), safe='/')
        )
    elif uri.startswith(('http://', 'https://')):
        # Drupal's image-style stream wrapper stores remote derivatives below
        # ``styles/<style>/http(s)/<host>/...`` and percent-encodes any query
        # string as part of the filename.
        relative_path = quote(
            uri.replace('://', '/', 1).lstrip('/'),
            safe='/',
        )
    else:
        return None
    return (
        f'/sites/{domain}/files/styles/square_thumbnail/{relative_path}'
    )


def _numeric_values(values):
    return sorted({str(value) for value in values if str(value).isdigit()}, key=int)


def _database_labels(domain, field, values, fetch):
    if field == 'im_field_cc_licence':
        return {value: LICENCE_LABELS[value] for value in values if value in LICENCE_LABELS}
    specification = DATABASE_LABEL_FIELDS.get(field)
    if not specification:
        return {}
    numeric = _numeric_values(values)
    if not numeric:
        return {}

    table, id_column, label_column = specification
    labels = {}
    for offset in range(0, len(numeric), 500):
        ids = ','.join(numeric[offset:offset + 500])
        try:
            rows = fetch(
                domain,
                f'SELECT {id_column}, {label_column} FROM {table} '
                f'WHERE {id_column} IN ({ids})',
            )
        except Exception as error:
            logger.warning(f'Could not map facet labels for {field}: {error}')
            return labels
        for identifier, label in rows:
            if label not in (None, ''):
                labels[str(identifier)] = str(label)
    return labels


def _record_url(document):
    path = document.get('path') or ''
    if path:
        parsed = urlparse(str(path))
        route = parsed.path or str(path)
        return '/' + route.lstrip('/')
    url = document.get('url') or ''
    parsed = urlparse(str(url))
    return '/' + parsed.path.lstrip('/') if parsed.path else '/'


def build_facet_index(
    domain,
    page_path,
    page_title,
    facets,
    result_hints=None,
    *,
    fetch=None,
    solr_get=None,
    solr_url=None,
):
    """Export the records and values used by one Drupal Facet API page."""
    fetch = fetch or db_manager.fetch
    solr_get = solr_get or requests.get
    result_hints = result_hints or []

    base_filters, env_id = _search_page_settings(domain, page_path, fetch)
    site_hash = _site_hash(domain, fetch)
    filters = [f'hash:{site_hash}'] if site_hash else []
    filters.extend(base_filters)
    facet_fields = [facet['field'] for facet in facets]
    solr_documents = []
    try:
        endpoint = _solr_url(domain, env_id, fetch, override=solr_url)
        if not base_filters:
            filters.extend(
                _infer_filters(solr_get, endpoint, filters, result_hints)
            )
        if base_filters or _filter_terms(filters, 'entity_type'):
            solr_documents = _fetch_documents(
                solr_get,
                endpoint,
                filters,
                facet_fields,
            )
    except Exception as error:
        logger.warning(
            f'Could not export Solr records for faceted page {page_path}; '
            f'using Drupal database fallback: {error}'
        )

    database_documents = _database_documents(
        domain,
        filters,
        result_hints,
        facet_fields,
        fetch,
    )
    documents = _merge_documents(
        solr_documents,
        database_documents,
        facet_fields,
    )

    all_values = {field: set() for field in facet_fields}
    records = []
    for document in documents:
        record_facets = {}
        for field in facet_fields:
            values = [str(value) for value in _list_values(document.get(field))]
            values = list(dict.fromkeys(value for value in values if value != ''))
            if values:
                record_facets[field] = values
                all_values[field].update(values)
        record = {
            'id': str(document.get('id') or document.get('entity_id') or ''),
            'title': str(document.get('label') or 'Untitled record'),
            'url': _record_url(document),
            'facets': record_facets,
        }
        if document.get('thumbnail'):
            record['thumbnail'] = str(document['thumbnail'])
        records.append(record)

    output_facets = []
    resolved_taxonomy_values = {}
    for facet in facets:
        field = facet['field']
        values = all_values[field]
        # Drupal would not offer a useful control for a facet with no values.
        # This is especially common when a shared Context assigns optional
        # Field API facets to several Solr-powered routes.
        if not values:
            continue
        labels = _database_labels(domain, field, values, fetch)
        labels.update({
            str(value): str(label)
            for value, label in facet.get('observed_values', {}).items()
            if str(value) not in labels
        })
        if field in TAXONOMY_LABEL_FIELDS:
            # Field API rows can outlive their referenced taxonomy terms.
            # Do not expose those orphaned tids as if they were term names.
            labels = {
                value: label
                for value, label in labels.items()
                if value in values and label and label != value
            }
            resolved_taxonomy_values[field] = set(labels)
            if not labels:
                continue
        else:
            for value in values:
                labels.setdefault(value, value)
        output_facets.append({
            'field': field,
            'label': facet['label'],
            'values': labels,
        })

    # Keep orphaned taxonomy references out of each record as well as the
    # filter controls. Other numeric facet fields (years, user IDs, etc.)
    # retain their existing fallback behaviour.
    for record in records:
        for field, allowed_values in resolved_taxonomy_values.items():
            if field not in record['facets']:
                continue
            values = [
                value for value in record['facets'][field]
                if value in allowed_values
            ]
            if values:
                record['facets'][field] = values
            else:
                del record['facets'][field]

    return {
        'version': 3,
        'page': {
            'path': '/' + str(page_path or '').strip('/'),
            'title': page_title,
            'display': (
                'thumbnails'
                if any(doc.get('entity_type') == 'file' for doc in documents)
                else 'list'
            ),
        },
        'facets': output_facets,
        'records': records,
    }


def write_facet_index(domain, page_path, page_title, facets, result_hints, destination, **kwargs):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    data = build_facet_index(
        domain,
        page_path,
        page_title,
        facets,
        result_hints,
        **kwargs,
    )
    destination.write_text(
        json.dumps(data, ensure_ascii=False, separators=(',', ':')),
        encoding='utf-8',
    )
    logger.info(
        f"Wrote {len(data['records'])} records for faceted page {page_path} "
        f'to {destination}'
    )
    return data
