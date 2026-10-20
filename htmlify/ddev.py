import json
import re
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import yaml


class DdevError(RuntimeError):
    pass


_FACET_CONFIG_BEGIN = 'HTMLIFY_FACET_CONFIG_BEGIN'
_FACET_CONFIG_END = 'HTMLIFY_FACET_CONFIG_END'


# This is deliberately evaluated by Drupal rather than duplicating Facet API
# and Context's CTools configuration loading in Python. It therefore includes
# code-exported defaults as well as per-site database overrides.
_FACET_CONFIG_PHP = r'''
chdir('/var/www/html');
define('DRUPAL_ROOT', getcwd());
$_SERVER['HTTP_HOST'] = getenv('HTMLIFY_DRUPAL_HOST');
$_SERVER['SERVER_NAME'] = $_SERVER['HTTP_HOST'];
$_SERVER['REQUEST_URI'] = '/';
$_SERVER['REQUEST_METHOD'] = 'GET';
$_SERVER['SCRIPT_NAME'] = '/index.php';
$_SERVER['REMOTE_ADDR'] = '127.0.0.1';
$_GET['q'] = '';
require_once DRUPAL_ROOT . '/includes/bootstrap.inc';
drupal_bootstrap(DRUPAL_BOOTSTRAP_FULL);

$result = array('version' => 1, 'contexts' => array());
if (module_exists('context') && module_exists('facetapi')) {
  module_load_include('inc', 'facetapi', 'facetapi.block');
  $delta_map = facetapi_get_delta_map();
  foreach (context_enabled_contexts(TRUE) as $context) {
    if (empty($context->reactions['block']['blocks'])) {
      continue;
    }
    $facets = array();
    foreach ($context->reactions['block']['blocks'] as $block) {
      if (!is_array($block) || empty($block['module']) || $block['module'] !== 'facetapi' || !isset($block['delta'])) {
        continue;
      }
      $delta = $block['delta'];
      $raw_delta = isset($delta_map[$delta]) ? $delta_map[$delta] : $delta;
      $parts = facetapi_parse_delta($raw_delta);
      if (count($parts) !== 3 || empty($parts[0]) || empty($parts[2])) {
        continue;
      }
      $definition = facetapi_facet_load($parts[2], $parts[0]);
      $label = is_array($definition) && !empty($definition['label']) ? $definition['label'] : $parts[2];
      // Apply the same route-aware label alterations Drupal uses when
      // rendering the block (for example Author -> Contributor in galleries).
      $block_data = array(
        'subject' => theme('facetapi_title', array(
          'title' => $label,
          'facet' => $definition,
        )),
        'content' => array(),
      );
      $block_object = (object) array(
        'module' => 'facetapi',
        'delta' => $delta,
        'context' => $context->name,
      );
      drupal_alter('block_view', $block_data, $block_object);
      if (!empty($block_data['subject'])) {
        $label = $block_data['subject'];
      }
      $facets[] = array(
        'field' => $parts[2],
        'label' => $label,
        'searcher' => $parts[0],
        'realm' => $parts[1],
        'weight' => isset($block['weight']) ? (int) $block['weight'] : 0,
      );
    }
    if (!$facets) {
      continue;
    }
    usort($facets, function ($a, $b) {
      return $a['weight'] == $b['weight'] ? 0 : ($a['weight'] < $b['weight'] ? -1 : 1);
    });
    $conditions = !empty($context->conditions) && is_array($context->conditions) ? array_keys($context->conditions) : array();
    $paths = array();
    if (!empty($context->conditions['path']['values'])) {
      $paths = array_values($context->conditions['path']['values']);
    }
    $result['contexts'][] = array(
      'name' => $context->name,
      'condition_mode' => isset($context->condition_mode) ? (int) $context->condition_mode : 0,
      'conditions' => $conditions,
      'paths' => $paths,
      'facets' => $facets,
    );
  }
}
print "\nHTMLIFY_FACET_CONFIG_BEGIN\n";
print drupal_json_encode($result);
print "\nHTMLIFY_FACET_CONFIG_END\n";
'''


def _solr_url(project_dir):
    config_path = project_dir / '.ddev' / 'config.yaml'
    if not config_path.exists():
        return None
    try:
        ddev_config = yaml.safe_load(config_path.read_text(encoding='utf-8')) or {}
    except (OSError, yaml.YAMLError):
        return None
    environment = ddev_config.get('web_environment', [])
    if isinstance(environment, dict):
        environment = [f'{key}={value}' for key, value in environment.items()]
    for setting in environment:
        name, separator, value = str(setting).partition('=')
        if separator and name == 'SOLR_DEFAULT_URL':
            return value
    return None


def export_facet_configuration(project_dir, base_url):
    """Load effective Context/Facet API configuration from DDEV Drupal."""
    project_dir = Path(project_dir).expanduser().resolve()
    hostname = urlparse(str(base_url)).hostname
    if not hostname:
        raise DdevError(f'Could not determine Drupal hostname from {base_url}')
    php = _FACET_CONFIG_PHP.replace(
        "getenv('HTMLIFY_DRUPAL_HOST')",
        json.dumps(hostname),
    )
    try:
        result = subprocess.run(
            [
                'ddev',
                'exec',
                '--raw',
                '--',
                'php',
                '-r',
                php,
            ],
            cwd=project_dir,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        stderr = getattr(error, 'stderr', '') or ''
        detail = stderr.strip() or str(error)
        raise DdevError(
            f'Could not load Drupal facet configuration from {project_dir}: {detail}'
        ) from error

    match = re.search(
        rf'{_FACET_CONFIG_BEGIN}\s*(.*?)\s*{_FACET_CONFIG_END}',
        result.stdout,
        re.DOTALL,
    )
    if not match:
        raise DdevError(
            'Drupal facet configuration command returned no JSON payload'
        )
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as error:
        raise DdevError(
            f'Drupal returned invalid facet configuration JSON: {error}'
        ) from error
    if not isinstance(payload, dict) or not isinstance(payload.get('contexts'), list):
        raise DdevError('Drupal returned an invalid facet configuration payload')
    return payload


def describe(project_dir):
    """Return the useful connection details from ``ddev describe -j``."""
    project_dir = Path(project_dir).expanduser().resolve()
    try:
        result = subprocess.run(
            ['ddev', 'describe', '-j'],
            cwd=project_dir,
            check=True,
            capture_output=True,
            text=True,
        )
        payload = json.loads(result.stdout)
        raw = payload['raw']
        db = raw['dbinfo']
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as error:
        raise DdevError(f'Could not read DDEV configuration from {project_dir}: {error}') from error

    if raw.get('status') != 'running':
        raise DdevError(f'DDEV project at {project_dir} is not running')

    return {
        'base_url': raw['primary_url'],
        'db_host': '127.0.0.1',
        'db_port': db['published_port'],
        'db_name': db['dbname'],
        'db_username': db['username'],
        'db_password': db['password'],
        'solr_url': _solr_url(project_dir),
        'platform_path': Path(raw['approot']),
    }
