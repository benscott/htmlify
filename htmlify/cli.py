import typer
from typing import List, Optional
import luigi
from luigi.execution_summary import LuigiStatusCode
import json
import os
from pathlib import Path
import time
import pandas as pd

from htmlify import config
from htmlify.ddev import (
    DdevError,
    describe as describe_ddev,
    export_facet_configuration,
)
from htmlify.tasks.site import SiteTask
from htmlify.tasks.site import STATIC_ARCHIVE_VERSION
from htmlify.tasks.page import PageTask
from htmlify.tasks.base import BaseTask
from htmlify.tasks.migrate import MigrateTask
from htmlify.tasks.sitemap import SiteMapTask
from htmlify.tasks.sites_list import SitesListTask
from htmlify.db import (
    database_authentication_errors,
    db_manager,
    unreachable_database_hosts,
)
from htmlify.config import logger
from htmlify.inventory import build_sitemap_inventory
from htmlify.progress import (
    clear_progress,
    configure_progress,
    format_duration,
    get_batch_failures,
    initialise_batch_progress,
    record_batch_completion,
    record_page_completion,
)
from htmlify.validation import validate_copy, validate_site

cli = typer.Typer()
count = 0


def _require_database_credentials():
    missing = [
        name for name, value in (
            ('DB_USERNAME', config.DB_USERNAME),
            ('DB_PASSWORD', config.DB_PASSWORD),
        )
        if not value
    ]
    if not missing:
        return
    typer.secho(
        f'Missing database credentials: {", ".join(missing)}',
        fg=typer.colors.RED,
        bold=True,
        err=True,
    )
    typer.secho(
        f'Set them in {config.ROOT_DIR / ".env"} or in the process environment.',
        fg=typer.colors.YELLOW,
        err=True,
    )
    raise typer.Exit(1)


def _concise_exception(error):
    outer_message = ' '.join(str(error).split()) or 'Unknown error'
    summary = f'{type(error).__name__}: {outer_message}'
    current = error
    seen = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        next_error = current.__cause__ or current.__context__
        if next_error is None:
            break
        current = next_error
    if current is not error:
        cause_message = ' '.join(str(current).split()) or 'Unknown error'
        summary += f' (caused by {type(current).__name__}: {cause_message})'
    return summary


def _concise_luigi_error(error):
    """Reduce Luigi's JSON-encoded traceback to its final exception line."""
    if not error:
        return 'Unknown error'
    if isinstance(error, str):
        try:
            decoded = json.loads(error)
        except (json.JSONDecodeError, TypeError):
            decoded = error
        if isinstance(decoded, str):
            error = decoded
    lines = [line.strip() for line in str(error).splitlines() if line.strip()]
    return lines[-1] if lines else 'Unknown error'


def _sitemap_failures_from_result(result):
    """Read failed sitemap tasks from the parent Luigi worker."""
    if result is None or not getattr(result, 'worker', None):
        return []

    failed = {}
    completed = set()
    for task, status, _runnable in result.worker._add_task_history:
        if not isinstance(task, SiteMapTask):
            continue
        if status == 'DONE':
            completed.add(task.task_id)
        elif status in {'FAILED', 'UNKNOWN'}:
            failed[task.task_id] = task

    failures = []
    for task_id, task in sorted(
        failed.items(), key=lambda item: str(item[1].domain),
    ):
        if task_id in completed:
            continue
        try:
            response = result.worker._scheduler.fetch_error(task_id)
            error = _concise_luigi_error(response.get('error'))
        except Exception:
            error = 'See the Luigi error log for the full traceback'
        failures.append({'item': str(task.domain), 'error': error})
    return failures


def _merge_sitemap_failures(*failure_lists):
    failures = {}
    for failure_list in failure_lists:
        for failure in failure_list:
            failures[str(failure['item'])] = failure
    return [failures[item] for item in sorted(failures)]


def _print_sitemap_failures(failures):
    if not failures:
        return
    typer.secho(
        f'\nSITEMAP FAILURES: {len(failures):,}',
        fg=typer.colors.RED,
        bold=True,
        err=True,
    )
    for failure in failures[:20]:
        typer.secho(
            f"  {failure['item']} — {failure['error']}",
            fg=typer.colors.RED,
            err=True,
        )
    if len(failures) > 20:
        typer.secho(
            f'  …and {len(failures) - 20:,} more failures',
            fg=typer.colors.RED,
            err=True,
        )
    typer.secho(
        f'Full tracebacks: {config.LOG_DIR / "error.log"}',
        fg=typer.colors.YELLOW,
        err=True,
    )


def _print_sitemap_inventory(inventory):
    rows = [
        (
            site.domain,
            f'{site.sitemap_urls:,}' if site.sitemap_urls is not None else '—',
            site.status,
        )
        for site in inventory.sites
    ]
    domain_width = max([len('Domain'), *(len(row[0]) for row in rows)])
    urls_width = max([len('URLs'), *(len(row[1]) for row in rows)])
    typer.echo(
        f"{'Domain':<{domain_width}}  {'URLs':>{urls_width}}  Status"
    )
    typer.echo(f"{'-' * domain_width}  {'-' * urls_width}  {'-' * 8}")
    for domain, urls, status in rows:
        typer.echo(f'{domain:<{domain_width}}  {urls:>{urls_width}}  {status}')

    summary = inventory.summary
    typer.echo('')
    typer.echo(f"Sites: {summary['sites']:,}")
    typer.echo(f"Sitemaps present: {summary['sitemaps']:,}")
    typer.echo(f"Total URLs: {summary['urls']:,}")
    for status, status_count in summary['statuses'].items():
        typer.echo(f'{status}: {status_count:,}')


@cli.command('sitemaps')
def sitemaps(
    domain: Optional[str] = typer.Option(
        None,
        '--domain',
        '-d',
        help='Build and report one domain instead of every registered site.',
    ),
    ddev_project: Optional[Path] = typer.Option(
        None,
        '--ddev-project',
        help='Read the selected domain from this running DDEV project.',
    ),
    force: bool = typer.Option(
        False,
        '--force',
        help='Rebuild sitemap files even when their current version exists.',
    ),
    build: bool = typer.Option(
        True,
        '--build/--status-only',
        help='Build missing/stale sitemaps, or only report existing files.',
    ),
    workers: int = typer.Option(
        10,
        '--workers',
        min=1,
        help='Number of sitemap tasks to run concurrently.',
    ),
    connection_timeout: int = typer.Option(
        10,
        '--connection-timeout',
        min=1,
        help='Maximum seconds for each MySQL connection attempt.',
    ),
    report: Optional[Path] = typer.Option(
        None,
        '--report',
        help='Write counts and statuses as JSON.',
    ),
):
    """Build database sitemaps without rendering pages, then show inventory."""
    if ddev_project and not domain:
        raise typer.BadParameter('--ddev-project requires --domain')

    ddev = None
    if ddev_project:
        try:
            ddev = describe_ddev(ddev_project)
        except DdevError as error:
            typer.secho(str(error), fg=typer.colors.RED, err=True)
            raise typer.Exit(1)
        config.configure_local_source(
            domain=domain,
            base_url=ddev['base_url'],
            db_host=ddev['db_host'],
            db_port=ddev['db_port'],
            db_name=ddev['db_name'],
            db_username=ddev['db_username'],
            db_password=ddev['db_password'],
            solr_url=ddev['solr_url'],
        )
        typer.secho(
            f"Using local Drupal database at {ddev['base_url']}",
            fg=typer.colors.CYAN,
        )

    sites_list_task = SitesListTask()
    sites_list_path = Path(sites_list_task.output().path)
    if not sites_list_path.is_file():
        try:
            sites_list_task.run()
        except Exception as error:
            typer.secho(
                f'Could not create sites list: {error}',
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(1)

    sites = pd.read_csv(sites_list_path)
    domain_column = 'resolved_domain' if 'resolved_domain' in sites else 'domain'
    if 'platform_path' in sites:
        sites = sites[sites.platform_path.notnull()]
    domains = [str(value) for value in sites[domain_column].dropna().unique()]
    if domain:
        if domain not in domains:
            raise typer.BadParameter(f'No registered site matching {domain}')
        domains = [domain]

    result = None
    sitemap_failures = []
    started = time.monotonic()
    if build:
        tasks = [SiteMapTask(domain=value, force=force) for value in domains]
        pending_domains = {
            task.domain for task in tasks if not task.complete()
        }
        if pending_domains:
            _require_database_credentials()
        selected_sites = sites[sites[domain_column].isin(pending_domains)]
        if ddev:
            database_targets = {(ddev['db_host'], ddev['db_port'])}
        else:
            database_targets = {
                (row.db_host, 3306)
                for row in selected_sites[selected_sites.db_host.notnull()].itertuples()
            }
        unreachable = unreachable_database_hosts(
            database_targets,
            timeout=min(connection_timeout, 10),
        )
        if unreachable:
            typer.secho(
                'Cannot reach the required MySQL server(s):',
                fg=typer.colors.RED,
                err=True,
            )
            for target in unreachable:
                typer.secho(
                    f"  {target['host']}:{target['port']} — {target['error']}",
                    fg=typer.colors.RED,
                    err=True,
                )
            if not ddev:
                typer.secho(
                    'Connect to the museum VPN, then run the command again.',
                    fg=typer.colors.YELLOW,
                    err=True,
                )
            raise typer.Exit(1)

        authentication_errors = database_authentication_errors(
            database_targets,
            config.DB_USERNAME,
            config.DB_PASSWORD,
            timeout=min(connection_timeout, 10),
        )
        if authentication_errors:
            typer.secho(
                'Cannot authenticate to the required MySQL server(s):',
                fg=typer.colors.RED,
                bold=True,
                err=True,
            )
            for target in authentication_errors:
                typer.secho(
                    f"  {target['host']}:{target['port']} — {target['error']}",
                    fg=typer.colors.RED,
                    err=True,
                )
            typer.secho(
                f'Check DB_USERNAME and DB_PASSWORD in '
                f'{config.ROOT_DIR / ".env"}.',
                fg=typer.colors.YELLOW,
                err=True,
            )
            raise typer.Exit(1)

        config.DB_CONNECT_TIMEOUT = connection_timeout
        os.environ['DB_CONNECT_TIMEOUT'] = str(connection_timeout)
        progress_path = (
            config.PROCESSING_DATA_DIR
            / 'progress'
            / f'sitemaps-{os.getpid()}-{time.time_ns()}.json'
        )
        configure_progress(progress_path)
        initialise_batch_progress('Sitemaps', len(tasks))

        @SiteMapTask.event_handler(luigi.Event.SUCCESS)
        def on_sitemap_success(_task):
            record_batch_completion(
                'Sitemaps', processed=True, failed=False,
            )

        @SiteMapTask.event_handler(luigi.Event.DEPENDENCY_PRESENT)
        def on_sitemap_already_complete(_task):
            record_batch_completion(
                'Sitemaps', processed=False, failed=False,
            )

        @SiteMapTask.event_handler(luigi.Event.FAILURE)
        def on_sitemap_failure(task, exception):
            error = _concise_exception(exception)
            typer.secho(
                f'SITEMAP FAILED: {task.domain} — {error}',
                fg=typer.colors.RED,
                bold=True,
                err=True,
            )
            record_batch_completion(
                'Sitemaps',
                processed=True,
                failed=True,
                item=task.domain,
                error=error,
            )

        @SiteMapTask.event_handler(luigi.Event.BROKEN_TASK)
        def on_broken_sitemap(task, exception):
            error = _concise_exception(exception)
            typer.secho(
                f'SITEMAP BROKEN: {task.domain} — {error}',
                fg=typer.colors.RED,
                bold=True,
                err=True,
            )
            record_batch_completion(
                'Sitemaps',
                processed=True,
                failed=True,
                item=task.domain,
                error=error,
            )

        try:
            result = luigi.build(
                tasks,
                local_scheduler=True,
                workers=workers,
                detailed_summary=True,
                log_level='INFO',
            )
        finally:
            sitemap_failures = _merge_sitemap_failures(
                get_batch_failures('Sitemaps'),
                _sitemap_failures_from_result(result),
            )
            typer.echo(
                f'Sitemap task time: '
                f'{format_duration(time.monotonic() - started)}'
            )
            clear_progress()

    _print_sitemap_failures(sitemap_failures)

    inventory_started = time.monotonic()
    inventory_progress = None
    if not build:
        typer.echo(f'Reading sitemap inventory for {len(domains):,} sites...')

        def inventory_progress(completed, total, current_domain):
            if completed == total or completed % 25 == 0:
                typer.echo(
                    f'  Read {completed:,}/{total:,} sites '
                    f'(latest: {current_domain})'
                )

    inventory = build_sitemap_inventory(
        domains,
        SiteMapTask.output_dir,
        config.SITES_DIR,
        config.PROCESSING_DATA_DIR,
        STATIC_ARCHIVE_VERSION,
        progress=inventory_progress,
    )
    if not build:
        typer.echo(
            f'Inventory scan time: '
            f'{format_duration(time.monotonic() - inventory_started)}'
        )
    inventory.failures = sitemap_failures
    _print_sitemap_inventory(inventory)
    if report:
        inventory.write_json(report)

    successful_statuses = {
        LuigiStatusCode.SUCCESS,
        LuigiStatusCode.SUCCESS_WITH_RETRY,
    }
    if result is not None and result.status not in successful_statuses:
        raise typer.Exit(1)

@cli.command("migrate")
def migrate(
    limit: Optional[int] = None, 
    offset: Optional[int] = None, 
    workers: int = typer.Option(
        10,
        '--workers',
        min=1,
        help='Number of page tasks to run concurrently; use 1 for sequential diagnostics.',
    ),
    domain: Optional[str] = typer.Option(
        None,
        "--domain",
        "-d",
        help="Run migration for a specific domain only",
        ),
    ddev_project: Optional[Path] = typer.Option(
        None,
        '--ddev-project',
        help='Render the selected domain from this running DDEV project.',
    ),
    platform_path: Optional[Path] = typer.Option(
        None,
        '--platform-path',
        help='Override the Drupal code root used for the sites directory.',
    ),
    files_mode: Optional[str] = typer.Option(
        None,
        '--files-mode',
        help='Use "symlink" locally or "external" for separately rsynced files.',
    ),
    solr_url: Optional[str] = typer.Option(
        None,
        '--solr-url',
        help='Solr core URL used to export static faceted-page indexes.',
    ),
    page: Optional[str] = typer.Option(
        None,
        '--page',
        help='Render one exact sitemap path, for example "biblio".',
    ),
    page_prefix: Optional[str] = typer.Option(
        None,
        '--page-prefix',
        help='Render sitemap paths below a prefix, for example "taxonomy/term".',
    ),
    page_limit: Optional[int] = typer.Option(
        None,
        '--page-limit',
        help='Render at most this many matching pages.',
    ),
    page_offset: int = typer.Option(
        0,
        '--page-offset',
        help='Skip this many matching pages before applying --page-limit.',
    ),
    sort_by: str = typer.Option(
        'name',
        '--sort-by',
        '--sort',
        help='Order multi-site runs by "name" or "url-count".',
    ),
    sort_order: str = typer.Option(
        'asc',
        '--sort-order',
        '--order',
        help='Use ascending ("asc") or descending ("desc") order.',
    ),
    ):

    task_params = {
        'force': True
    }

    if page and page_prefix:
        raise typer.BadParameter('--page and --page-prefix cannot be combined')
    if page_limit is not None and page_limit < 1:
        raise typer.BadParameter('--page-limit must be at least 1')
    if page_offset < 0:
        raise typer.BadParameter('--page-offset cannot be negative')
    if sort_by not in {'name', 'url-count'}:
        raise typer.BadParameter(
            '--sort-by must be "name" or "url-count"'
        )
    if sort_order not in {'asc', 'desc'}:
        raise typer.BadParameter(
            '--sort-order must be "asc" or "desc"'
        )
    if (page or page_prefix or page_limit is not None or page_offset) and not domain:
        raise typer.BadParameter('page filters require --domain')

    task_params.update({
        'page_path': page,
        'page_prefix': page_prefix,
        'page_limit': page_limit,
        'page_offset': page_offset,
        'sort_by': sort_by,
        'sort_order': sort_order,
    })

    if files_mode:
        if files_mode not in {'symlink', 'external'}:
            raise typer.BadParameter('files mode must be "symlink" or "external"')
        config.DRUPAL_FILES_MODE = files_mode

    if ddev_project:
        if not domain:
            raise typer.BadParameter('--ddev-project requires --domain')
        try:
            ddev = describe_ddev(ddev_project)
        except DdevError as error:
            typer.secho(str(error), fg=typer.colors.RED, err=True)
            raise typer.Exit(1)

        facet_config_path = (
            config.PROCESSING_DATA_DIR
            / 'drupal-facets'
            / f'{domain}.json'
        )
        try:
            facet_configuration = export_facet_configuration(
                ddev_project,
                ddev['base_url'],
            )
            facet_config_path.parent.mkdir(parents=True, exist_ok=True)
            facet_config_path.write_text(
                json.dumps(facet_configuration, ensure_ascii=False),
                encoding='utf-8',
            )
        except DdevError as error:
            typer.secho(
                f'Warning: {error}. Falling back to rendered facet blocks.',
                fg=typer.colors.YELLOW,
                err=True,
            )
            facet_config_path = None

        config.configure_local_source(
            domain=domain,
            base_url=ddev['base_url'],
            db_host=ddev['db_host'],
            db_port=ddev['db_port'],
            db_name=ddev['db_name'],
            db_username=ddev['db_username'],
            db_password=ddev['db_password'],
            solr_url=solr_url or ddev['solr_url'],
            facet_config_path=facet_config_path,
        )
        platform_path = platform_path or ddev['platform_path']
        typer.secho(
            f"Using local Drupal at {ddev['base_url']}",
            fg=typer.colors.CYAN,
        )

    elif solr_url:
        config.SOLR_BASE_URL = solr_url.rstrip('/')
        os.environ['SOLR_BASE_URL'] = config.SOLR_BASE_URL

    if platform_path:
        task_params['platform_path'] = str(platform_path.expanduser().resolve())

    if domain: 
        task_params['domain'] = domain
        typer.secho(f"Migrating domain {domain}", fg=typer.colors.YELLOW) 
    elif limit:
        if offset:
            typer.secho(
                f'Migrating {limit} sites by {sort_by} {sort_order}, '
                f'skipping first {offset}',
                fg=typer.colors.YELLOW,
            )
        else:
            typer.secho(
                f'Migrating {limit} sites by {sort_by} {sort_order}',
                fg=typer.colors.YELLOW,
            )
    else:
        typer.secho(
            f'Migrating all sites by {sort_by} {sort_order}',
            fg=typer.colors.YELLOW,
        )
      
    def status_update(task):
        global count
        count += 1
        typer.secho(f'{count}. {task.domain} complete', fg=typer.colors.GREEN)   

    def _failed_site(task, exception=None):
        typer.secho(f'{task.domain} failed', fg=typer.colors.RED)
        logger.critical(f'{task.domain} failed: {exception}')

    @SiteTask.event_handler(luigi.Event.SUCCESS)
    def on_success(task):
        status_update(task)
            
    @SiteTask.event_handler(luigi.Event.DEPENDENCY_PRESENT)
    def on_dependency_present(task):
        status_update(task) 

    @SiteTask.event_handler(luigi.Event.BROKEN_TASK)
    def on_broken_task(task, exception):
        _failed_site(task, exception)

    @SiteTask.event_handler(luigi.Event.FAILURE)
    def on_failure(task, exception):
        _failed_site(task, exception)

    @PageTask.event_handler(luigi.Event.SUCCESS)
    def on_page_success(task):
        record_page_completion(task.domain, processed=True)

    @BaseTask.event_handler(luigi.Event.FAILURE)
    def report_dependency_failure(task, exception):
        typer.secho(
            f'{task} failed: {type(exception).__name__}: {exception}',
            fg=typer.colors.RED,
            err=True,
        )

    error_log = config.LOG_DIR / 'error.log'
    error_log_offset = error_log.stat().st_size if error_log.exists() else 0

    task = MigrateTask(**task_params)
    _require_database_credentials()
    run_started = time.monotonic()
    progress_path = (
        config.PROCESSING_DATA_DIR
        / 'progress'
        / f'migrate-{os.getpid()}-{time.time_ns()}.json'
    )
    configure_progress(progress_path)
    try:
        result = luigi.build(
            [task],
            local_scheduler=True,
            workers=workers,
            detailed_summary=True,
        )
    finally:
        typer.echo(
            f'Total migration time: '
            f'{format_duration(time.monotonic() - run_started)}'
        )
        clear_progress()

    successful_statuses = {
        LuigiStatusCode.SUCCESS,
        LuigiStatusCode.SUCCESS_WITH_RETRY,
    }
    if result.status not in successful_statuses:
        typer.secho('\nMigration failed.', fg=typer.colors.RED, err=True)
        if error_log.exists():
            with error_log.open('r', encoding='utf-8', errors='replace') as log:
                log.seek(error_log_offset)
                new_errors = log.read().strip()
            if new_errors:
                max_error_characters = 20000
                if len(new_errors) > max_error_characters:
                    new_errors = (
                        '... earlier worker errors omitted; full log: '
                        f'{error_log}\n\n'
                        f'{new_errors[-max_error_characters:]}'
                    )
                typer.secho(
                    f'Underlying error:\n{new_errors}',
                    fg=typer.colors.RED,
                    err=True,
                )
            else:
                typer.secho(
                    f'No worker traceback was captured; inspect {error_log}',
                    fg=typer.colors.RED,
                    err=True,
                )
        raise typer.Exit(1)


@cli.command('validate-copy')
def validate_copy_command(
    source: Path = typer.Argument(..., help='Directory passed to rsync as the source.'),
    destination: Path = typer.Argument(..., help='Directory populated by rsync.'),
    checksums: bool = typer.Option(
        False,
        '--checksums',
        help='Hash file contents in addition to comparing type and size.',
    ),
    allow_extra: bool = typer.Option(
        False,
        '--allow-extra',
        help='Do not fail when destination-only files are present.',
    ),
    report: Optional[Path] = typer.Option(
        None,
        '--report',
        help='Write the complete result as JSON.',
    ),
):
    """Validate a file tree copied separately with rsync."""
    try:
        result = validate_copy(source, destination, checksums=checksums)
    except FileNotFoundError as error:
        typer.secho(str(error), fg=typer.colors.RED, err=True)
        raise typer.Exit(1)

    if report:
        result.write_json(report)

    typer.echo(f'Checked {result.checked_files} source files')
    typer.echo(f'Missing: {len(result.missing)}')
    typer.echo(f'Changed: {len(result.changed)}')
    typer.echo(f'Extra: {len(result.extra)}')

    failed = bool(result.missing or result.changed or (result.extra and not allow_extra))
    if failed:
        for label, paths in (
            ('missing', result.missing),
            ('changed', result.changed),
            ('extra', result.extra if not allow_extra else []),
        ):
            for path in paths[:20]:
                typer.secho(f'{label}: {path}', fg=typer.colors.RED, err=True)
        raise typer.Exit(1)


@cli.command('validate-site')
def validate_site_command(
    site: Path = typer.Argument(
        ...,
        help='Root directory of a generated static site.',
    ),
    domain: Optional[str] = typer.Option(
        None,
        '--domain',
        help='Canonical hostname; normally read from the generated homepage.',
    ),
    report: Optional[Path] = typer.Option(
        None,
        '--report',
        help='Write the complete result, including every referrer, as JSON.',
    ),
):
    """Find missing internal pages, files, and broken symlinks."""
    try:
        result = validate_site(site, domain=domain)
    except FileNotFoundError as error:
        typer.secho(str(error), fg=typer.colors.RED, err=True)
        raise typer.Exit(1)

    if report:
        result.write_json(report)

    typer.echo(f'Checked HTML pages: {result.checked_html_files}')
    typer.echo(f'Checked CSS files: {result.checked_css_files}')
    typer.echo(f'Checked facet indexes: {result.checked_facet_indexes}')
    typer.echo(f'Internal references: {result.checked_internal_references}')
    typer.echo(f'External references excluded: {result.skipped_external_references}')
    typer.echo(f'Missing pages: {len(result.missing_pages)}')
    for prefix, count in result.missing_pages_by_prefix.items():
        typer.echo(f'  {prefix}: {count}')
    typer.echo(f'Missing files: {len(result.missing_files)}')
    typer.echo(f'Broken symlinks: {len(result.broken_symlinks)}')
    typer.echo(f'Read/parse errors: {len(result.errors)}')

    if not result.ok:
        for label, issues in (
            ('missing page', result.missing_pages),
            ('missing file', result.missing_files),
        ):
            for issue in issues[:20]:
                locations = issue['pages'] or issue['referrers']
                referrers = ', '.join(locations[:3])
                if len(locations) > 3:
                    referrers += f" (+{len(locations) - 3} more)"
                typer.secho(
                    f"{label}: {issue['target']} <- {referrers}",
                    fg=typer.colors.RED,
                    err=True,
                )
        for issue in result.broken_symlinks[:20]:
            typer.secho(
                f"broken symlink: {issue['path']} -> {issue['target']}",
                fg=typer.colors.RED,
                err=True,
            )
        for issue in result.errors[:20]:
            typer.secho(
                f"error: {issue['source']}: {issue['error']}",
                fg=typer.colors.RED,
                err=True,
            )
        raise typer.Exit(1)


if __name__ == "__main__":
    cli()
