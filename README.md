# htmlify
Convert Scratchpads to HTML

Install the project dependencies from the repository root:

```bash
uv sync
```

## Useful commands

Run these commands from the repository root. Use `uv run htmlify --help` or
`uv run htmlify COMMAND --help` for the complete option list.

### List sites, URL counts, and archive status

Read the existing sitemap files without connecting to Drupal or its databases:

```bash
uv run htmlify sitemaps --status-only
```

The output lists every site, its sitemap URL count, and whether its archive is
`complete`, `stale`, `partial`, or `not run`. It also prints totals. To save the
same inventory as JSON:

```bash
uv run htmlify sitemaps --status-only \
  --report sitemap-inventory.json
```

Build or refresh the sitemap for one production site, without rendering its
pages:

```bash
uv run htmlify sitemaps \
  --domain example.myspecies.info \
  --force
```

Build missing or stale sitemaps for all registered sites:

```bash
uv run htmlify sitemaps --workers 10
```

### Generate a site

Generate one complete production site:

```bash
uv run htmlify migrate \
  --domain example.myspecies.info
```

Use one worker when diagnosing a page that appears to hang or fail. This makes
the relevant error much easier to associate with its page:

```bash
uv run htmlify migrate \
  --domain example.myspecies.info \
  --workers 1
```

Generate one page plus the homepage:

```bash
uv run htmlify migrate \
  --domain example.myspecies.info \
  --page biblio
```

Generate a batch of 100 taxonomy pages plus the homepage:

```bash
uv run htmlify migrate \
  --domain example.myspecies.info \
  --page-prefix taxonomy/term \
  --page-limit 100
```

Add `--page-offset 100` for the next batch.

### Inspect and validate an archive

Serve one generated site locally, then open <http://localhost:8000/>:

```bash
uv run python -m http.server 8000 \
  --directory /path/to/sites/example.myspecies.info
```

Check the generated site for missing internal pages, files, references, and
broken symlinks:

```bash
uv run htmlify validate-site \
  /path/to/sites/example.myspecies.info \
  --report site-validation.json
```

Follow worker errors while a migration is running:

```bash
tail -f data/.log/error.log
```

### Run the tests

```bash
uv run python -m unittest discover -s htmlify/tests
```

## Apache

https://www.digitalocean.com/community/tutorials/how-to-install-the-apache-web-server-on-ubuntu-22-04

## MYSQL

-- Create the new user
CREATE USER 'static'@'sp-static.nhm.ac.uk' IDENTIFIED BY 'bugFUV7d}Kangar00';

-- Grant all privileges on all databases
GRANT ALL PRIVILEGES ON *.* TO 'static'@'sp-static.nhm.ac.uk' WITH GRANT OPTION;

-- Apply the changes
FLUSH PRIVILEGES;



drush @hm sqlq "select nid, alias from hosting_site_alias where alias like '%myspecies.info' and alias not like 'www%'" > /tmp/static-site-aliases.csv

drush @hm sqlq "select hc.nid, hc.name as domain, hp.publish_path, hs.db_name, db.human_name from hosting_context hc inner join hosting_site hs on hs.nid=hc.nid inner join hosting_platform hp on hp.nid = hs.platform inner join hosting_server db on db.nid = hs.db_server where hs.status=1 order by hc.name" > /tmp/static-sites.csv 


drush @macrostomorpha.info solr-delete-index



## Rendering from local DDEV

Run a single canonical site through the local Drupal instance without changing
the URLs written to the archive:

```bash
uv run htmlify migrate \
  --domain gadus.myspecies.info \
  --ddev-project /Users/ben/Projects/Scratchpads/Code/scratchpads2
```

`htmlify` reads the web URL, published database port, credentials, database
name, Drupal code root, and `SOLR_DEFAULT_URL` from DDEV. DDEV must already be
running. Absolute links emitted with the DDEV hostname are converted back to
the canonical domain before saving them.

Page discovery does not recursively crawl the website. The sitemap is built
from Drupal's database using published content records, visible internal menu
links, and compiled `views_page` routes from `menu_router`. Administrative,
development, wildcard, AJAX, feed, and export routes are excluded. This also
finds page displays supplied as code-exported default Views, which may not have
rows in `views_view` or `views_display`.

### Sitemap inventory

Build only the database sitemap for one locally hosted site and show its URL
count and migration status:

```bash
uv run htmlify sitemaps \
  --domain weevil.myspecies.info \
  --ddev-project /Users/ben/Projects/Scratchpads/Code/scratchpads2 \
  --force
```

Run the sitemap tasks for every registered site and save the inventory:

```bash
uv run htmlify sitemaps --report sitemap-inventory.json
```

Use `--status-only` to read existing sitemap files without querying any site
databases. The table shows each domain, its sitemap URL count, and one of four
archive states: `complete`, `stale`, `partial`, or `not run`. A complete archive
becomes stale when its sitemap is newer than the completion marker or when the
archive format version changes. The command also prints totals for sites,
sitemaps, URLs, and each archive state. Before starting, an all-sites build
checks that each production MySQL host is reachable and gives a museum-VPN
message instead of launching hundreds of tasks that will time out. Connection
attempts default to ten seconds and can be changed with
`--connection-timeout SECONDS`. It also requires `DB_USERNAME` and
`DB_PASSWORD` in the project `.env` (or process environment), and verifies the
login once per distinct database server before scheduling work. Progress output
counts successful and failed sites and estimates the remaining time. Each
failure is printed immediately as
`SITEMAP FAILED: domain — exception`; the end-of-run summary repeats the first
20 failures and points to `data/.log/error.log` for full tracebacks. JSON
inventory reports also include the complete concise failure list.

Pages whose Drupal `#region-content` contains only page chrome (for example a
taxonomy title, tabs, or an empty-view message) are not written. A hidden
marker under `data/processing/skipped` records the intentional omission so
Luigi does not treat it as a failed task. No marker is placed in the published
site directory. Database candidates returning 401, 403, 404, or 410 are
recorded as skipped in the same way.

### Testing a subset of pages

Page filters apply after the database sitemap has been built. Render one exact
page, such as the faceted bibliography browser:

```bash
uv run htmlify migrate \
  --domain weevil.myspecies.info \
  --ddev-project /Users/ben/Projects/Scratchpads/Code/scratchpads2 \
  --page biblio
```

Or render the first 100 matching taxonomy pages:

```bash
uv run htmlify migrate \
  --domain weevil.myspecies.info \
  --ddev-project /Users/ben/Projects/Scratchpads/Code/scratchpads2 \
  --page-prefix taxonomy/term \
  --page-limit 100
```

Use `--page-offset 100` with the same prefix and limit for the next batch.
Filters accept paths with or without a leading slash. Filtered runs do not
write the whole-site completion marker; they use a separate partial-run marker.
The homepage is always included in addition to the selected pages. The
maximum-page check is applied only to this filtered set.

During page generation, Htmlify prints a compact progress update approximately
every five seconds. It includes completed and total pages, percentage, elapsed
time, processing rate, and an ETA calculated from pages completed during the
current run. Pages already present in the archive count towards completion but
do not artificially inflate the processing rate. The final output also shows
the total migration runtime.

## Static faceted search

Any page containing Drupal Facet API blocks (for example `/biblio`, `/gallery`,
or `/interactions`) is converted into a static, page-specific browser. Htmlify
reads the facet fields and labels from the rendered page, exports that route's
matching records and exact facet relationships from Solr, and writes a compact
index below `assets/facets/`. The normal `/search` route is excluded and keeps
its Google site-search form.

The generated browser provides local text search, combined facet selections,
A–Z/value filtering, counts, and paginated results. Its selections use
shareable `?facet=FIELD:VALUE` query parameters and work entirely in the
browser after archiving; the published site does not need Drupal or Solr.

With DDEV the Solr URL is discovered automatically. For another environment,
set `SOLR_BASE_URL` or pass the core URL explicitly:

```bash
uv run htmlify migrate --domain example.myspecies.info \
  --solr-url http://localhost:8983/solr/collection1
```

## Copying Drupal files separately

On the archive server, generate HTML without creating a local `sites` symlink:

```bash
uv run htmlify migrate --domain example.myspecies.info --files-mode external
```

Copy the required Drupal file tree separately with rsync. A trailing slash on
both paths makes their contents directly comparable:

```bash
rsync -a --delete /source/sites/ /archive/example.myspecies.info/sites/
```

Validate every copied file by relative path, type, size, and SHA-256 checksum:

```bash
uv run htmlify validate-copy \
  /source/sites/ \
  /archive/example.myspecies.info/sites/ \
  --checksums \
  --report /archive/example.myspecies.info/file-validation.json
```

The validator exits non-zero for missing, changed, or extra files. Use
`--allow-extra` when destination-only files are expected.

## Validating a generated site

Scan a completed static site for missing internal pages and files:

```bash
uv run htmlify validate-site \
  /archive/example.myspecies.info \
  --report /archive/example.myspecies.info/site-validation.json
```

The validator reads the generated HTML, CSS, and facet JSON indexes locally.
It checks internal page links, images, stylesheets, scripts, downloads, CSS
`url(...)` references, facet result links and thumbnails, and broken symlinks.
External URLs are excluded. Missing pages are grouped by the first URL path
component (such as `node`, `biblio`, or `taxonomy`), and the JSON report lists
every immediate referrer and every HTML page that loads it. This includes
tracing a missing CSS image or facet thumbnail back through its stylesheet or
JSON index to the affected pages. The command exits non-zero when anything is
missing or unreadable.
