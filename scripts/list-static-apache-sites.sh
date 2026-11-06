#!/usr/bin/env bash

set -uo pipefail

sites_root="${1:-/var/www/new}"
available_root="${2:-/etc/apache2/sites-available}"
enabled_root="${3:-/etc/apache2/sites-enabled}"

if [[ ! -d "$sites_root" ]]; then
  echo "Static-site directory does not exist: $sites_root" >&2
  exit 1
fi

shopt -s nullglob
site_dirs=("$sites_root"/*)

printf '%-50s %s\n' 'DOMAIN' 'APACHE STATUS'
printf '%-50s %s\n' '------' '-------------'

site_count=0
available_count=0
enabled_count=0
missing_count=0

for site_dir in "${site_dirs[@]}"; do
  [[ -d "$site_dir" ]] || continue

  domain="${site_dir##*/}"
  if [[ ! "$domain" =~ ^[A-Za-z0-9][A-Za-z0-9.-]*$ ]]; then
    printf '%-50s %s\n' "$domain" 'skipped (unsafe directory name)'
    continue
  fi

  ((site_count += 1))
  config_name="${domain}.conf"
  available_config="${available_root}/${config_name}"
  enabled_config="${enabled_root}/${config_name}"

  if [[ ! -f "$available_config" ]]; then
    printf '%-50s %s\n' "$domain" 'missing configuration'
    ((missing_count += 1))
  elif [[ -e "$enabled_config" || -L "$enabled_config" ]]; then
    printf '%-50s %s\n' "$domain" 'enabled'
    ((available_count += 1))
    ((enabled_count += 1))
  else
    printf '%-50s %s\n' "$domain" 'available, not enabled'
    ((available_count += 1))
  fi
done

echo
echo "Static sites: $site_count"
echo "Configurations available: $available_count"
echo "Already enabled: $enabled_count"
echo "Configurations missing: $missing_count"

