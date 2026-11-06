#!/usr/bin/env bash

set -euo pipefail

sites_root="${1:-/var/www/new}"
available_root="${2:-/etc/apache2/sites-available}"

if [[ ! -d "$sites_root" ]]; then
  echo "Static-site directory does not exist: $sites_root" >&2
  exit 1
fi

if (( EUID == 0 )); then
  sudo_command=()
else
  sudo_command=(sudo)
fi

shopt -s nullglob
site_dirs=("$sites_root"/*)

site_count=0
enabled_count=0
missing_count=0

for site_dir in "${site_dirs[@]}"; do
  [[ -d "$site_dir" ]] || continue

  domain="${site_dir##*/}"
  if [[ ! "$domain" =~ ^[A-Za-z0-9][A-Za-z0-9.-]*$ ]]; then
    echo "Skipping unsafe directory name: $domain" >&2
    continue
  fi

  ((site_count += 1))
  config_name="${domain}.conf"
  available_config="${available_root}/${config_name}"

  if [[ ! -f "$available_config" ]]; then
    echo "Skipping ${domain}: no Apache configuration at ${available_config}"
    ((missing_count += 1))
    continue
  fi

  echo "Enabling ${domain}"
  "${sudo_command[@]}" a2ensite "$config_name"
  ((enabled_count += 1))
done

if (( enabled_count == 0 )); then
  echo "No matching Apache configurations were found; Apache was not reloaded." >&2
  exit 1
fi

echo
echo "Checking Apache configuration before reload..."
"${sudo_command[@]}" apache2ctl configtest

echo "Reloading Apache..."
"${sudo_command[@]}" systemctl reload apache2

echo
echo "Static sites inspected: $site_count"
echo "Configurations enabled: $enabled_count"
echo "Configurations missing: $missing_count"
echo "Apache reloaded successfully."

