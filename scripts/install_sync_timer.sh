#!/usr/bin/env bash
# Install systemd user timers that keep the local Recebimentos data in step with
# the Portal da Transparencia from this repository checkout:
#
#   gov-spending-sync.timer       daily: the current month and the 12 before it
#   gov-spending-sync-full.timer  weekly: every month since 2013-12 (--all)
#
# The Portal regenerates its archives shortly after 00:00 UTC (about 21:00 in
# Brasilia), so the 06:00 local runs pick up that day's files. The download server
# sits behind a WAF that challenges clients sending many requests, so the full
# sweep runs weekly, months are checked a few seconds apart, and a challenged run
# stops instead of retrying.
#
# Usage: scripts/install_sync_timer.sh
# Remove with:
#   systemctl --user disable --now gov-spending-sync.timer gov-spending-sync-full.timer

set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
cli="$repo_dir/.venv/bin/gov-spending"

if [[ ! -x "$cli" ]]; then
    echo "Missing $repo_dir/.venv; run: uv sync --extra dev" >&2
    exit 1
fi
if [[ ! -f "$repo_dir/dbt/profiles.yml" ]]; then
    echo "Missing dbt/profiles.yml; run: cp dbt/profiles.yml.example dbt/profiles.yml" >&2
    exit 1
fi

mkdir -p "$unit_dir"

write_units() {
    local name="$1" description="$2" arguments="$3" on_calendar="$4"

    cat > "$unit_dir/$name.service" <<EOF
[Unit]
Description=$description
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
WorkingDirectory=$repo_dir
ExecStart=$cli sync-recebimentos $arguments --dbt-build
Nice=10
# dbt over the full history is memory heavy; past this cap only this unit is
# killed, never the desktop session (the profile also caps DuckDB itself).
MemoryMax=10G
EOF

    cat > "$unit_dir/$name.timer" <<EOF
[Unit]
Description=$description

[Timer]
OnCalendar=$on_calendar
RandomizedDelaySec=15min
Persistent=true

[Install]
WantedBy=timers.target
EOF
}

write_units gov-spending-sync \
    "Daily Portal da Transparencia Recebimentos sync (recent months)" \
    "" "*-*-* 06:00:00"
write_units gov-spending-sync-full \
    "Weekly Portal da Transparencia Recebimentos sync (every month)" \
    "--all" "Sun *-*-* 07:00:00"

systemctl --user daemon-reload
systemctl --user enable --now gov-spending-sync.timer gov-spending-sync-full.timer
systemctl --user list-timers 'gov-spending-sync*' --no-pager
echo "Logs: journalctl --user -u gov-spending-sync.service -u gov-spending-sync-full.service"
