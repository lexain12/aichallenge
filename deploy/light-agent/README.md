# Light-agent deployment and operator runbook

This runbook is preparatory. It does not authorize connecting to a VM, changing
SSH access, installing a crontab, rebooting a host, or provisioning credentials.
Perform those actions only in a separately authorized live acceptance window.
Every command block is fail-closed: do not run a later command or block after
any unexpected nonzero exit.

## One runtime identity and one home

The deployment contract has one dedicated `light-agent` account whose home
directory is exactly `/var/lib/light-agent`. Both OpenSSH and Cronie start the
runtime with `HOME=/var/lib/light-agent`, and their working directory is
`/var/lib/light-agent`. This is security-relevant: the fixed commands intentionally
omit `--config`, so `light-agent` resolves its default `light-agent.toml` from
that working directory.

The account needs a real `/bin/sh` because `sshd` invokes a forced command
through the account shell. It is not an interactive administration account:
lock its password, give it no sudo membership, and authorize only the dedicated
forced-command key. During an authorized install, an administrator can use:

```bash
sudo useradd --system --create-home --home-dir /var/lib/light-agent --shell /bin/sh light-agent
sudo passwd --lock light-agent
sudo install -d -o light-agent -g light-agent -m 0700 /var/lib/light-agent
sudo install -d -o root -g root -m 0755 /opt/light-agent/bin
sudo install -o root -g root -m 0755 target/release/light-agent /opt/light-agent/bin/light-agent
sudo install -o light-agent -g light-agent -m 0600 light-agent.example.toml /var/lib/light-agent/light-agent.toml
```

For an existing account, verify rather than silently changing it:

```bash
getent passwd light-agent
sudo -u light-agent -H sh -c 'cd /var/lib/light-agent && test "$HOME" = /var/lib/light-agent && test "$PWD" = /var/lib/light-agent && test -r light-agent.toml'
```

The owner-only configuration is
`/var/lib/light-agent/light-agent.toml` (mode `0600`). Durable state is
`/var/lib/light-agent/state.sqlite3`; SQLite and runtime lock sidecars are
created beside it under the mode `0700` home. The example intentionally omits
`scheduler.lock_path`, so the validated default is derived beside the database,
not under `/run/lock`. Keep provider and MCP credentials out of process arguments
and shell history.

## Cronie preflight and managed environment

Only Cronie is supported.

### Ubuntu/Debian package transition

On Ubuntu or Debian, replacing the distribution cron daemon is an explicit
maintenance-window operation, not an ordinary dependency install. This runbook
supports only the live-observed starting point: `cron=INSTALLED(<version>)`,
`ubuntu-standard=INSTALLED(<version>)`, `cronie=ABSENT`, and `cron.service`
enabled and active. Any other package or service state must stop and require an
operator-specific plan; this is deliberately not a general package migration
framework.

Use this read-only discovery snippet. It queries each package separately and
prints exactly `INSTALLED(version)` or `ABSENT`; any other recorded dpkg state
is rejected instead of being silently treated as absent. Then inspect the
complete resolver simulation:

```bash
/bin/sh -eu <<'CRONIE_DISCOVERY'
package_state() {
  package=$1
  if record=$(/usr/bin/dpkg-query -W -f='${db:Status-Status}|${Version}' "$package" 2>/dev/null); then
    case "$record" in
      installed\|?*) printf 'INSTALLED(%s)\n' "${record#installed|}" ;;
      *) echo "unsupported dpkg state for $package" >&2; return 1 ;;
    esac
  else
    printf 'ABSENT\n'
  fi
}
cron_state=$(package_state cron)
ubuntu_standard_state=$(package_state ubuntu-standard)
cronie_state=$(package_state cronie)
printf 'cron=%s\nubuntu-standard=%s\ncronie=%s\n' \
  "$cron_state" "$ubuntu_standard_state" "$cronie_state"
case "$cron_state" in INSTALLED\(*) ;; *) exit 1;; esac
case "$ubuntu_standard_state" in INSTALLED\(*) ;; *) exit 1;; esac
test "$cronie_state" = ABSENT
test "$(systemctl show cron.service --property=UnitFileState --value)" = enabled
test "$(systemctl show cron.service --property=ActiveState --value)" = active
sudo /usr/bin/apt-get --simulate install cronie
CRONIE_DISCOVERY
```

Read the simulation before continuing. In particular, `cron` and
`ubuntu-standard` may be removed; this was observed during Ubuntu 25.10 live
acceptance and must be separately authorized. Do not proceed if anything else
would be removed without review.

Repeat the three package queries inside the following root snapshot script. It
records versions and explicit package/service markers, refuses all unsupported
preconditions, then stops `cron.service` and copies configuration and spool only
after proving the service inactive:

```bash
sudo /bin/sh -eu <<'CRONIE_SNAPSHOT'
rollback=/root/light-agent-cronie.rollback
package_state() {
  package=$1
  if record=$(/usr/bin/dpkg-query -W -f='${db:Status-Status}|${Version}' "$package" 2>/dev/null); then
    case "$record" in
      installed\|?*) printf 'INSTALLED(%s)\n' "${record#installed|}" ;;
      *) echo "unsupported dpkg state for $package" >&2; return 1 ;;
    esac
  else
    printf 'ABSENT\n'
  fi
}
test ! -e "$rollback"
cron_state=$(package_state cron)
ubuntu_standard_state=$(package_state ubuntu-standard)
cronie_state=$(package_state cronie)
case "$cron_state" in INSTALLED\(*) ;; *) exit 1;; esac
case "$ubuntu_standard_state" in INSTALLED\(*) ;; *) exit 1;; esac
test "$cronie_state" = ABSENT
test "$(systemctl show cron.service --property=UnitFileState --value)" = enabled
test "$(systemctl show cron.service --property=ActiveState --value)" = active

install -d -o root -g root -m 0700 "$rollback"
umask 077
printf 'cron=%s\nubuntu-standard=%s\ncronie=%s\n' \
  "$cron_state" "$ubuntu_standard_state" "$cronie_state" > "$rollback/packages.before"
install -o root -g root -m 0600 /dev/null "$rollback/cron.INSTALLED"
install -o root -g root -m 0600 /dev/null "$rollback/ubuntu-standard.INSTALLED"
install -o root -g root -m 0600 /dev/null "$rollback/cronie.ABSENT"
install -o root -g root -m 0600 /dev/null "$rollback/cron.service.enabled"
install -o root -g root -m 0600 /dev/null "$rollback/cron.service.active"

systemctl stop cron.service
test "$(systemctl show cron.service --property=ActiveState --value)" = inactive
test -f /etc/crontab
test ! -L /etc/crontab
test -d /etc/cron.d
test ! -L /etc/cron.d
test -d /var/spool/cron
test ! -L /var/spool/cron
cp --archive --no-dereference /etc/crontab "$rollback/etc-crontab"
cp --archive --no-dereference /etc/cron.d "$rollback/etc-cron.d"
cp --archive --no-dereference /var/spool/cron "$rollback/var-spool-cron"
CRONIE_SNAPSHOT

sudo /usr/bin/apt-get install cronie
sudo systemctl enable --now cronie.service
```

The install is accepted only if the package, binary identity, and service all
agree. Retain the rollback directory until scheduled jobs and a reboot have
been accepted:

```bash
/usr/bin/dpkg-query -W -f='${db:Status-Status}|${Version}\n' cronie
/usr/bin/crontab -V
sudo systemctl is-enabled cronie.service
sudo systemctl is-active cronie.service
```

If the transition must be reversed, run this only in a new quiescent maintenance
window. It stops Cronie only when systemd still has the unit, installs (rather
than unconditionally reinstalls) the two original packages, stops the restored
daemon, restores the protected snapshot, and reapplies the recorded
enabled+active service state:

```bash
sudo /bin/sh -eu <<'CRONIE_ROLLBACK'
rollback=/root/light-agent-cronie.rollback
test -f "$rollback/cron.INSTALLED"
test -f "$rollback/ubuntu-standard.INSTALLED"
test -f "$rollback/cronie.ABSENT"
test -f "$rollback/cron.service.enabled"
test -f "$rollback/cron.service.active"

cronie_load=$(systemctl show cronie.service --property=LoadState --value)
case "$cronie_load" in
  loaded)
    systemctl stop cronie.service
    test "$(systemctl show cronie.service --property=ActiveState --value)" = inactive
    ;;
  not-found) ;;
  *) echo 'unsupported cronie.service load state' >&2; exit 1 ;;
esac

/usr/bin/apt-get install cron ubuntu-standard
systemctl stop cron.service
test "$(systemctl show cron.service --property=ActiveState --value)" = inactive
test ! -e /etc/crontab.cronie-failed
test ! -e /etc/cron.d.cronie-failed
test ! -e /var/spool/cron.cronie-failed
mv -T /etc/crontab /etc/crontab.cronie-failed
mv -T /etc/cron.d /etc/cron.d.cronie-failed
mv -T /var/spool/cron /var/spool/cron.cronie-failed
cp --archive --no-dereference "$rollback/etc-crontab" /etc/crontab
cp --archive --no-dereference "$rollback/etc-cron.d" /etc/cron.d
cp --archive --no-dereference "$rollback/var-spool-cron" /var/spool/cron
systemctl daemon-reload
systemctl enable --now cron.service
test "$(systemctl show cron.service --property=UnitFileState --value)" = enabled
test "$(systemctl show cron.service --property=ActiveState --value)" = active
CRONIE_ROLLBACK
```

Do not merge the two spool trees and do not restore a live spool. After rollback,
review the retained `.cronie-failed` trees before any separate cleanup.

### Runtime preflight

Before allowing scheduled mutations, verify on the VM:

```bash
/usr/bin/crontab -V
sudo -u light-agent -H /bin/sh <<'LIGHT_AGENT_CRONIE_PREFLIGHT'
set -eu
umask 077
validation_file=$(mktemp /var/lib/light-agent/.light-agent-crontab-preflight.XXXXXX)
trap 'rm -f -- "$validation_file"' EXIT HUP INT TERM
chmod 0600 "$validation_file"
printf 'HOME=/var/lib/light-agent\nCRON_TZ=UTC\n0 0 * * * /bin/true\n' > "$validation_file"
/usr/bin/crontab -T "$validation_file"
LIGHT_AGENT_CRONIE_PREFLIGHT
```

The version must identify Cronie and syntax validation must exit successfully.
Cronie 1.7 requires `-T` to receive a regular file path; `-T -` is not a
portable stdin form. The trap must remove the owner-only probe on success,
failure, or interruption.
Install and inspect only the `light-agent` account's crontab. The managed block
sets `HOME=/var/lib/light-agent`, then emits a validated `CRON_TZ` and only:

```text
/opt/light-agent/bin/light-agent run-job <canonical-job-id>
```

Cronie also starts jobs in the account home from the passwd database. The HOME
line and passwd home must agree; the deployment acceptance fails if either is
different. Task text never appears in the crontab. Do not manually edit inside
the managed block. Reconciliation removes an existing managed block and appends
one normalized block at the end of the crontab. All unmanaged bytes retain their
original order; only one newline separator is added when the unmanaged content
does not already end in a newline. Keeping the block at EOF prevents its `HOME`
or final `CRON_TZ` assignment from changing later unmanaged entries.

Run reconciliation from the same home contract:

```bash
sudo -u light-agent -H sh -c 'cd /var/lib/light-agent && exec /opt/light-agent/bin/light-agent cron-sync'
sudo -u light-agent /usr/bin/crontab -l
```

## Forced-command SSH

Provision a separate owner key for the terminal client. Add one line based on
`authorized_keys.example` to `/var/lib/light-agent/.ssh/authorized_keys` only
after replacing the key-material placeholder locally. The `.ssh` directory and
file must be owned by `light-agent` with modes `0700` and `0600`. Keep the fixed
command exactly:

First make `AllowUsers light-agent` an explicit SSH admission preflight. Keep
the current administrator session open until a second terminal has proved that
a new administrator login still works. Back up `/etc/ssh/sshd_config` and every
locally managed file in `/etc/ssh/sshd_config.d/`, then inspect the effective
configuration:

```bash
sudo /usr/sbin/sshd -T | sed -n '/^allowusers /p'
```

The effective `AllowUsers` rule must contain `light-agent` **and every existing
administrator principal that must retain access**. Do not append a competing
drop-in blindly and do not replace an existing list with only `light-agent`.
Edit the authoritative rule; if there is no rule, create a locally managed
drop-in whose list is supplied and reviewed by the administrator. Before any
reload, validate both syntax and the resulting effective list:

```bash
sudo /usr/sbin/sshd -t
sudo /usr/sbin/sshd -T | sed -n '/^allowusers /p'
sudo systemctl reload ssh
```

If the configuration uses conditional `Match` blocks, also evaluate them with
`/usr/sbin/sshd -T -C` using the real user, host, and source-address tuple for
each retained access path; the context-free `-T` output does not prove a
conditional path.

After the reload, use a second terminal to establish a fresh login with an
existing administrator account and only then test `light-agent`. For rollback,
use the still-open administrator session to restore the exact backed-up files
(and remove only a newly created drop-in), run `/usr/sbin/sshd -t` again, reload
with `sudo systemctl reload ssh`, and prove a fresh administrator login. Never
reload an invalid configuration and never close the retained session before
the rollback login succeeds.

```bash
sudo install -d -o light-agent -g light-agent -m 0700 /var/lib/light-agent/.ssh
sudo install -o light-agent -g light-agent -m 0600 /path/to/prepared-authorized-keys /var/lib/light-agent/.ssh/authorized_keys
```

```text
/opt/light-agent/bin/light-agent serve-stdio
```

OpenSSH sets HOME and changes to the account home before invoking the account
shell with the forced command. The key entry disables PTY, agent forwarding,
port forwarding, X11 forwarding, and user-supplied commands. The runtime is an
NDJSON stdio process, not a network listener.

On the Mac, keep connection policy in `~/.ssh/config`:

```sshconfig
Host light-agent-vm
    HostName <PRIVATE_ADDRESS>
    User light-agent
    Port 22
    IdentityFile ~/.ssh/<DEDICATED_IDENTITY_FILE>
    IdentitiesOnly yes
    StrictHostKeyChecking yes
```

Verify a new or changed host key out of band before accepting it. Then create
an owner-only `light-agent-client.toml` from
`light-agent-client.example.toml` and run:

```bash
light-agent-client --config ./light-agent-client.toml
```

The client launches system OpenSSH as the argument vector
`ssh -T <alias> /opt/light-agent/bin/light-agent serve-stdio`; the client does
not invoke a local shell or override the user, port, identity, or host-key policy.

## Telegram MCP

Deploy the offline Linux site bundle under a separate, non-login `telegram-mcp`
account. The checked-in unit uses the system Python with the explicit
`PYTHONPATH=/opt/telegram-mcp/site`; it does not depend on `uv`, a virtual
environment, or a release symlink. The application bind address is fixed in
code to `127.0.0.1:8000`. Point the server-side MCP entry to
`http://127.0.0.1:8000/mcp` and never expose it through a public listener or SSH
forwarding.

The artifact is a trusted `.tar.gz` built for the VM's Linux and Python version.
It has exactly one top-level directory. Its separately authenticated checksum
is a digest-only file containing exactly 64 hexadecimal characters and one
newline; it contains no filename that could select a different input. Put both
files in a protected staging directory and require all checks before changing
`/opt`:

```bash
archive=/path/to/verified/telegram-mcp-site.tar.gz
checksum_file=/path/to/verified/telegram-mcp-site.tar.gz.sha256.digest
test "$(/usr/bin/wc -c < "$checksum_file")" -eq 65
expected_sha256=$(/usr/bin/tr 'A-F' 'a-f' < "$checksum_file")
test "${#expected_sha256}" -eq 64
case "$expected_sha256" in ''|*[!0-9a-f]*) echo 'invalid SHA-256 digest' >&2; exit 1;; esac
actual_sha256=$(/usr/bin/sha256sum "$archive" | /usr/bin/awk '{print $1}')
test "$actual_sha256" = "$expected_sha256"
top_level_count=$(/usr/bin/tar -tzf "$archive" | /usr/bin/awk -F/ 'NF {print $1}' | /usr/bin/sort -u | /usr/bin/wc -l)
test "$top_level_count" -eq 1
```

Create the service identity once. For an existing account, use
`getent passwd telegram-mcp` to verify the same home and non-login shell instead
of silently changing it:

```bash
sudo useradd --system --create-home --home-dir /var/lib/telegram-mcp \
  --shell /usr/sbin/nologin telegram-mcp
sudo install -d -o root -g root -m 0755 /opt/telegram-mcp
```

Stage into a new sibling directory. Refuse a stale staging tree; do not extract
over the active site. After extraction, normalize every directory to
`root:root 0755` and every regular file to `root:root 0644`:

```bash
sudo test ! -e /opt/telegram-mcp/site.new
sudo install -d -o root -g root -m 0755 /opt/telegram-mcp/site.new
sudo /usr/bin/tar --extract --gzip --file "$archive" \
  --directory /opt/telegram-mcp/site.new \
  --strip-components=1 --no-same-owner --no-same-permissions
sudo chown -R root:root /opt/telegram-mcp/site.new
sudo find /opt/telegram-mcp/site.new -type d -exec chmod 0755 {} +
sudo find /opt/telegram-mcp/site.new -type f -exec chmod 0644 {} +
```

Before stopping the live service, prove that the staged bundle imports as the
service account with an explicit, clean Python path and without loading secret
values:

```bash
sudo -u telegram-mcp /usr/bin/env -i \
  HOME=/var/lib/telegram-mcp PATH=/usr/bin:/bin \
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/opt/telegram-mcp/site.new \
  /usr/bin/python3 -c 'from telegram_mcp.server import main; assert callable(main)'
```

Create one root-only rollback set before overwriting either configuration file.
The fixed rollback path deliberately makes a second rollout fail instead of
overwriting the only recovery copy. `cp --archive --no-dereference` retains the
original owner and mode, including those of the secret environment file.

The only supported initial service cases are a true first install (the unit is
absent and systemd reports `not-found`) or an existing regular unit. The
supported existing states are exactly enabled+active or disabled+inactive.
Static, masked, generated, alias, failed, activating/deactivating, and mixed
enabled/inactive or disabled/active states require an operator-specific plan.
The following root script validates service state before creating markers, and
records service state independently from site and environment state:

```bash
sudo /bin/sh -eu <<'TELEGRAM_ROLLBACK_SNAPSHOT'
rollback=/root/telegram-mcp.rollback
unit=/etc/systemd/system/telegram-mcp.service
environment=/etc/telegram-mcp/telegram-mcp.env
test ! -e "$rollback"
test ! -e /opt/telegram-mcp/site.previous
test ! -e /opt/telegram-mcp/site.failed
load_state=$(systemctl show telegram-mcp.service --property=LoadState --value)
if test ! -e "$unit"; then
  test ! -L "$unit"
  test "$load_state" = not-found
  service_state=absent
else
  test -f "$unit"
  test ! -L "$unit"
  test "$load_state" = loaded
  fragment=$(systemctl show telegram-mcp.service --property=FragmentPath --value)
  test "$fragment" = "$unit"
  enabled_state=$(systemctl show telegram-mcp.service --property=UnitFileState --value)
  active_state=$(systemctl show telegram-mcp.service --property=ActiveState --value)
  case "$enabled_state:$active_state" in
    enabled:active) service_state=enabled-active ;;
    disabled:inactive) service_state=disabled-inactive ;;
    *) echo 'unsupported telegram-mcp service state' >&2; exit 1 ;;
  esac
fi

install -d -o root -g root -m 0700 "$rollback"
case "$service_state" in
  absent)
    install -o root -g root -m 0600 /dev/null "$rollback/unit.absent"
    install -o root -g root -m 0600 /dev/null "$rollback/service.absent"
    ;;
  enabled-active)
    cp --archive --no-dereference "$unit" "$rollback/telegram-mcp.service"
    install -o root -g root -m 0600 /dev/null "$rollback/unit.existed"
    install -o root -g root -m 0600 /dev/null "$rollback/service.enabled"
    install -o root -g root -m 0600 /dev/null "$rollback/service.active"
    ;;
  disabled-inactive)
    cp --archive --no-dereference "$unit" "$rollback/telegram-mcp.service"
    install -o root -g root -m 0600 /dev/null "$rollback/unit.existed"
    install -o root -g root -m 0600 /dev/null "$rollback/service.disabled"
    install -o root -g root -m 0600 /dev/null "$rollback/service.inactive"
    ;;
esac

if test -e "$environment"; then
  test -f "$environment"
  test ! -L "$environment"
  cp --archive --no-dereference "$environment" "$rollback/telegram-mcp.env"
  install -o root -g root -m 0600 /dev/null "$rollback/environment.existed"
else
  test ! -L "$environment"
  install -o root -g root -m 0600 /dev/null "$rollback/environment.absent"
fi
if test -e /opt/telegram-mcp/site; then
  test -d /opt/telegram-mcp/site
  test ! -L /opt/telegram-mcp/site
  install -o root -g root -m 0600 /dev/null "$rollback/site.existed"
else
  test ! -L /opt/telegram-mcp/site
  install -o root -g root -m 0600 /dev/null "$rollback/site.absent"
fi
TELEGRAM_ROLLBACK_SNAPSHOT
```

Prepare the credential file outside the repository and shell history. It
contains `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, and
`TELETHON_SESSION_STRING`; never print their values. Install it as
`root:telegram-mcp 0640` under a group-searchable, otherwise private directory.
Install the checked-in unit from a separately verified checkout or deployment
bundle:

```bash
unit_source=/path/to/verified/telegram-mcp.service
sudo install -d -o root -g telegram-mcp -m 0750 /etc/telegram-mcp
sudo install -o root -g telegram-mcp -m 0640 \
  /path/to/prepared-telegram-mcp.env /etc/telegram-mcp/telegram-mcp.env
sudo install -o root -g root -m 0644 "$unit_source" \
  /etc/systemd/system/telegram-mcp.service
sudo systemd-analyze verify /etc/systemd/system/telegram-mcp.service
sudo systemctl daemon-reload
```

Keep exactly one immediately usable rollback tree. With the service stopped,
both `mv -T` operations are same-filesystem atomic renames; the temporary
absence of `site` is not observable by a running service:

```bash
sudo systemctl stop telegram-mcp.service
if sudo test -e /opt/telegram-mcp/site; then
  sudo mv -T /opt/telegram-mcp/site /opt/telegram-mcp/site.previous
fi
sudo mv -T /opt/telegram-mcp/site.new /opt/telegram-mcp/site
sudo systemctl enable --now telegram-mcp.service
```

Verify the installed identity, command, health, and listener without dumping the
environment or journal payloads:

```bash
sudo systemctl is-active --quiet telegram-mcp.service
sudo systemctl show telegram-mcp.service --property=User --property=Group \
  --property=ExecStart --property=EnvironmentFiles --property=WorkingDirectory
sudo ss -ltnp 'sport = :8000'
```

The listener output must show `127.0.0.1:8000` and must not show
`0.0.0.0:8000` or `[::]:8000`. Verify an MCP initialize/list-tools request from
the local host using a client that does not log headers or payloads. If
acceptance fails, use only the protected markers to decide whether to restore
or remove a file. This avoids treating a missing backup as proof that the file
was new. Preserve the failed site for diagnosis. The site branch explicitly
handles the rename gap where `site.previous` exists but `site` is absent:

```bash
sudo /bin/sh -eu <<'TELEGRAM_ROLLBACK'
rollback=/root/telegram-mcp.rollback
unit=/etc/systemd/system/telegram-mcp.service
environment=/etc/telegram-mcp/telegram-mcp.env
current_load=$(systemctl show telegram-mcp.service --property=LoadState --value)
case "$current_load" in
  loaded)
    systemctl stop telegram-mcp.service
    test "$(systemctl show telegram-mcp.service --property=ActiveState --value)" = inactive
    ;;
  not-found) ;;
  *) echo 'unsupported current telegram-mcp load state' >&2; exit 1 ;;
esac

if test -f "$rollback/site.existed"; then
  if test -d /opt/telegram-mcp/site.previous; then
    if test -d /opt/telegram-mcp/site; then
      mv -T /opt/telegram-mcp/site /opt/telegram-mcp/site.failed
    else
      test ! -e /opt/telegram-mcp/site
    fi
    mv -T /opt/telegram-mcp/site.previous /opt/telegram-mcp/site
  else
    test -d /opt/telegram-mcp/site
  fi
elif test -f "$rollback/site.absent"; then
  if test -d /opt/telegram-mcp/site; then
    mv -T /opt/telegram-mcp/site /opt/telegram-mcp/site.failed
  else
    test -d /opt/telegram-mcp/site.new
  fi
else
  echo 'site rollback marker missing' >&2
  exit 1
fi

if test -f "$rollback/unit.existed"; then
  cp --archive --no-dereference "$rollback/telegram-mcp.service" "$unit"
elif test -f "$rollback/unit.absent" && test -f "$rollback/service.absent"; then
  if test "$current_load" = loaded; then
    systemctl disable telegram-mcp.service
  fi
  rm -f "$unit"
else
  echo 'unit rollback marker missing' >&2
  exit 1
fi
if test -f "$rollback/environment.existed"; then
  cp --archive --no-dereference "$rollback/telegram-mcp.env" "$environment"
elif test -f "$rollback/environment.absent"; then
  rm -f "$environment"
else
  echo 'environment rollback marker missing' >&2
  exit 1
fi
systemctl daemon-reload

if test -f "$rollback/service.enabled" && test -f "$rollback/service.active"; then
  test -f "$rollback/unit.existed"
  systemctl enable telegram-mcp.service
  systemctl restart telegram-mcp.service
  test "$(systemctl show telegram-mcp.service --property=UnitFileState --value)" = enabled
  test "$(systemctl show telegram-mcp.service --property=ActiveState --value)" = active
  ss -ltnp 'sport = :8000'
elif test -f "$rollback/service.disabled" && test -f "$rollback/service.inactive"; then
  test -f "$rollback/unit.existed"
  systemctl disable telegram-mcp.service
  systemctl stop telegram-mcp.service
  test "$(systemctl show telegram-mcp.service --property=UnitFileState --value)" = disabled
  test "$(systemctl show telegram-mcp.service --property=ActiveState --value)" = inactive
elif test -f "$rollback/service.absent" && test -f "$rollback/unit.absent"; then
  test "$(systemctl show telegram-mcp.service --property=LoadState --value)" = not-found
else
  echo 'service rollback markers missing or inconsistent' >&2
  exit 1
fi
TELEGRAM_ROLLBACK
```

On a first install, the `.absent` markers authorize removal only of the files
created by that rollout; no service is restarted after removing a first-install
unit or site. Remove `site.failed`, retire `site.previous`, and delete the
rollback directory only after successful acceptance or separately verified
recovery. The credential file must be excluded from Git, diagnostic bundles,
process arguments, and ordinary backups unless the backup is explicitly
protected as secret material.

## Inspection and export

The terminal client supports multiple dialogs and the read-only views used by
`/dialogs`, `/history`, `/jobs`, `/job`, `/runs`, `/audit`, and `/dump`.
`/export <local-path>` streams a bounded JSONL export to the Mac, verifies byte
count and SHA-256 incrementally, fsyncs, and atomically renames only after a
complete transfer. The local path is never sent to the server.

The SQL shell is deliberately VM-local and uses the same home/config contract:

```bash
sudo -u light-agent -H sh -c 'cd /var/lib/light-agent && exec /opt/light-agent/bin/light-agent db-shell --readonly'
```

It accepts only one bounded read statement and must not be exposed through the
SSH forced command. Prefer logical `/dump` or `/export` for ordinary inspection.

Schedule create/update/enable/disable/delete requests require an explicit,
terminal-safe confirmation showing the action, job identity, name, schedule,
timezone, and full task text. An unanswered or expired confirmation must not
mutate SQLite or the crontab.

## Backup and restore

The configuration, database, and exports are sensitive: they can contain dialog
text, scheduled task text, tool audit metadata, model output, and credentials.
Encrypt backups, restrict their ownership, and never paste their content into
logs or tickets. Do not copy only the main SQLite file while writers are active.

For an authorized backup window, first prevent new SSH sessions and cron starts,
wait for existing `light-agent` processes to exit, then use SQLite's online
backup operation to a pre-created protected destination. Record a checksum but
not file contents. Back up the configuration separately under the same secret
handling policy. Re-enable entry points only after the backup succeeds.

For restore, keep the service quiescent, retain the previous database and its
sidecars as a recoverable rollback set, place the restored database at
`/var/lib/light-agent/state.sqlite3` with owner `light-agent:light-agent` and
mode `0600`, and validate startup. Then run `cron-sync` from the runtime home and
compare the managed crontab block with `/jobs`. Never merge arbitrary sidecars
from different snapshots.

## Startup and crash recovery

Every `serve-stdio`, `run-job`, and `cron-sync` process acquires a runtime-owner
lease before mutating durable work. Startup serializes owner inspection with a
coordinator lock. A recorded owner is recovered only when its exact owner lock
can be acquired exclusively, proving that process is dead. A live owner keeps
its lock and is left untouched, including work belonging to another concurrent
SSH session or cron process.

Recovery is terminal and does not replay provider or MCP calls:

- a pending read-only tool becomes `failed` with `process_interrupted`;
- a pending write tool becomes `uncertain` with `process_interrupted`, because
  the external side effect may already have happened;
- a pending interactive turn becomes `interrupted`, so no partial assistant
  answer is committed as completed;
- a pending cron run becomes `interrupted`; it is not reported as completed.

Inspect `/audit`, `/history`, and `/runs` after recovery. Never automatically
retry an uncertain write tool; verify the external system first. If ownership
metadata or lock identity cannot be verified, startup fails closed instead of
guessing that work is dead.

After an authorized reboot:

1. Verify the `light-agent` passwd home, file ownership, `0700` home, `0600`
   configuration/database, root-owned executable, and Telegram loopback service.
2. Confirm no unexpected public listener exists and inspect the `light-agent`
   crontab without printing task text (the crontab contains IDs only).
3. Connect once through the Mac client; startup recovery should terminalize only
   provably dead owners. Use `/dialogs`, `/history`, `/jobs`, `/runs`, `/audit`,
   and `/dump` to verify durable state without raw database access.
4. Run `cron-sync` from `/var/lib/light-agent`, re-read the crontab, and verify
   scheduled jobs are reconciled. If reconciliation fails, leave saved jobs in
   their visible failed/pending sync state and repair Cronie before retrying.
5. Keep one live client turn active while opening a second session and verify the
   live owner remains pending rather than being recovered. Perform destructive
   crash simulation only in a separately approved test window.
6. If startup validation fails, keep cron and SSH entry points quiescent and use
   the restore procedure above; do not delete the rollback database or sidecars
   until dialogs, jobs, runs, audit rows, and a new backup are verified.

## Live acceptance gate

First run all deterministic checks documented in the repository README. Then,
in an explicitly authorized window, set `LIGHT_AGENT_LIVE=1` and
`LIGHT_AGENT_CLIENT_CONFIG` to an owner-only Mac client file and invoke ignored
tests individually. Set `LIGHT_AGENT_CRON_LIVE=1` only while authorizing the
confirmed create/delete cron roundtrip. Reboot recovery additionally uses a
non-sensitive persisted dialog ID prepared before the reboot.

Record only commands, timestamps, safe status codes, and redacted outcomes in
`docs/day18-results.md`. Never record credentials, task text, model responses,
raw MCP payloads, raw database rows, key material, or private host addresses.
