# Shell Usage

`claude-pool` can run as a Unix-socket daemon so non-Python programs can send
one JSON request per line and read one JSON response per line.

## Doctor

`doctor` checks the Claude Code binary, runs `claude --version`, and makes one
real test request.

```sh
claude-pool doctor
claude-pool doctor --claude-bin /path/to/claude --timeout 60
```

## Serve

Start one daemon for one profile:

```sh
claude-pool serve \
  --socket /tmp/claude-pool-default.sock \
  --warm 1 \
  --max-workers 4
```

Use the TUI backend when you need to avoid Claude Code print mode:

```sh
claude-pool serve \
  --backend tui \
  --socket /tmp/claude-pool-tui.sock \
  --warm 1
```

The default socket is `$XDG_RUNTIME_DIR/claude-pool.sock` when
`XDG_RUNTIME_DIR` is set, otherwise `/tmp/claude-pool-$(id -u).sock`.

## Ask

Send one prompt through the daemon:

```sh
claude-pool ask "Reply with exactly: OK" --socket /tmp/claude-pool-default.sock
claude-pool ask "This may take longer" --timeout 120 --socket /tmp/claude-pool-default.sock
```

`ask` prints result text to stdout. It exits 1 for daemon errors, timeouts, and
Claude result messages with `is_error: true`.

## Status

```sh
claude-pool status --socket /tmp/claude-pool-default.sock
```

Status prints warm worker count, in-flight request count, daemon pid, and a
small profile summary.

## Multiple Profiles

Run one daemon per profile and give each daemon a distinct socket:

```sh
claude-pool serve \
  --socket /tmp/claude-pool-sonnet.sock \
  --model sonnet \
  --warm 2

claude-pool serve \
  --socket /tmp/claude-pool-opus.sock \
  --model opus \
  --warm 1

claude-pool ask "Use the sonnet profile" --socket /tmp/claude-pool-sonnet.sock
claude-pool ask "Use the opus profile" --socket /tmp/claude-pool-opus.sock
```

## systemd --user Sketch

Save as `~/.config/systemd/user/claude-pool.service` and adjust paths:

```ini
[Unit]
Description=claude-pool default profile

[Service]
ExecStart=%h/.local/bin/claude-pool serve --socket %t/claude-pool.sock --warm 1
Restart=on-failure

[Install]
WantedBy=default.target
```

Then run:

```sh
systemctl --user daemon-reload
systemctl --user enable --now claude-pool.service
claude-pool status --socket "$XDG_RUNTIME_DIR/claude-pool.sock"
```
