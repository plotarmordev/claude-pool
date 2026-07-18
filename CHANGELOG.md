# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- `ClaudePool(tui_ready_timeout=...)` and `claude-pool serve --tui-ready-timeout` to configure
  the TUI worker readiness deadline. The default is unchanged at 30 seconds.
- `ClaudePool(spawn_concurrency=...)` and `claude-pool serve --spawn-concurrency` to cap
  simultaneous worker cold starts. The default is unchanged: unbounded.

## [0.2.2] - 2026-06-13

### Fixed

- TUI prompt sanitization now also strips Unicode C1 control characters.

## [0.2.1] - 2026-06-13

### Fixed

- TUI `system_prompt` now replaces the session system prompt instead of appending to it.
- TUI prompts are sanitized of control characters before bracketed paste.

## [0.2.0] - 2026-06-13

### Added

- TUI backend that drives plain `claude` in a pty through Stop-hook turn results, without `-p`.
- Backend selection with `ClaudePool(backend=...)`, `claude-pool serve --backend`, and daemon status reporting.
- `claude-pool doctor --backend stream-json|tui|both` coverage.
- Cross-backend parity tests for shared pool, session, timeout, and cleanup contracts.

### Fixed

- v0.1.x daemon metadata, client timeout, protocol fixture, and TUI worker hygiene issues.

## [0.1.0] - 2026-06-12

### Added

- Stream-json worker lifecycle with bounded stderr capture and POSIX process-group cleanup.
- Warm worker pool with checkout, retirement, idle expiry, replenishment, and clean shutdown.
- Explicit multi-turn sessions for async and sync callers.
- Sync mirrors for one-shot asks, sessions, pool close, and context-manager usage.
- Unix-socket daemon with ask/status protocol, CLI ask/status clients, and doctor command.
- Test coverage across Python 3.10, 3.11, 3.12, and 3.13.
