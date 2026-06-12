# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] - 2026-06-12

### Added

- Stream-json worker lifecycle with bounded stderr capture and POSIX process-group cleanup.
- Warm worker pool with checkout, retirement, idle expiry, replenishment, and clean shutdown.
- Explicit multi-turn sessions for async and sync callers.
- Sync mirrors for one-shot asks, sessions, pool close, and context-manager usage.
- Unix-socket daemon with ask/status protocol, CLI ask/status clients, and doctor command.
- Test coverage across Python 3.10, 3.11, 3.12, and 3.13.
