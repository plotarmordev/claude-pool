# claude-pool

**`claude -p`, without the cold start and the black box.**

A tiny, zero-dependency Python utility that runs the Claude Code CLI headlessly
the way you run it interactively: persistent warm worker processes on your
existing subscription login, structured JSON in/out, fresh context per request
unless you explicitly open a conversation.

> **Work in progress.** v0.1 is being built in the open — see
> [GOAL-pool-v01.md](GOAL-pool-v01.md) for the build plan and
> [PROTOCOL.md](PROTOCOL.md) for the observed stream-json worker protocol it
> is built on. Not affiliated with Anthropic.
