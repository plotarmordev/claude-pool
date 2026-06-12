from __future__ import annotations

# Do not mix async and sync methods on one ClaudePool instance.

from claude_pool import ClaudePool


def main() -> None:
    pool = ClaudePool()
    try:
        result = pool.ask_sync("Reply with exactly: one-shot")
        print("one_shot:", result.text)

        with pool.session_sync() as session:
            first = session.send("Remember the word: river.")
            second = session.send("What word did I ask you to remember?")
            print("session_id:", first.session_id)
            print("first:", first.text)
            print("second:", second.text)
    finally:
        pool.close()


if __name__ == "__main__":
    main()
