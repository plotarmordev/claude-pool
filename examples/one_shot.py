from __future__ import annotations

import asyncio

from claude_pool import ClaudePool


async def main() -> None:
    async with ClaudePool() as pool:
        result = await pool.ask("Reply with one short sentence about warm worker pools.")

    print("text:", result.text)
    print("is_error:", result.is_error)
    print("subtype:", result.subtype)
    print("session_id:", result.session_id)
    print("usage:", result.usage)
    print("cost_usd:", result.cost_usd)
    print("duration_ms:", result.duration_ms)
    print("rate_limit:", result.rate_limit)


if __name__ == "__main__":
    asyncio.run(main())
