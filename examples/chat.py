from __future__ import annotations

import asyncio

from claude_pool import ClaudePool


async def main() -> None:
    prompts = [
        "Choose a small integer and reply with only the number.",
        "Double it and reply with only the result.",
        "What was my first number?",
    ]

    async with ClaudePool() as pool:
        async with pool.session() as session:
            for index, prompt in enumerate(prompts, start=1):
                result = await session.send(prompt)
                print(f"turn {index} session_id:", result.session_id)
                print(f"turn {index} text:", result.text)


if __name__ == "__main__":
    asyncio.run(main())
