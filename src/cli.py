import asyncio
from dotenv import load_dotenv
from config.settings import Settings
from axis.replication.service import run


async def main():
    # Load environment variables from a .env file
    load_dotenv()

    settings = Settings()
    await run(settings)


if __name__ == "__main__":
    asyncio.run(main())
