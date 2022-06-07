import asyncio
import os
from concurrent.futures.process import ProcessPoolExecutor

import asyncpg
import discord
from core import bot_config
from discord.ext import commands


def marco(response: str):
    return response


class StatBot(commands.Bot):
    def __init__(self, description: str, pm_help: bool, intents: discord.Intents):
        super().__init__(
            command_prefix=bot_config.BOT_PREFIX,
            description=description,
            pm_help=pm_help,
            intents=intents
        )

        self.initial_extensions = [
            'cogs.cogs',
            'cogs.synchronization',
            'cogs.user_commands',
            'cogs.dev_commands',
            'cogs.block',
            'cogs.vocab'
        ]

        self.process_executor = None

        self.del_queue_empty = asyncio.Event()
        self.del_queue_empty.set()

        self.shutting_down = False

        self.exit_code = 0
        self.pool = None

    async def setup_hook(self) -> None:
        self.process_executor = ProcessPoolExecutor(os.cpu_count())

        process_nums = list()
        for x in range(os.cpu_count()):
            process_nums.append(x)

        results = self.process_executor.map(marco, process_nums, chunksize=1)
        if len(list(results)) == os.cpu_count():
            print('Opened ' + str(os.cpu_count()) + ' processes successfully')

        try:
            self.pool = await asyncpg.create_pool(
                min_size=2,
                max_size=15,
                host=bot_config.DB_HOST,
                port=bot_config.DB_PORT,
                user=bot_config.DB_USER,
                database=bot_config.DB,
                password=bot_config.DB_PASS
            )
            print('Database connection established')
        except (
            asyncpg.InterfaceError, asyncpg.InvalidCatalogNameError, asyncpg.InvalidPasswordError, OSError,  Exception
        ) as e:
            print('Connection Exception: Unable to connect to database')
            print('{!r}: errno is {}'.format(e, e.args[0]))
            return

        for extension in self.initial_extensions:
            if extension == '':
                continue
            try:
                await self.load_extension(extension)
                print('{} extension loaded'.format(extension))
            except (
                commands.ExtensionNotFound,
                commands.ExtensionAlreadyLoaded,
                commands.NoEntryPointError,
                commands.ExtensionFailed
            ) as e:
                print(e)
                exc = '{}: {}'.format(type(e).__name__, e)
                print('Failed to load extension {}\n{}'.format(extension, exc))
                print('No further extensions will be loaded ')

    async def close(self) -> None:
        try:
            await asyncio.wait_for(self.pool.close(), timeout=120)
            self.pool = None
            print('Closed DB connection pool')
        except (Exception, asyncio.CancelledError) as e:
            print('Something went wrong while trying to close the DB connection pool')
            print('{}: {}'.format(type(e).__name__, e))

        self.process_executor.shutdown()
        print('Closed process pool successfully')

        await super().close()
