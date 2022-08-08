import asyncio
import sys
from typing import Optional

import discord
from discord import Interaction
from discord.app_commands import AppCommandError

import cogs.block
from core import bot_config
from core.statbot import StatBot

description = '''Bot focused on statistics/data for both utility and fun.'''

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = StatBot(description=description, pm_help=True, intents=intents)

tree = bot.tree


@bot.event
async def on_ready():
    print('------')
    print('Logged in as')
    print(bot.user.name)
    print(bot.user.id)
    print('------')
    bot.owner_id = bot_config.BOT_OWNER


@tree.error
async def on_app_command_error(interaction: Interaction, error: AppCommandError):
    print(error)


def get_block_cog() -> Optional[cogs.block.Block]:
    return bot.get_cog('Block')


@bot.event
async def on_message(message):
    block_cog = get_block_cog()
    if not bot.shutting_down:
        if block_cog is not None:
            valid_cmd = await block_cog.check_valid_command(message)
            blocked = False
            if valid_cmd:
                blocked = await block_cog.check_block_list(message)
            if not (valid_cmd and blocked):
                await bot.process_commands(message)
        else:
            await bot.process_commands(message)


async def main():
    async with bot:
        try:
            await bot.start(bot_config.BOT_TOKEN)
        except discord.LoginFailure as e:
            print('Login Failure: {}'.format(e))

        print('Exit code: ' + str(bot.exit_code))
        return bot.exit_code


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
