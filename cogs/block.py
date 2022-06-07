import re

import discord
from discord.ext import commands

from core.statbot import StatBot
from core.utility import get_conn


class Block(commands.Cog):
    def __init__(self, bot: StatBot) -> None:
        self.bot = bot

    async def _is_blocked(self, user_id: int) -> bool:
        async with get_conn(self.bot) as conn:
            list_ = await conn.fetch(
                'SELECT UserID FROM statbot_db.USERS '
                'WHERE Restricted = $1',
                True
            )
            results = [item for sublist in list_ for item in sublist]

            if user_id not in results:
                return False
            return True

    async def _dm_block_event(self, message: discord.Message) -> None:
        solyx = self.bot.get_user(122800503846338563)
        if solyx.dm_channel is None:
            await solyx.create_dm()

        if not isinstance(message.channel, discord.DMChannel):
            await solyx.dm_channel.send(
                'A blocked user has attempted to use the bot\n'
                '**Name:** {}\n**Guild:** {}\n**Channel:** {}'
                ''.format(message.author.name,
                          message.guild.name,
                          message.channel.name))
        else:
            await solyx.dm_channel.send('A blocked user has attempted to use the bot through DMs.\n'
                                        '**Name:** {}'.format(message.author.name))

    async def check_valid_command(self, message: discord.Message) -> bool:
        prefix_str = ''.join(list(await self.bot.get_prefix(message)))
        reg_ex = r"^" + prefix_str + "(.*?)( |$)"
        match = re.search(reg_ex, message.content)

        cmd = None
        if match is not None:
            cmd = self.bot.get_command(match.group(1))

        return True if cmd else False

    async def check_block_list(self, message: discord.Message) -> bool:
        blocked = await self._is_blocked(message.author.id)
        if blocked is False or self.bot.owner_id == message.author.id:
            return False

        await message.channel.send('🛑 **| You\'ve been blocked from using this bot**', delete_after=3)

        await self._dm_block_event(message)

        return True

    @commands.command(hidden=True, name='block')
    @commands.is_owner()
    async def block(self, ctx: commands.Context, user_id: str) -> None:
        async with get_conn(self.bot) as conn:
            try:
                user_id = int(user_id)
            except ValueError:
                await ctx.send('Please enter a valid user ID.')
                return

            async with conn.transaction():
                # Check if user is in the database
                row = await conn.fetchrow(
                    'SELECT * FROM statbot_db.USERS WHERE UserID = $1 FOR KEY SHARE',
                    user_id
                )
                if row:
                    if row is True:
                        await ctx.send('User is already blocked.'.format(user_id))
                        return
                    else:
                        await conn.execute(
                            'UPDATE statbot_db.USERS SET Restricted = $1 '
                            'WHERE UserID = $2',
                            True,
                            user_id
                        )
                        await ctx.send('`{}` has been blocked from using the bot.'.format(user_id))
                else:
                    await ctx.send('Please enter a valid user ID.')
                    return

    @commands.command(hidden=True, name='unblock')
    @commands.is_owner()
    async def unblock(self, ctx: commands.Context, user_id: str) -> None:
        async with get_conn(self.bot) as conn:
            try:
                user_id = int(user_id)
            except ValueError:
                await ctx.send('Please enter a valid user ID.')
                return

            async with conn.transaction():
                # Check if user is in the database
                row = await conn.fetchrow(
                    'SELECT * FROM statbot_db.USERS WHERE UserID = $1 FOR KEY SHARE',
                    user_id
                )
                if row:
                    if row is False:
                        await ctx.send('User is not blocked.'.format(user_id))
                        return
                    else:
                        await conn.execute(
                            'UPDATE statbot_db.USERS SET Restricted = $1 '
                            'WHERE UserID = $2', False,
                            user_id
                        )
                        await ctx.send('`{}` has been unblocked'.format(user_id))
                else:
                    await ctx.send('Please enter a valid user ID.')
                    return

    @commands.command(hidden=True, name='blocklist')
    @commands.is_owner()
    async def blocklist(self, ctx: commands.Context) -> None:
        async with get_conn(self.bot) as conn:
            rows = await conn.fetch(
                'SELECT UserID FROM statbot_db.USERS '
                'WHERE Restricted = $1',
                True
            )
            if not rows:
                await ctx.send('There are currently no blocked users.')
                return

            results = [item for sublist in rows for item in sublist]
            embed_str = '```\nName/User ID\n'
            tabs = '  '
            for idx, user_id in enumerate(results):
                try:
                    # target = await commands.UserConverter().convert(ctx, str(user_id))
                    target = self.bot.get_user(user_id)
                except commands.errors.BadArgument:
                    target = None
                if target is None:
                    embed_str += '[{}] {}\n{}-> Name not found\n\n'.format((idx + 1), user_id, tabs)
                else:
                    embed_str += '[{}] {}\n{}-> {}\n\n'.format((idx + 1), target.name, tabs, user_id)

            embed_str += '```'
            embed = discord.Embed(colour=discord.Colour(0xff0000))
            embed.add_field(name='🛑\t\t| Blocked Users List |\t\t🛑', value=embed_str)

            await ctx.send(embed=embed)


async def setup(bot: StatBot) -> None:
    await bot.add_cog(Block(bot))
