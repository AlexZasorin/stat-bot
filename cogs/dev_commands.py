import discord
from core.statbot import StatBot
from discord.ext import commands


class DevCommands(commands.Cog):
    def __init__(self, bot: StatBot) -> None:
        self.bot = bot

    async def _shutdown_bot(self, restart: bool = False) -> None:
        print('Shutting the bot down')

        self.bot.shutting_down = True

        if restart:
            self.bot.exit_code = 2
        else:
            self.bot.exit_code = 0

        await self.bot.close()

    @commands.command(hidden=True)
    @commands.is_owner()
    async def nick(self, ctx: commands.Context, *args: str) -> None:
        if args is None:
            await ctx.me.edit(nick=None)
        else:
            str_ = ' '.join(args)
            await ctx.me.edit(nick=str_)

    @commands.command(hidden=True)
    @commands.is_owner()
    async def dbstats(self, ctx: commands.Context) -> None:
        p = self.bot.pool
        await ctx.send('Database Pool Connection Status Report:\nMin Size: {}, Max Size: {}, Size: {}, Free Size: {}'
                       ''.format(p.minsize, p.maxsize, p.size, p.freesize))

    @commands.command(hidden=True)
    @commands.is_owner()
    async def say(self, ctx: commands.Context, *args: str) -> None:
        msg = ' '.join(args)
        await ctx.send(msg)
        await ctx.message.delete()

    @commands.command(hidden=True)
    @commands.is_owner()
    async def shutdown(self, ctx: commands.Context, *args: str) -> None:
        await ctx.send('Goodbye!')

        await self._shutdown_bot(restart=False)

    @commands.command(hidden=True)
    @commands.is_owner()
    async def restart(self, ctx: commands.Context) -> None:
        await ctx.send('Restarting...')
        await self._shutdown_bot(restart=True)

    @commands.command(hidden=True)
    @commands.is_owner()
    async def beta_sync(self, ctx: commands.Context) -> None:
        await self.bot.tree.sync(guild=discord.Object(450727992025415691))
        print('Commands synced to beta test server')


async def setup(bot: StatBot) -> None:
    await bot.add_cog(DevCommands(bot))
