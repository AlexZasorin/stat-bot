from core.statbot import StatBot
from discord.ext import commands


class Cogs(commands.Cog):
    def __init__(self, bot: StatBot) -> None:
        self.bot = bot

    @commands.command(hidden=True)
    @commands.is_owner()
    async def load(self, ctx: commands.Context, extension_name: str) -> None:
        extension_name = 'cogs.' + extension_name
        try:
            await self.bot.load_extension(extension_name)
        except (
                commands.ExtensionNotFound,
                commands.ExtensionAlreadyLoaded,
                commands.NoEntryPointError,
                commands.ExtensionFailed
        ) as e:
            await ctx.send("```py\n{}: {}\n```".format(type(e).__name__, str(e)))
            return

        await ctx.send("`{}` loaded.".format(extension_name))

    @commands.command(hidden=True)
    @commands.is_owner()
    async def unload(self, ctx: commands.Context, extension_name: str) -> None:
        extension_name = 'cogs.' + extension_name
        try:
            await self.bot.unload_extension(extension_name)
        except (commands.ExtensionNotLoaded, commands.ExtensionNotFound) as e:
            await ctx.send("```py\n{}: {}\n```".format(type(e).__name__, str(e)))
            return

        await ctx.send("`{}` unloaded.".format(extension_name))

    @commands.command(hidden=True)
    @commands.is_owner()
    async def reload(self, ctx: commands.Context, extension_name: str) -> None:
        extension_name = 'cogs.' + extension_name
        try:
            await self.bot.reload_extension(extension_name)
        except (
                   commands.ExtensionNotFound,
                   commands.ExtensionNotLoaded,
                   commands.NoEntryPointError,
                   commands.ExtensionFailed
        ) as e:
            await ctx.send("```py\n{}: {}\n```".format(type(e).__name__, str(e)))
            return

        await ctx.send("`{}` loaded.".format(extension_name))


async def setup(bot: StatBot) -> None:
    await bot.add_cog(Cogs(bot))
