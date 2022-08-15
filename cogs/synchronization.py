import datetime
import logging
import time

import asyncpg
import dateutil.parser
import discord
from discord.ext import commands

from core.statbot import StatBot
from core.utility import get_conn


class Synchronization(commands.Cog):
    def __init__(self, bot: StatBot) -> None:
        self.bot = bot
        self.log = logging.getLogger('statbot')
        self.log.setLevel(level=logging.INFO)

    async def _remove_text_channel(self, channel: discord.TextChannel) -> None:
        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    'SELECT ChannelID FROM statbot_db.CHANNELS '
                    'WHERE ChannelID = $1 FOR UPDATE',
                    channel.id
                )

                if not rows:
                    self.log.info('FAILED TO REMOVE CHANNEL: Channel not in DB')
                    self.log.debug(channel)
                    return

                await conn.execute(
                    'DELETE FROM statbot_db.CHANNELS '
                    'WHERE ChannelID = $1',
                    channel.id
                )

    async def _add_text_channel(self, channel: discord.TextChannel) -> None:
        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    'SELECT ServerID FROM statbot_db.SERVERS '
                    'WHERE ServerID = $1 FOR KEY SHARE',
                    channel.guild.id
                )

                if not rows:
                    self.log.info('FAILED TO ADD CHANNEL: Server not in DB')
                    self.log.debug(channel.guild)
                    self.log.debug(channel)
                    return

                await conn.execute(
                    'INSERT INTO statbot_db.CHANNELS (ChannelID, ServerID, Importing) '
                    'VALUES ($1, $2, $3)',
                    channel.id,
                    channel.guild.id,
                    True
                )

                await self._add_messages(channel, conn)

                await conn.execute(
                    'UPDATE statbot_db.CHANNELS SET Importing = $1 '
                    'WHERE ChannelID = $2',
                    False,
                    channel.id
                )

    async def _add_user(self, member: discord.Member) -> None:
        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    'SELECT ServerID FROM statbot_db.SERVERS '
                    'WHERE ServerID = $1 FOR KEY SHARE',
                    member.guild.id
                )

                if not rows:
                    self.log.info('FAILED TO ADD USER: Server not in DB')
                    self.log.debug(member.guild)
                    self.log.debug(member)
                    return

                try:
                    await conn.execute(
                        'INSERT INTO statbot_db.USERS VALUES ($1, $2)',
                        member.id,
                        False
                    )
                except asyncpg.UniqueViolationError as e:
                    self.log.info('User already exists in DB (EXPECTED)')
                    self.log.debug(e)

                try:
                    await conn.execute(
                        'INSERT INTO statbot_db.HAS_USERS VALUES ($1, $2)',
                        member.guild.id,
                        member.id
                    )
                except asyncpg.UniqueViolationError as e:
                    self.log.debug(e)
                    return

    async def _remove_user(self, member: discord.Member) -> None:
        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    'SELECT ServerID FROM statbot_db.SERVERS '
                    'WHERE ServerID = $1 FOR UPDATE',
                    member.guild.id
                )
                if not rows:
                    self.log.info('FAILED TO REMOVE USER: Server not in DB')
                    self.log.debug(member.guild)
                    self.log.debug(member)
                    return

                try:
                    await conn.execute(
                        'DELETE FROM statbot_db.HAS_USERS '
                        'WHERE UserID = $1 AND ServerID = $2',
                        member.id,
                        member.guild.id
                    )
                except Exception as e:
                    self.log.error('{!r}: errno is {}'.format(e, e.args[0]))
                    return

    async def _delete_message(self, message_id: int) -> None:
        async with get_conn(self.bot) as conn:
            try:
                await conn.execute(
                    'DELETE FROM statbot_db.MESSAGES '
                    'WHERE MessageID = $1',
                    message_id
                )
            except Exception as e:
                self.log.info('FAILED TO DELETE MESSAGE: Message is not in DB.')
                self.log.debug(message_id)
                self.log.debug('{!r}: errno is {}'.format(e, e.args[0]))
                return

    async def _bulk_delete_message(self, message_ids: set[int]) -> None:
        async with get_conn(self.bot) as conn:
            for message_id in message_ids:
                try:
                    await conn.execute(
                        'DELETE FROM statbot_db.MESSAGES '
                        'WHERE MessageID = $1',
                        message_id
                    )
                except Exception as e:
                    self.log.info('FAILED TO BULK DELETE MESSAGE: Message is not in DB')
                    self.log.debug(message_id)
                    self.log.debug('{!r}: errno is {}'.format(e, e.args[0]))
                    continue
                self.log.info('Bulk message deleted: ' + str(message_id))

    async def _update_message(self, message_id: int, content: str, edited_timestamp: datetime.datetime) -> None:
        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    'SELECT MessageID FROM statbot_db.MESSAGES '
                    'WHERE MessageID = $1 FOR KEY SHARE',
                    message_id
                )
                if not rows:
                    self.log.info('FAILED TO UPDATE MESSAGE: Message is not in DB')
                    self.log.debug(message_id)
                    return

                try:
                    await conn.execute(
                        'UPDATE statbot_db.MESSAGES '
                        'SET Content = $1, EditTime = $2 '
                        'WHERE MessageID = $3',
                        content,
                        edited_timestamp,
                        message_id
                    )
                except Exception as e:
                    self.log.info('FAILED TO UPDATE MESSAGE: Message is not in DB')
                    self.log.debug(message_id)
                    return

    async def _log_message(self, message: discord.Message) -> None:
        async with get_conn(self.bot) as conn:
            if message.type != discord.MessageType.default and message.type != discord.MessageType.reply:
                self.log.info('FAILED TO LOG MESSAGE: Type not default or reply')
                self.log.debug(message)
                self.log.debug('Message content: {}'.format(message.content))
                return

            async with conn.transaction():
                rows = await conn.fetch(
                    'SELECT ServerID FROM statbot_db.SERVERS '
                    'WHERE ServerID = $1 FOR KEY SHARE',
                    message.guild.id
                )
                if not rows:
                    self.log.info('FAILED TO LOG MESSAGE: Server not in DB')
                    self.log.debug(message)
                    self.log.debug(message.guild)
                    return

                rows = await conn.fetch(
                    'SELECT ChannelID FROM statbot_db.CHANNELS '
                    'WHERE ChannelID = $1 FOR KEY SHARE',
                    message.channel.id
                )
                if not rows:
                    self.log.info('FAILED TO LOG MESSAGE: Channel not yet added')
                    self.log.debug(message)
                    self.log.debug(message.channel)
                    return

                attachment_url_list = [attachment.url for attachment in message.attachments]

                await conn.execute(
                    'INSERT INTO statbot_db.MESSAGES VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
                    message.id,
                    None if not message.reference else message.reference.message_id,
                    message.content,
                    attachment_url_list,
                    message.created_at,
                    message.edited_at,
                    message.guild.id,
                    message.channel.id,
                    message.author.id
                )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.guild is None:
            return
        await self._log_message(message)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent) -> None:
        channel = self.bot.get_channel(payload.channel_id)
        if (isinstance(channel, discord.DMChannel) or
                isinstance(channel, discord.GroupChannel)):
            self.log.info('FAILED TO DELETE MESSAGE: Ignoring DMChannel/GroupChannel')
            return

        await self._delete_message(payload.message_id)

    @commands.Cog.listener()
    async def on_raw_bulk_message_delete(self, payload: discord.RawBulkMessageDeleteEvent) -> None:
        await self._bulk_delete_message(payload.message_ids)

    @commands.Cog.listener()
    async def on_raw_message_edit(self, payload: discord.RawMessageUpdateEvent) -> None:
        channel = self.bot.get_channel(payload.data['channel_id'])
        if (isinstance(channel, discord.DMChannel) or
                isinstance(channel, discord.GroupChannel)):
            self.log.info('FAILED TO UPDATE MESSAGE: Ignoring DMChannel/GroupChannel')
            return

        if 'content' not in payload.data:
            self.log.info('FAILED TO UPDATE MESSAGE: Embed only edit {}'.format(payload.message_id))
            return

        message_id = payload.message_id
        content = payload.data['content']
        edited_timestamp = payload.data['edited_timestamp']

        if edited_timestamp is not None:
            edited_timestamp = dateutil.parser.parse(edited_timestamp, ignoretz=True)

        await self._update_message(message_id, content, edited_timestamp)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member) -> None:
        await self._add_user(member)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member) -> None:
        await self._remove_user(member)

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel) -> None:
        if isinstance(channel, discord.TextChannel):
            await self._remove_text_channel(channel)

    @commands.Cog.listener()
    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel) -> None:
        if isinstance(channel, discord.TextChannel):
            await self._add_text_channel(channel)

    @commands.Cog.listener()
    async def on_guild_channel_update(self, before: discord.abc.GuildChannel, after: discord.abc.GuildChannel) -> None:
        if isinstance(before, discord.TextChannel) and isinstance(after, discord.TextChannel):
            pre = (before.permissions_for(before.guild.me).read_message_history and
                   before.permissions_for(before.guild.me).read_messages)
            post = (after.permissions_for(after.guild.me).read_message_history and
                    after.permissions_for(after.guild.me).read_messages)
            self.log.info('Pre: {}, Post: {}'.format(pre, post))
            self.log.info(before.guild)
            if pre is True and post is False:
                await self._remove_text_channel(after)
                self.log.info('Removing text channel')
            elif pre is False and post is True:
                await self._add_text_channel(after)
                self.log.info('Adding text channel')

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member) -> None:
        if before.id != self.bot.user.id:
            return

        for text_channel in after.guild.text_channels:
            pre_perm = text_channel.permissions_for(before).read_message_history
            post_perm = text_channel.permissions_for(after).read_message_history

            self.log.info('Pre: {}, Post: {}'.format(pre_perm, post_perm))
            self.log.info(before.guild)

            if pre_perm is True and post_perm is False:
                await self._remove_text_channel(text_channel)
                self.log.info('Removing text channel')
            elif pre_perm is False and post_perm is True:
                await self._add_text_channel(text_channel)
                self.log.info('Adding text channel')

    async def _add_messages(self, text_channel: discord.TextChannel, conn: asyncpg.Connection) -> None:
        msg_queue = list()
        counter = 0

        async with conn.transaction():
            async for message in text_channel.history(limit=None):
                if message.type != discord.MessageType.default and message.type != discord.MessageType.reply:
                    self.log.info('FAILED TO LOG MESSAGE: Type not default or reply')
                    self.log.debug(message)
                    self.log.debug('Message content: {}'.format(message.content))
                    continue

                attachment_url_list = [attachment.url for attachment in message.attachments]

                msg_queue.append([
                    message.id,
                    None if not message.reference else message.reference.message_id,
                    message.content,
                    attachment_url_list,
                    message.created_at,
                    message.edited_at,
                    message.guild.id,
                    message.channel.id,
                    message.author.id
                ])

                counter += 1
                if counter % 1000 == 0:
                    await conn.executemany(
                        'INSERT INTO statbot_db.MESSAGES '
                        'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
                        msg_queue
                    )
                    msg_queue.clear()

                if counter % 10000 == 0:
                    self.log.info('Fetched 10000 messages from {} channel in {} '
                                  'server'.format(text_channel.name, text_channel.guild.name))

            if counter % 10000 != 0:
                self.log.info('Fetched {} messages from {} channel, '
                              '{} server'.format((counter % 10000),
                                                 text_channel.name, text_channel.guild.name))
                await conn.executemany(
                    'INSERT INTO statbot_db.MESSAGES '
                    'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
                    msg_queue
                )
                msg_queue.clear()

    async def _server_add_users(self, guild: discord.Guild, conn: asyncpg.Connection) -> None:
        counter = 0
        for member in guild.members:
            try:
                await conn.execute(
                    'INSERT INTO statbot_db.USERS VALUES ($1, $2)',
                    member.id,
                    False
                )
            except asyncpg.UniqueViolationError as e:
                self.log.info('FAILED TO ADD USER: Already exists in DB (expected)')
                self.log.debug('{!r}: errno is {}'.format(e, e.args[0]))

            await conn.execute(
                'INSERT INTO statbot_db.HAS_USERS VALUES ($1, $2)',
                guild.id,
                member.id
            )
            counter += 1
        self.log.info('Added {} users from {}'.format(counter, guild.name))

    @commands.command(hidden=True, name='addguild')
    @commands.is_owner()
    async def addguild(self, ctx: discord.ext.commands.Context) -> None:
        async with get_conn(self.bot) as conn:
            # Timing how long this command takes to run
            start = time.time()

            if ctx.guild is None:
                await ctx.send('You\'re going to have to send this message from the server you want added.')
                return

            try:
                await conn.execute(
                    'INSERT INTO statbot_db.SERVERS '
                    'VALUES ($1, $2)',
                    ctx.guild.id,
                    True
                )
            except asyncpg.UniqueViolationError as e:
                self.log.warning('This server already exists in the database')
                self.log.warning(ctx.guild)
                self.log.warning('{!r}: errno is {}'.format(e, e.args[0]))
                return

            # Now add all users in server
            await self._server_add_users(ctx.guild, conn)

            for text_channel in ctx.guild.text_channels:
                await conn.execute(
                    'INSERT INTO statbot_db.CHANNELS (ChannelID, ServerID, Importing) VALUES ($1, $2, $3)',
                    text_channel.id,
                    ctx.guild.id,
                    True
                )

                if text_channel.permissions_for(ctx.guild.me).read_message_history is False:
                    self.log.info('Skipping restricted channel')
                    self.log.debug(ctx.guild)
                    self.log.debug(text_channel)
                    continue

                await self._add_messages(text_channel, conn)

                await conn.execute(
                    'UPDATE statbot_db.CHANNELS SET Importing = $1 '
                    'WHERE ChannelID = $2',
                    False,
                    text_channel.id
                )

            await conn.execute(
                'UPDATE statbot_db.SERVERS SET Importing = $1 '
                'WHERE ServerID = $2',
                False,
                ctx.guild.id
            )
            self.log.info('Server message transfer complete')

            end = time.time()
            self.log.info(str(round((end - start) / 60, 2)) + ' minutes elapsed')

    @commands.command(hidden=True, name='removeguild')
    @commands.is_owner()
    async def removeguild(self, ctx: discord.ext.commands.Context) -> None:
        async with get_conn(self.bot) as conn:
            # Timing how long this command takes to run
            start = time.time()

            if ctx.guild is None:
                await ctx.send('You\'re going to have to send this message from the server you want removed.')
                return

            async with conn.transaction():
                row = await conn.fetchrow(
                    'SELECT ServerID FROM statbot_db.SERVERS '
                    'WHERE ServerID = $1 FOR UPDATE',
                    ctx.guild.id
                )
                if row is None:
                    self.log.warning('Can\'t remove a server that\'s not in the database')
                    self.log.warning(ctx.guild)
                    return

                await conn.execute(
                    'DELETE FROM statbot_db.SERVERS '
                    'WHERE ServerID = $1',
                    ctx.guild.id
                )

            self.log.info('Server removal complete')

            end = time.time()
            self.log.info(str(round((end - start) / 60, 2)) + ' minutes elapsed')

    @commands.command(hidden=True, name='regeneratedb')
    @commands.is_owner()
    async def regeneratedb(self, ctx: discord.ext.commands.Context, *args: str) -> None:
        async with get_conn(self.bot) as conn:
            for guild in self.bot.guilds:
                async with conn.transaction():
                    print('Regenerating {}'.format(guild.name))
                    rows = await conn.fetch(
                        'SELECT ServerID FROM statbot_db.SERVERS '
                        'WHERE ServerID = $1 FOR UPDATE',
                        guild.id
                    )
                    if len(rows) != 1:
                        print('Can\'t remove a server that\'s not in the database')
                        continue

                    await conn.execute(
                        'DELETE FROM statbot_db.SERVERS '
                        'WHERE ServerID = $1',
                        guild.id
                    )
                    print('Removed {}, now re-adding'.format(guild.name))

                    try:
                        await conn.execute(
                            'INSERT INTO statbot_db.SERVERS '
                            'VALUES ($1, $2)',
                            guild.id,
                            True
                        )
                    except asyncpg.UniqueViolationError as e:
                        print('This server already exists in the database')
                        print(e)
                        return

                    # Now add all users in server
                    await self._server_add_users(guild, conn)

                    for text_channel in guild.text_channels:
                        await conn.execute(
                            'INSERT INTO statbot_db.CHANNELS VALUES ($1, $2)',
                            text_channel.id,
                            guild.id
                        )

                        if text_channel.permissions_for(guild.me).read_message_history is False:
                            print('Skipping restricted channel')
                            continue

                        await self._add_messages(text_channel, conn)

                    await conn.execute(
                        'UPDATE statbot_db.SERVERS SET Importing = $1 '
                        'WHERE ServerID = $2',
                        False,
                        guild.id
                    )
                    print('Server message transfer complete')
                    print('{} server regeneration complete'.format(guild.name))

            print('Regeneration complete')


async def setup(bot: StatBot) -> None:
    await bot.add_cog(Synchronization(bot))
