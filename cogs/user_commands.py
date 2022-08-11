import io
import os
import re
import time
from functools import partial
from typing import Optional, TypedDict, Union

import PIL
import aiofiles
import discord
import numpy as np
import requests
import wordcloud as wc
from PIL import Image, ImageEnhance
from discord import Member, TextChannel
from discord.ext import commands
from scipy.ndimage import gaussian_gradient_magnitude
from wordcloud import STOPWORDS, ImageColorGenerator

from cogs import constant
from core import utility
from core.statbot import StatBot
from core.utility import get_conn, Status


class WcParams(TypedDict):
    emojis: bool
    emojis_only: bool
    target: Optional[discord.Member]
    mask: Optional[str]
    url: Optional[str]


def _edge_find(mask_array: np.ndarray) -> np.ndarray:
    mask_array_copy = mask_array.copy()
    mask_array_copy[mask_array.sum(axis=2) == 0] = 255
    edges = np.mean([gaussian_gradient_magnitude(mask_array[:, :, i] / 255., 2) for i in range(3)], axis=0)
    mask_array_copy[edges > .08] = 255

    return mask_array_copy


# noinspection PyTypeChecker
def _generate_cloud(
        stopwords: set[str], word_string: str, collocations: bool, image_name: str = None, image_url: str = None
) -> Optional[io.BytesIO]:
    start_time = time.time()

    if image_name is not None:
        image_path = constant.WC_IMAGES[image_name]['image_path']
        color_image_path = constant.WC_IMAGES[image_name]['color_image_path']
        bg_color = constant.WC_IMAGES[image_name]['bg_color']
        scale = constant.WC_IMAGES[image_name]['scale']
    else:
        image_path = None
        color_image_path = None
        bg_color = None
        scale = constant.WC_SCALE if image_url is None else None

    mask_array = None
    color_mask_array = None
    if image_path:
        mask_image_file = Image.open(os.path.abspath(image_path))
        mask_array = np.array(mask_image_file)
        mask_image_file.close()

        if color_image_path:
            mask_color_image_file = Image.open(os.path.abspath(color_image_path))
            color_mask_array = np.array(mask_color_image_file)
            mask_color_image_file.close()
    elif image_url is not None:
        with Image.open(requests.get(image_url, stream=True).raw) as mask_image_file:
            mask_image_file = mask_image_file.convert('RGBA')
            enhancer = ImageEnhance.Color(mask_image_file)
            mask_image_file = enhancer.enhance(1.5)

            if mask_image_file.size[0] > constant.WC_WIDTH or mask_image_file.size[1] > constant.WC_WIDTH:
                max_size = max(mask_image_file.size[0], mask_image_file.size[1])
                rescale = constant.WC_WIDTH / max_size
                mask_image_file = mask_image_file.resize(
                    (
                        int(mask_image_file.size[0] * rescale),
                        int(mask_image_file.size[1] * rescale)
                    )
                )

            width, height = mask_image_file.size

            mask_array = np.array(mask_image_file)
            has_transparency = (mask_array[:, :, 3] == 0).any()

            with mask_image_file.copy() as color_mask_file:
                for i in range(3):
                    idx = mask_array[:, :, i] <= 255
                    mask_array[idx, i] = 0

                mask_image_file = Image.new('RGBA', (width, height), 'WHITE')
                mask_file = Image.fromarray(mask_array)
                Image.Image.paste(mask_image_file, mask_file, (0, 0), mask_file)

                color_image_file = Image.new('RGBA', (width, height), 'WHITE')
                Image.Image.paste(color_image_file, color_mask_file, (0, 0), color_mask_file)

                if has_transparency:
                    mask_array = np.array(mask_image_file)
                    color_mask_array = np.array(color_image_file)
                else:
                    mask_array = np.array(color_image_file)

    if mask_array is not None:
        if color_mask_array is not None:
            mask_array_copy = mask_array.copy()
            color_mask_array_copy = _edge_find(color_mask_array)
            colors = ImageColorGenerator(color_mask_array_copy)
        else:
            mask_array_copy = _edge_find(mask_array)
            colors = ImageColorGenerator(mask_array_copy)

        wordcloud = wc.WordCloud(
            color_func=colors,
            stopwords=stopwords,
            margin=constant.WC_MARGIN,
            max_words=constant.WC_MAX_WORDS,
            max_font_size=constant.WC_MAX_FONT_SIZE,
            background_color=bg_color,
            mask=mask_array_copy,
            collocations=collocations,
            normalize_plurals=False,
            mode=constant.WC_COLOR_MODE,
            relative_scaling=0,
        ).generate(word_string)
    else:
        wordcloud = wc.WordCloud(
            height=constant.WC_HEIGHT, width=constant.WC_WIDTH,
            color_func=wc.random_color_func,
            stopwords=stopwords,
            margin=constant.WC_MARGIN,
            max_words=constant.WC_MAX_WORDS,
            max_font_size=constant.WC_MAX_FONT_SIZE,
            scale=scale,
            background_color=bg_color,
            collocations=collocations,
            normalize_plurals=False,
        ).generate(word_string)

    output_buffer = io.BytesIO()
    wordcloud.to_image().save(output_buffer, constant.WC_FILE_FORMAT)
    output_buffer.seek(0)

    end_time = time.time()
    print(str(end_time - start_time))

    return output_buffer


# noinspection PyMethodMayBeStatic
class UserCommands(commands.Cog):
    def __init__(self, bot: StatBot):
        self.bot = bot

    async def _handle_server_status_response(self, ctx: discord.ext.commands.Context, server_status: Status) -> None:
        if server_status != Status.AVAILABLE:
            if server_status == Status.IMPORTING:
                await ctx.send(constant.RESPONSES['server_importing'])
            else:
                await ctx.send(constant.RESPONSES['server_not_added'])
            return

    async def _handle_channel_status_response(self, ctx: discord.ext.commands.Context, channel_status: Status) -> None:
        if channel_status != Status.AVAILABLE:
            if channel_status == Status.IMPORTING:
                await ctx.send(constant.RESPONSES['channel_importing'])
            else:
                await ctx.send(constant.RESPONSES['channel_not_added'])
            return

    async def _find_user_from_str(self, ctx: commands.Context, user_str: str) -> discord.Member:
        target = None
        match = re.search(constant.REGEX['user_mention'], user_str)
        if match:
            mention = match.group()
            target = await commands.MemberConverter().convert(ctx, mention)

        return target

    async def _find_channel_from_str(self, ctx: commands.Context, channel_str: str) -> discord.TextChannel:
        channel = None
        match = re.search(constant.REGEX['channel_mention'], channel_str)
        if match:
            mention = match.group()
            channel = await commands.TextChannelConverter().convert(ctx, mention)

        return channel

    async def _process_wordcloud_args(self, ctx: commands.Context, args: tuple[str]) -> Optional[WcParams]:
        params = {
            'emojis': True,
            'emojis_only': False,
            'target': None,
            'mask': None,
            'url': None
        }

        for arg in args:
            match = re.search(constant.REGEX['user_mention'], arg)
            url_match = re.search(constant.REGEX['urls'], arg)
            if arg.lower() == 'noemojis':
                params['emojis'] = False
            elif arg.lower() == 'emojisonly':
                params['emojis_only'] = True
            elif arg.lower() in constant.WC_MASK_ARGS:
                params['mask'] = arg.lower()
            elif match and not params['target']:
                mention = match.group()
                try:
                    params['target'] = await commands.MemberConverter().convert(ctx, mention)
                except commands.errors.BadArgument:
                    await ctx.send('Sorry, I can\'t seem to find that user.')
                    return None
            elif url_match:
                params['url'] = url_match.group()

        if params['emojis'] is False and params['emojis_only'] is True:
            await ctx.send('You can\'t pick both the `noemojis` and the `emojisonly` filter in the same command!')
            return None

        return params

    async def _get_stopwords(self) -> set[str]:
        stopwords = set(STOPWORDS)
        async with aiofiles.open('datasets/stopwords.txt', 'r') as words_file:
            add_words = (await words_file.read()).split('\n')

        stopwords.update(add_words)

        return stopwords

    async def _pre_filter_wc_string(
            self, ctx: commands.Context, msg_list: list[str], params: WcParams) -> Optional[str]:
        # command filtering
        async with aiofiles.open('wordcloud/prefixes.txt', 'r') as prefixes:
            pref_str = '|'.join((await prefixes.read()).split('\n'))

        reg_ex = '^(' + pref_str + ').*?( |$)'
        string_list = list()
        for row in msg_list:
            if not re.search(reg_ex, row[0]):
                string_list.append(row[0])

        joined_msgs = ' '.join(string_list)

        # emoji filtering
        if not params['emojis']:
            joined_msgs = re.sub(constant.REGEX['emojis'], '', joined_msgs)

        if params['emojis_only']:
            match = re.findall(constant.REGEX['emoji_names'], joined_msgs)
            if match:
                joined_msgs = ' '.join(match)
            else:
                await ctx.send('No emojis found in your message history.')
                return None
        else:
            # link filtering
            joined_msgs = re.sub(constant.REGEX['urls'], '', joined_msgs)

        return joined_msgs

    async def _assign_args(
            self,
            ctx: discord.ext.commands.Context,
            arg1: Optional[Union[discord.Member, discord.TextChannel]],
            arg2: Optional[Union[discord.Member, discord.TextChannel]]
    ) -> tuple[Union[Member, TextChannel], Union[TextChannel, Member, None]]:
        user_target = ctx.author
        channel_target = None
        if arg1:
            if isinstance(arg1, discord.Member):
                # arg1 is a Member obj and arg2 is (potentially) a TextChannel obj
                user_target = arg1
                if arg2:
                    channel_target = arg2
            else:
                # arg1 is a TextChannel obj and arg2 (potentially) is a Member obj
                channel_target = arg1
                if arg2:
                    user_target = arg2

        return user_target, channel_target

    @commands.command(name='wordcloud')
    async def wordcloud(self, ctx: commands.Context, *args: str) -> None:
        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                server_status = await utility.server_status(conn, ctx.guild)
                await self._handle_server_status_response(ctx, server_status)
                if server_status != Status.AVAILABLE:
                    return

                params = await self._process_wordcloud_args(ctx, args)
                if params is None:
                    return

                uploaded = None
                if len(ctx.message.attachments) > 0:
                    uploaded = ctx.message.attachments[0].url
                elif params['url']:
                    uploaded = params['url']

                try:
                    if ctx.author.nick:
                        bot_response = await ctx.send('I\'ll ping you when it\'s ready {}!'.format(ctx.author.nick))
                    else:
                        bot_response = await ctx.send('I\'ll ping you when it\'s ready {}!'.format(ctx.author.name))

                    if not params['target']:
                        params['target'] = ctx.author

                    msg_list = await conn.fetch(
                        'SELECT M.Content '
                        'FROM statbot_db.SERVERS AS S, statbot_db.MESSAGES AS M '
                        'WHERE S.ServerID = $1 AND M.AuthorID = $2 AND S.ServerID = M.ServerID',
                        ctx.guild.id,
                        params['target'].id
                    )

                    filtered_msgs = await self._pre_filter_wc_string(ctx, msg_list, params)
                    if filtered_msgs is None:
                        return

                    collocations = False if params['emojis_only'] else constant.WC_COLLOCATIONS

                    stopwords = await self._get_stopwords()

                    func = partial(
                        _generate_cloud,
                        stopwords,
                        filtered_msgs,
                        image_name=params['mask'] if not uploaded else None,
                        collocations=collocations,
                        image_url=uploaded
                    )

                    try:
                        final_image = await self.bot.loop.run_in_executor(self.bot.process_executor, func)
                    except ValueError as e:
                        print('ValueError: {}'.format(e))
                        await ctx.send(
                            'This user does not have enough interesting words to generate a wordcloud. Sorry!',
                            delete_after=5
                        )
                        return
                    except PIL.UnidentifiedImageError as e:
                        print('PIL.UnidentifiedImageError: {}'.format(e))
                        await ctx.send('Invalid image file type.', delete_after=5)
                        return

                    file = discord.File(filename='wordcloud.png', fp=final_image)

                    if params['target'] == ctx.author:
                        await ctx.send('Here you go {} ^_^'.format(ctx.author.mention), file=file)
                    elif params['target'].nick:
                        await ctx.send(
                            'Here is {}\'s word cloud, {} ^_^'.format(params['target'].nick, ctx.author.mention),
                            file=file
                        )
                    else:
                        await ctx.send(
                            'Here is {}\'s word cloud, {} ^_^'.format(params['target'].name, ctx.author.mention),
                            file=file
                        )
                finally:
                    await bot_response.delete()

    @commands.command(name='msgcount')
    async def msgcount(
            self,
            ctx: commands.Context,
            arg1: Optional[Union[discord.Member, discord.TextChannel]],
            arg2: Optional[Union[discord.Member, discord.TextChannel]]
    ) -> None:
        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                server_status = await utility.server_status(conn, ctx.guild)
                await self._handle_server_status_response(ctx, server_status)
                if server_status != Status.AVAILABLE:
                    return

                (target, channel) = await self._assign_args(ctx, arg1, arg2)
                if target is None:
                    return

                # No channel was specified by the user
                if not channel:
                    msg_count = await conn.fetchrow(
                        'SELECT COUNT(*) '
                        'FROM statbot_db.SERVERS AS S, statbot_db.MESSAGES AS M '
                        'WHERE S.ServerID = $1 AND M.AuthorID = $2 AND S.ServerID = M.ServerID',
                        ctx.guild.id,
                        target.id
                    )

                    total = await conn.fetchrow(
                        'SELECT COUNT(*) '
                        'FROM statbot_db.SERVERS AS S, statbot_db.MESSAGES AS M '
                        'WHERE S.ServerID = M.ServerID AND S.ServerID = $1',
                        ctx.guild.id
                    )
                else:
                    channel_status = await utility.channel_status(conn, ctx.channel)
                    await self._handle_channel_status_response(ctx, channel_status)
                    if channel_status != Status.AVAILABLE:
                        return

                    msg_count = await conn.fetchrow(
                        'SELECT COUNT(*) '
                        'FROM statbot_db.CHANNELS C, statbot_db.MESSAGES AS M '
                        'WHERE C.ChannelID = $1 AND C.ChannelID = M.ChannelID '
                        'AND M.AuthorID = $2',
                        channel.id,
                        target.id
                    )

                    total = await conn.fetchrow(
                        'SELECT COUNT(*) '
                        'FROM statbot_db.MESSAGES '
                        'WHERE ChannelID = $1',
                        channel.id
                    )

        percent = round((msg_count['count'] / total['count']) * 100, 2)

        if not channel:
            await ctx.send(
                '**{}** has contributed **{}** messages out of the total **{}** messages sent in the **{}** server. '
                'That\'s **{}%**! Woah!'
                ''.format(target.name, msg_count['count'], total['count'], ctx.guild.name, percent)
            )
        else:
            await ctx.send(
                '**{}** has contributed **{}** messages out of the total **{}** messages sent in the **{}** channel. '
                'That\'s **{}%**! Woah!'
                ''.format(target.name, msg_count['count'], total['count'], channel.name, percent)
            )


async def setup(bot: StatBot) -> None:
    await bot.add_cog(UserCommands(bot))
