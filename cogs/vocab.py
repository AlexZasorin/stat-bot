import inspect
import re
import string
import textwrap
from functools import partial
from typing import Optional, Union

import asyncpg
import discord
import nltk
import numpy as np
import pandas as pd
import textstat
from discord import Member, TextChannel
from discord.ext import commands
from nltk import WordNetLemmatizer
from num2words import num2words

from cogs import constant
from core import utility
from core.statbot import StatBot
from core.utility import get_conn, STATUS


# noinspection PyMethodMayBeStatic
class Vocab(commands.Cog):
    def __init__(self, bot: StatBot):
        self.bot = bot
        self.words = None
        self.nltk_words = None
        self.ud_df = None
        self.most_common_words = None
        self.wnl = None

    def vocab_data_init(self):
        self.wnl = WordNetLemmatizer()

        print('NLTK: Downloading \"words\" dataset...')
        nltk.download('words', quiet=True)
        print('NLTK: Downloading \"names\" dataset...')
        nltk.download('names', quiet=True)
        print('NLTK: Done')

        ud = pd.read_csv('datasets/urbandict-word-defs.csv', on_bad_lines='skip')

        stopwords = pd.DataFrame(columns=['word'], data=open('datasets/stopwords.txt', 'r').read().split('\n'))
        common_words = pd.DataFrame(columns=['word'], data=open('datasets/1000-most-common-words.txt', 'r').read().split('\n'))
        self.most_common_words = set(pd.concat([common_words, stopwords], axis=0).word.drop_duplicates().tolist())
        self.most_common_words.update([n.lower() for n in nltk.corpus.names.words()])

        # Urban Dictionary DF Setup
        self.ud_df = ud[['word', 'up_votes', 'down_votes']]
        sum_col = self.ud_df.loc[:, ['up_votes', 'down_votes']].sum(axis=1)
        self.ud_df = self.ud_df.assign(engagement=sum_col)

        self.ud_df.word.replace(to_replace=[r'^\s+$', ''], value=np.nan, regex=True, inplace=True)
        self.ud_df.dropna(inplace=True)
        self.ud_df.word = self.ud_df.word.str.lower()

        self.ud_df = self.ud_df.groupby('word', as_index=False).agg(
            {
                'up_votes': 'sum',
                'down_votes': 'sum',
                'engagement': 'sum'
            }
        )

        # Word Lists
        slang_words = set(self.ud_df.word.tolist())
        self.nltk_words = set(nltk.corpus.words.words())
        self.words = set(
            pd.concat(
                [
                    pd.DataFrame(columns=['word'], data=slang_words),
                    pd.DataFrame(columns=['word'], data=self.nltk_words)
                ],
                axis=0
            ).word.drop_duplicates().tolist()
        )

    async def cog_load(self) -> None:
        await self.bot.loop.run_in_executor(None, self.vocab_data_init)

    def _clean_content(self, msg_str):
        lower_case = msg_str.lower()
        remove_urls = constant.REGEX['urls'].sub('', lower_case)
        remove_user = constant.REGEX['user_mention'].sub('', remove_urls)
        remove_channel = constant.REGEX['channel_mention'].sub('', remove_user)
        remove_emojis = constant.REGEX['emoji_names'].sub('', remove_channel)

        remove_punctuation = remove_emojis.translate(str.maketrans('', '', string.punctuation))

        return remove_punctuation

    def _unique_words(self, msg_list):
        msg_set = set(msg_list)
        msg_set = {w for w in msg_set if w in self.words}

        msg_set = {self.wnl.lemmatize(w) if w in self.nltk_words else w for w in msg_set}

        return ' '.join(msg_set)

    def _filter_common_words(self, msg_list):
        return ' '.join([w for w in msg_list if w not in self.most_common_words])

    def _unique_words_per_user(self, dfr, author, less_common_words):
        complement_set = set(dfr.less_common_words.loc[dfr.authorid != author].str.split().explode())
        author_set = set(less_common_words.split())

        result_set = author_set.difference(complement_set)

        return ' '.join(result_set)

    def _clean_leave_punctuation(self, msg_str):
        remove_urls = constant.REGEX['urls'].sub('', msg_str)
        remove_user = constant.REGEX['user_mention'].sub('', remove_urls)
        remove_channel = constant.REGEX['channel_mention'].sub('', remove_user)
        remove_emojis = constant.REGEX['emoji_names'].sub('', remove_channel)

        remove_emojis = remove_emojis.strip()

        if re.match(r'^\s*$', remove_emojis) or (len(remove_emojis) == 1 and remove_emojis[-1] in ['!', '.', '?']):
            return np.nan
        else:
            remove_emojis = remove_emojis.capitalize()

            if len(remove_emojis) != 0:
                if remove_emojis[-1] not in ['!', '.', '?']:
                    remove_emojis = remove_emojis + '.'

            return remove_emojis

    async def _get_agg_msgs(
            self,
            conn: asyncpg.Connection,
            guild_id: int,
            user_id: Optional[int] = None,
            channel_id: Optional[int] = None
    ) -> pd.DataFrame:
        if channel_id:
            if user_id:
                df = await utility.fetch_as_dataframe(
                    conn,
                    "SELECT M.authorid, STRING_AGG(M.content, ' ') as msgs "
                    "FROM statbot_db.MESSAGES as M "
                    "WHERE M.serverid = $1 AND M.channelid = $2 AND M.authorid = $3 "
                    "GROUP BY M.authorid",
                    guild_id,
                    channel_id,
                    user_id
                )
            else:
                df = await utility.fetch_as_dataframe(
                    conn,
                    "SELECT M.authorid, STRING_AGG(M.content, ' ') as msgs "
                    "FROM statbot_db.MESSAGES as M "
                    "WHERE M.serverid = $1 AND M.channelid = $2 "
                    "GROUP BY M.authorid",
                    guild_id,
                    channel_id
                )
        else:
            if user_id:
                df = await utility.fetch_as_dataframe(
                    conn,
                    "SELECT M.authorid, STRING_AGG(M.content, ' ') as msgs "
                    "FROM statbot_db.MESSAGES as M "
                    "WHERE M.serverid = $1 AND M.authorid = $2 "
                    "GROUP BY M.authorid",
                    guild_id,
                    user_id
                )
            else:
                df = await utility.fetch_as_dataframe(
                    conn,
                    "SELECT M.authorid, STRING_AGG(M.content, ' ') as msgs "
                    "FROM statbot_db.MESSAGES as M "
                    "WHERE M.serverid = $1 "
                    "GROUP BY M.authorid",
                    guild_id
                )

        df.msgs.replace(to_replace=[r'^\s+$', ''], value=np.nan, regex=True, inplace=True)
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df

    async def _get_agg_msgs_grade(
            self,
            conn: asyncpg.Connection,
            guild_id: int,
            user_id: Optional[int] = None,
            channel_id: Optional[int] = None
    ) -> pd.DataFrame:
        if channel_id:
            if user_id:
                df_grade = await utility.fetch_as_dataframe(
                    conn,
                    "SELECT M.authorid, M.content as msgs, M.sent "
                    "FROM statbot_db.MESSAGES as M "
                    "WHERE M.serverid = $1 AND M.authorid = $2 AND M.channelid = $3 "
                    "AND M.sent BETWEEN NOW() - INTERVAL '1 MONTH' AND NOW() "
                    "ORDER BY M.sent",
                    guild_id,
                    user_id,
                    channel_id
                )
            else:
                df_grade = await utility.fetch_as_dataframe(
                    conn,
                    "SELECT M.authorid, M.content as msgs, M.sent "
                    "FROM statbot_db.MESSAGES as M "
                    "WHERE M.serverid = $1 AND M.channelid = $2 "
                    "AND M.sent BETWEEN NOW() - INTERVAL '1 MONTH' AND NOW() "
                    "ORDER BY M.sent",
                    guild_id,
                    channel_id
                )
        else:
            if user_id:
                df_grade = await utility.fetch_as_dataframe(
                    conn,
                    "SELECT M.authorid, M.content as msgs, M.sent "
                    "FROM statbot_db.MESSAGES as M "
                    "WHERE M.serverid = $1 AND M.authorid = $2 AND M.sent BETWEEN NOW() - INTERVAL '1 MONTH' AND NOW() "
                    "ORDER BY M.sent",
                    guild_id,
                    user_id,
                )
            else:
                df_grade = await utility.fetch_as_dataframe(
                    conn,
                    "SELECT M.authorid, M.content as msgs, M.sent "
                    "FROM statbot_db.MESSAGES as M "
                    "WHERE M.serverid = $1 AND M.sent BETWEEN NOW() - INTERVAL '1 MONTH' AND NOW() "
                    "ORDER BY M.sent",
                    guild_id
                )

        df_grade.msgs.replace(to_replace=[r'^\s+$', ''], value=np.nan, regex=True, inplace=True)
        df_grade.dropna(inplace=True)
        df_grade.reset_index(drop=True, inplace=True)

        return df_grade

    def _summary(
            self, data: pd.DataFrame, grade_data: pd.DataFrame, server: bool = False
    ) -> list[pd.DataFrame, pd.DataFrame, Optional[pd.DataFrame], float]:
        ret = list()

        vocab_ranking = self._ranking(data)

        ret.append(vocab_ranking.loc[:, ['authorid', 'unique_counts']])

        vocab_unique = self._unique(vocab_ranking)

        ret.append(vocab_unique.loc[:, ['authorid', 'word', 'counts', 'interesting_metric']])

        # Most interesting urban dictionary ones
        if server:
            int_df = self._interesting(vocab_ranking)

            ret.append(int_df.loc[:, ['word', 'engagement', 'counts', 'interesting_metric']])
        else:
            ret.append(None)

        # Overall grade level for server
        server_grade_level = self._grade(grade_data)

        ret.append(server_grade_level)

        return ret

    async def _handle_server_status_response(self, ctx: discord.ext.commands.Context, server_status: STATUS) -> None:
        if server_status != STATUS.AVAILABLE:
            if server_status == STATUS.IMPORTING:
                await ctx.send(constant.RESPONSES['server_importing'])
            else:
                await ctx.send(constant.RESPONSES['server_not_added'])
            return

    async def _handle_channel_status_response(self, ctx: discord.ext.commands.Context, channel_status: STATUS) -> None:
        if channel_status != STATUS.AVAILABLE:
            if channel_status == STATUS.IMPORTING:
                await ctx.send(constant.RESPONSES['channel_importing'])
            else:
                await ctx.send(constant.RESPONSES['channel_not_added'])
            return

    async def _add_ranking_field(
            self,
            summary: discord.Embed,
            ranking_result: pd.DataFrame,
            channel_target: discord.TextChannel,
            user_target: Optional[discord.Member] = None
    ) -> None:
        if user_target:
            user_idx = ranking_result.loc[ranking_result.authorid == user_target.id].index[0]
            ranking_result = ranking_result.iloc[max(user_idx - 2, 0):min(user_idx + 3, len(ranking_result.index) - 1)]
        else:
            user_idx = -1
            ranking_result = ranking_result.head(5)

        ranking_str = '```py\n'
        tabs = '  '
        for idx, row in ranking_result.iterrows():
            user_id = row['authorid']
            count = row['unique_counts']
            try:
                target = self.bot.get_user(user_id)
            except commands.errors.BadArgument:
                target = None

            if target is None:
                # noinspection PyTypeChecker
                ranking_str += '[{}] #{}\n{}-> {} unique words\n\n'.format((idx + 1), user_id, tabs, count)
            elif idx == user_idx:
                # noinspection PyTypeChecker
                ranking_str += '@ [{}] {}\n{}-> {} unique words\n\n'.format((idx + 1), target.name, tabs, count)
            else:
                # noinspection PyTypeChecker
                ranking_str += '[{}] #{}\n{}-> {} unique words\n\n'.format((idx + 1), target.name, tabs, count)
        ranking_str += '```'

        if user_target:
            ranking_str += '\nYou are ranked ' + num2words(user_idx + 1, to='ordinal_num') + ' in the server!'
            if channel_target:
                ranking_str += ' (#{})'.format(channel_target.name)

        field_title_str = 'Unique Words Ranking'
        if channel_target:
            field_title_str += ' (#{})'.format(channel_target.name)
        summary.add_field(name=field_title_str, value=ranking_str, inline=True)

    def _select_random_top(self, data: pd.DataFrame) -> pd.DataFrame:
        copy = data.copy()
        choice_size = max(int(len(copy.index) * 0.03), 5)
        sample_size = len(copy.index) if len(copy.index) < 5 else 5
        copy = copy.head(choice_size).sample(sample_size)
        copy.sort_values(by='counts', ascending=False, inplace=True)

        return copy

    async def _add_unique_field(
            self,
            summary: discord.Embed,
            unique_result: pd.DataFrame,
            user_target: Optional[discord.Member] = None
    ) -> None:
        if user_target:
            unique_result = unique_result.loc[unique_result.authorid == user_target.id]
            unique_result.reset_index(drop=True, inplace=True)

        unique_result = self._select_random_top(unique_result)

        unique_str = '```py\n'
        tabs = '  '
        for idx, row in unique_result.iterrows():
            user_id = row['authorid']
            word = row['word']
            times = row['counts']

            if user_target:
                unique_str += '[*] {}\n{}-> Said {} times\n\n'.format(word, tabs, times)
            else:
                try:
                    target = self.bot.get_user(user_id)
                except commands.errors.BadArgument:
                    target = None

                if target:
                    tmp = '[*] {}\n{}-> Said by "{}" {} times'.format(word, tabs, target.name, times)
                    unique_str += textwrap.fill(
                        tmp, width=32, subsequent_indent='     ', replace_whitespace=False,
                    ) + '\n\n'
                else:
                    tmp = '[*] {}\n{}-> Said by "{}" {} times'.format(word, tabs, user_id, times)
                    unique_str += textwrap.fill(
                        tmp, width=32, subsequent_indent='     ', replace_whitespace=False,
                    ) + '\n\n'

        unique_str += '```'

        field_title_str = 'Interesting Words Unique to '
        if user_target:
            field_title_str += '{}'.format(user_target.name)
        else:
            field_title_str += 'a User'
        summary.add_field(name=field_title_str, value=unique_str, inline=True)

    async def _check_vocab_args(
            self,
            arg1: Optional[Union[discord.Member, discord.TextChannel]],
            arg2: Optional[Union[discord.Member, discord.TextChannel]]
    ) -> None:
        if ((isinstance(arg1, discord.Member) and isinstance(arg2, discord.Member)) or
                (isinstance(arg1, discord.TextChannel) and isinstance(arg2, discord.TextChannel))):
            raise ValueError('You can only pass in up to one user and one channel to this command.')

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

    def _word_freq_from_series(self, msgs: pd.Series) -> pd.DataFrame:
        word_freq = msgs.str.split().explode().value_counts().rename_axis(['word']).reset_index(name='counts')
        word_freq.word = word_freq.word.apply(lambda x: self.wnl.lemmatize(x) if x in self.nltk_words else x)
        word_freq = word_freq.groupby('word', as_index=False).agg({'counts': 'sum'})

        return word_freq

    @commands.group(invoke_without_command=True)
    async def vocab(
            self,
            ctx: commands.Context,
            arg1: Optional[Union[discord.Member, discord.TextChannel]],
            arg2: Optional[Union[discord.Member, discord.TextChannel]]
    ) -> None:
        await self._check_vocab_args(arg1, arg2)

        (user_target, channel_target) = await self._assign_args(ctx, arg1, arg2)

        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                server_status = await utility.server_status(conn, ctx.guild)
                await self._handle_server_status_response(ctx, server_status)
                if server_status != STATUS.AVAILABLE:
                    return

                if channel_target:
                    channel_status = await utility.channel_status(conn, ctx.channel)
                    await self._handle_channel_status_response(ctx, channel_status)
                    if channel_status != STATUS.AVAILABLE:
                        return

                    df = await self._get_agg_msgs(conn, ctx.guild.id, channel_id=channel_target.id)
                    df_grade = await self._get_agg_msgs_grade(
                        conn, ctx.guild.id, user_id=user_target.id, channel_id=channel_target.id
                    )
                else:
                    df = await self._get_agg_msgs(conn, ctx.guild.id)
                    df_grade = await self._get_agg_msgs_grade(conn, ctx.guild.id, user_id=user_target.id)

        if user_target.id not in df.authorid.to_list() or user_target.id not in df_grade.authorid.to_list():
            target_name = '#' + channel_target.name if channel_target else '**' + ctx.guild.name + '**'
            await ctx.send('It looks like you don\'t have any messages sent in {}.'.format(target_name))
            return

        [ranking_result, unique_result, _, grade] \
            = await self.bot.loop.run_in_executor(None, partial(self._summary, df, df_grade))

        if (user_target.id not in ranking_result.authorid.to_list()
                or user_target.id not in unique_result.authorid.to_list()):
            await ctx.send('It looks like you don\'t have enough words said to generate a report.')
            return

        title = user_target.name + '\'s Vocab Report'
        if channel_target:
            title += ' (#{})'.format(channel_target.name)
        summary = discord.Embed(colour=discord.Colour(0xff6600), title=title)

        await self._add_ranking_field(summary, ranking_result, channel_target, user_target)

        await self._add_unique_field(summary, unique_result, user_target)

        await self._add_grade_field(summary, grade)

        await ctx.send(embed=summary)

    def _unique_words_col(self, data: pd.DataFrame) -> pd.DataFrame:
        vocab_ranking = data.copy()

        vocab_ranking.msgs = vocab_ranking.msgs.apply(self._clean_content)
        vocab_ranking.msgs.replace(to_replace=[r'^\s+$', ''], value=np.nan, regex=True, inplace=True)
        vocab_ranking.dropna(inplace=True)
        vocab_ranking.reset_index(drop=True, inplace=True)

        vocab_ranking['unique_words'] = vocab_ranking.msgs.str.split().apply(self._unique_words)

        vocab_ranking['less_common_words'] = vocab_ranking.unique_words.str.split().apply(self._filter_common_words)
        vocab_ranking.less_common_words.replace(to_replace=[r'^\s+$', ''], value=np.nan, regex=True, inplace=True)
        vocab_ranking.dropna(inplace=True)
        vocab_ranking.reset_index(drop=True, inplace=True)

        return vocab_ranking

    def _ranking(self, data: pd.DataFrame) -> pd.DataFrame:
        vocab_ranking = self._unique_words_col(data)

        vocab_ranking['unique_counts'] = vocab_ranking.unique_words.str.split().apply(lambda x: len(x))
        vocab_ranking.sort_values(by='unique_counts', ascending=False, inplace=True)
        vocab_ranking.reset_index(drop=True, inplace=True)

        return vocab_ranking

    @vocab.command(name='ranking')
    async def vocab_ranking(
            self,
            ctx: commands.Context,
            arg1: Optional[Union[discord.Member, discord.TextChannel]],
            arg2: Optional[Union[discord.Member, discord.TextChannel]]
    ):
        await self._check_vocab_args(arg1, arg2)

        (user_target, channel_target) = await self._assign_args(ctx, arg1, arg2)

        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                server_status = await utility.server_status(conn, ctx.guild)
                await self._handle_server_status_response(ctx, server_status)
                if server_status != STATUS.AVAILABLE:
                    return

                if channel_target:
                    channel_status = await utility.channel_status(conn, ctx.channel)
                    await self._handle_channel_status_response(ctx, channel_status)
                    if channel_status != STATUS.AVAILABLE:
                        return

                    df = await self._get_agg_msgs(conn, ctx.guild.id, channel_id=channel_target.id)
                else:
                    df = await self._get_agg_msgs(conn, ctx.guild.id)

        if user_target.id not in df.authorid.to_list():
            target_name = '#' + channel_target.name if channel_target else '**' + ctx.guild.name + '**'
            await ctx.send('It looks like you don\'t have any messages sent in {}.'.format(target_name))
            return

        ranking_result = await self.bot.loop.run_in_executor(None, partial(self._ranking, df))
        ranking_result = ranking_result.loc[:, ['authorid', 'unique_counts']]

        if user_target.id not in ranking_result.authorid.to_list():
            await ctx.send('It looks like you don\'t have enough words said to find your ranking.')
            return

        title = user_target.name + '\'s Ranking Report'
        if channel_target:
            title += ' (#{})'.format(channel_target.name)
        summary = discord.Embed(colour=discord.Colour(0xff6600), title=title)

        await self._add_ranking_field(summary, ranking_result, channel_target, user_target)

        await ctx.send(embed=summary)

    def _unique(self, data: pd.DataFrame) -> pd.DataFrame:
        vocab_ranking = self._unique_words_col(data)

        vocab_ranking['unique_to_user'] = vocab_ranking.apply(
            lambda x: self._unique_words_per_user(vocab_ranking, x.authorid, x.less_common_words), axis=1)
        vocab_ranking.unique_to_user.replace(to_replace=[r'^\s+$', ''], value=np.nan, regex=True, inplace=True)
        vocab_ranking.dropna(inplace=True)
        vocab_ranking.reset_index(drop=True, inplace=True)

        vocab_unique = vocab_ranking.copy().drop(columns=['msgs'])

        vocab_unique = vocab_unique.assign(word=vocab_unique.unique_to_user.str.split()).explode('word')
        vocab_unique = pd.merge(vocab_unique, self.ud_df, on=['word'])
        vocab_unique.drop(columns=['less_common_words', 'unique_to_user'], inplace=True)

        word_freq = self._word_freq_from_series(vocab_ranking.loc[:, 'msgs'])

        vocab_unique = pd.merge(vocab_unique, word_freq, how='left', on=['word'])

        vocab_unique = self._calculate_interesting_metric(vocab_unique)

        return vocab_unique

    @vocab.command(name='unique')
    async def vocab_unique(
            self,
            ctx: commands.Context,
            arg1: Optional[Union[discord.Member, discord.TextChannel]],
            arg2: Optional[Union[discord.Member, discord.TextChannel]]
    ):
        await self._check_vocab_args(arg1, arg2)

        (user_target, channel_target) = await self._assign_args(ctx, arg1, arg2)

        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                server_status = await utility.server_status(conn, ctx.guild)
                await self._handle_server_status_response(ctx, server_status)
                if server_status != STATUS.AVAILABLE:
                    return

                if channel_target:
                    channel_status = await utility.channel_status(conn, ctx.channel)
                    await self._handle_channel_status_response(ctx, channel_status)
                    if channel_status != STATUS.AVAILABLE:
                        return

                    df = await self._get_agg_msgs(conn, ctx.guild.id, channel_id=channel_target.id)
                else:
                    df = await self._get_agg_msgs(conn, ctx.guild.id)

        if user_target.id not in df.authorid.to_list():
            target_name = '#' + channel_target.name if channel_target else '**' + ctx.guild.name + '**'
            await ctx.send('It looks like you don\'t have any messages sent in {}.'.format(target_name))
            return

        unique_result = await self.bot.loop.run_in_executor(None, partial(self._unique, df))
        unique_result = unique_result.loc[:, ['authorid', 'word', 'counts', 'interesting_metric']]

        if user_target.id not in unique_result.authorid.to_list():
            await ctx.send('It looks like you don\'t have enough words said to find any unique words.')
            return

        title = user_target.name + '\'s Unique Words Report'
        if channel_target:
            title += ' (#{})'.format(channel_target.name)
        summary = discord.Embed(colour=discord.Colour(0xff6600), title=title)

        await self._add_unique_field(summary, unique_result, user_target)

        await ctx.send(embed=summary)

    def _calculate_interesting_metric(self, data: pd.DataFrame) -> pd.DataFrame:
        int_df = data.copy()
        int_df['normalized_engagement'] = int_df.engagement / int_df.engagement.abs().max()
        int_df['normalized_count'] = int_df.counts / int_df.counts.abs().max()
        int_df['interesting_metric'] = int_df.apply(
            lambda x: x.normalized_engagement * x.normalized_count, axis=1
        )
        int_df.sort_values(by='interesting_metric', ascending=False, inplace=True)
        int_df.drop(columns=['normalized_engagement', 'normalized_count'], inplace=True)
        int_df.reset_index(drop=True, inplace=True)

        return int_df

    def _interesting(self, data: pd.DataFrame):
        vocab_ranking = self._unique_words_col(data)

        server_words_df = pd.DataFrame(columns=['word'],
                                       data=set(vocab_ranking.less_common_words.str.split().explode()))
        word_freq = self._word_freq_from_series(vocab_ranking.loc[:, 'msgs'])
        int_df = pd.merge(server_words_df, word_freq, on='word')
        int_df = pd.merge(int_df, self.ud_df, on='word')

        int_df = self._calculate_interesting_metric(int_df)

        return int_df

    async def _add_interesting_field(
            self,
            summary: discord.Embed,
            interesting_result: pd.DataFrame,
            user_target: Optional[discord.Member] = None,
            inline: bool = True,
    ) -> None:
        interesting_result = self._select_random_top(interesting_result)

        interesting_str = '```py\n'
        tabs = '  '
        for idx, row in interesting_result.iterrows():
            word = row['word']
            times = row['counts']

            interesting_str += '[*] {}\n{}-> Said {} times\n\n'.format(word, tabs, times)
        interesting_str += '```'

        field_name_str = 'Interesting Words Said '
        if user_target:
            field_name_str += 'by {}'.format(user_target.name)
        else:
            field_name_str += 'in Server'

        summary.add_field(
            name=field_name_str, value=interesting_str, inline=inline
        )

    @vocab.command(name='interesting')
    async def vocab_interesting(
            self,
            ctx: commands.Context,
            arg1: Optional[Union[discord.Member, discord.TextChannel]],
            arg2: Optional[Union[discord.Member, discord.TextChannel]]
    ):
        await self._check_vocab_args(arg1, arg2)

        (user_target, channel_target) = await self._assign_args(ctx, arg1, arg2)

        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                server_status = await utility.server_status(conn, ctx.guild)
                await self._handle_server_status_response(ctx, server_status)
                if server_status != STATUS.AVAILABLE:
                    return

                if channel_target:
                    channel_status = await utility.channel_status(conn, ctx.channel)
                    await self._handle_channel_status_response(ctx, channel_status)
                    if channel_status != STATUS.AVAILABLE:
                        return

                    df = await self._get_agg_msgs(conn, ctx.guild.id, channel_id=channel_target.id, user_id=user_target.id)
                else:
                    df = await self._get_agg_msgs(conn, ctx.guild.id, user_id=user_target.id)

        if len(df.index) == 0:
            target_name = '#' + channel_target.name if channel_target else '**' + ctx.guild.name + '**'
            await ctx.send('It looks like you don\'t have any messages sent in {}.'.format(target_name))
            return

        interesting_result = await self.bot.loop.run_in_executor(None, partial(self._interesting, df))
        interesting_result = interesting_result.loc[:, ['word', 'engagement', 'counts', 'interesting_metric']]

        if len(interesting_result.index) == 0:
            await ctx.send('It looks like you don\'t have enough words said to find any interesting words.')
            return

        title = user_target.name + '\'s Interesting Words Report'
        if channel_target:
            title += ' (#{})'.format(channel_target.name)
        summary = discord.Embed(colour=discord.Colour(0xff6600), title=title)

        await self._add_interesting_field(summary, interesting_result, user_target)

        await ctx.send(embed=summary)

    def _grade(self, data: pd.DataFrame) -> float:
        server_messages_grade = data.copy()

        server_messages_grade.msgs = server_messages_grade.msgs.apply(self._clean_leave_punctuation)
        server_messages_grade.dropna(inplace=True)
        server_messages_grade.reset_index(drop=True, inplace=True)

        server_messages_str = ' '.join(server_messages_grade.msgs)
        server_grade_level = textstat.textstat.text_standard(server_messages_str, float_output=True)

        return server_grade_level

    async def _add_grade_field(self, summary: discord.Embed, grade: float, server: bool = False):
        grade_ordinal_num = num2words(int(grade), to='ordinal_num')
        grade_ordinal = num2words(int(grade), to='ordinal')
        a_or_an = 'a' if grade_ordinal[0] not in ('a', 'e', 'i', 'o', 'u') else 'an'
        if server:
            grade_str = 'The server '
        else:
            grade_str = 'Your '
        grade_str += 'messages are readable by {} ***{} grader!***'.format(a_or_an, grade_ordinal_num)
        summary.add_field(name='Message Readability', value=grade_str, inline=False)
        summary.set_footer(text='(readability score is based on messages sent in the past month)')

    @vocab.command(name='grade')
    async def vocab_grade(
            self,
            ctx: commands.Context,
            arg1: Optional[Union[discord.Member, discord.TextChannel]],
            arg2: Optional[Union[discord.Member, discord.TextChannel]]
    ):
        await self._check_vocab_args(arg1, arg2)

        (user_target, channel_target) = await self._assign_args(ctx, arg1, arg2)

        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                server_status = await utility.server_status(conn, ctx.guild)
                await self._handle_server_status_response(ctx, server_status)
                if server_status != STATUS.AVAILABLE:
                    return

                if channel_target:
                    channel_status = await utility.channel_status(conn, ctx.channel)
                    await self._handle_channel_status_response(ctx, channel_status)
                    if channel_status != STATUS.AVAILABLE:
                        return

                    df_grade = await self._get_agg_msgs_grade(
                        conn, ctx.guild.id, user_id=user_target.id, channel_id=channel_target.id
                    )
                else:
                    df_grade = await self._get_agg_msgs_grade(conn, ctx.guild.id, user_id=user_target.id)

        if len(df_grade.index) == 0:
            target_name = '#' + channel_target.name if channel_target else '**' + ctx.guild.name + '**'
            await ctx.send('It looks like you don\'t have any messages sent in {}.'.format(target_name))
            return

        grade = await self.bot.loop.run_in_executor(None, partial(self._grade, df_grade))

        title = user_target.name + '\'s Readability Report'
        if channel_target:
            title += ' (#{})'.format(channel_target.name)
        summary = discord.Embed(colour=discord.Colour(0xff6600), title=title)

        await self._add_grade_field(summary, grade)

        await ctx.send(embed=summary)

    @vocab.group(name='server', invoke_without_command=True)
    async def vocab_server(
            self,
            ctx: commands.Context,
            arg1: Optional[discord.TextChannel]
    ) -> None:
        channel_target = None
        if arg1:
            channel_target = arg1

        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                server_status = await utility.server_status(conn, ctx.guild)
                await self._handle_server_status_response(ctx, server_status)
                if server_status != STATUS.AVAILABLE:
                    return

                if channel_target:
                    channel_status = await utility.channel_status(conn, ctx.channel)
                    await self._handle_channel_status_response(ctx, channel_status)
                    if channel_status != STATUS.AVAILABLE:
                        return

                    df = await self._get_agg_msgs(conn, ctx.guild.id, channel_id=channel_target.id)
                    df_grade = await self._get_agg_msgs_grade(
                        conn, ctx.guild.id, channel_id=channel_target.id
                    )
                else:
                    df = await self._get_agg_msgs(conn, ctx.guild.id)
                    df_grade = await self._get_agg_msgs_grade(conn, ctx.guild.id)

        if len(df.index) == 0 or len(df_grade.index) == 0:
            target_name = '#' + channel_target.name if channel_target else '**' + ctx.guild.name + '**'
            await ctx.send('It looks like there are not enough words said in {}.'.format(target_name))
            return

        [ranking_result, unique_result, interesting_result, grade] \
            = await self.bot.loop.run_in_executor(None, partial(self._summary, df, df_grade, server=True))

        if len(ranking_result) == 0 or len(unique_result) == 0 or len(unique_result) == 0:
            await ctx.send(
                'It looks like there aren\'t enough users that have spoken in this server to generate a report'
            )
            return

        title = ctx.guild.name + '\'s Vocab Report'
        if channel_target:
            title += ' (#{})'.format(channel_target.name)
        summary = discord.Embed(colour=discord.Colour(0xff6600), title=title)

        await self._add_ranking_field(summary, ranking_result, channel_target)

        await self._add_unique_field(summary, unique_result)

        await self._add_interesting_field(summary, interesting_result, inline=False)

        await self._add_grade_field(summary, grade, server=True)

        await ctx.send(embed=summary)

    @vocab_server.command(name='ranking')
    async def vocab_server_ranking(
            self,
            ctx: commands.Context,
            arg1: Optional[discord.TextChannel]
    ) -> None:
        channel_target = None
        if arg1:
            channel_target = arg1

        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                server_status = await utility.server_status(conn, ctx.guild)
                await self._handle_server_status_response(ctx, server_status)
                if server_status != STATUS.AVAILABLE:
                    return

                if channel_target:
                    channel_status = await utility.channel_status(conn, ctx.channel)
                    await self._handle_channel_status_response(ctx, channel_status)
                    if channel_status != STATUS.AVAILABLE:
                        return

                    df = await self._get_agg_msgs(conn, ctx.guild.id, channel_id=channel_target.id)
                else:
                    df = await self._get_agg_msgs(conn, ctx.guild.id)

        if len(df.index) == 0:
            target_name = '#' + channel_target.name if channel_target else '**' + ctx.guild.name + '**'
            await ctx.send('It looks like there are not enough words said in {}.'.format(target_name))
            return

        ranking_result = await self.bot.loop.run_in_executor(None, partial(self._ranking, df))

        if len(ranking_result) == 0:
            await ctx.send(
                'It looks like there aren\'t enough users that have spoken in this server to generate a report'
            )
            return

        title = ctx.guild.name + '\'s Ranking Report'
        if channel_target:
            title += ' (#{})'.format(channel_target.name)
        summary = discord.Embed(colour=discord.Colour(0xff6600), title=title)

        await self._add_ranking_field(summary, ranking_result, channel_target)

        await ctx.send(embed=summary)

    @vocab_server.command(name='unique')
    async def vocab_server_unique(
            self,
            ctx: commands.Context,
            arg1: Optional[discord.TextChannel]
    ) -> None:
        channel_target = None
        if arg1:
            channel_target = arg1

        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                server_status = await utility.server_status(conn, ctx.guild)
                await self._handle_server_status_response(ctx, server_status)
                if server_status != STATUS.AVAILABLE:
                    return

                if channel_target:
                    channel_status = await utility.channel_status(conn, ctx.channel)
                    await self._handle_channel_status_response(ctx, channel_status)
                    if channel_status != STATUS.AVAILABLE:
                        return

                    df = await self._get_agg_msgs(conn, ctx.guild.id, channel_id=channel_target.id)
                else:
                    df = await self._get_agg_msgs(conn, ctx.guild.id)

        if len(df.index) == 0:
            target_name = '#' + channel_target.name if channel_target else '**' + ctx.guild.name + '**'
            await ctx.send('It looks like there are not enough words said in {}.'.format(target_name))
            return

        unique_result = await self.bot.loop.run_in_executor(None, partial(self._unique, df))

        if len(unique_result) == 0:
            await ctx.send(
                'It looks like there aren\'t enough users that have spoken in this server to generate a report'
            )
            return

        title = ctx.guild.name + '\'s Unique Words Report'
        if channel_target:
            title += ' (#{})'.format(channel_target.name)
        summary = discord.Embed(colour=discord.Colour(0xff6600), title=title)

        await self._add_unique_field(summary, unique_result)

        await ctx.send(embed=summary)

    @vocab_server.command(name='interesting')
    async def vocab_server_interesting(
            self,
            ctx: commands.Context,
            arg1: Optional[discord.TextChannel]
    ) -> None:
        channel_target = None
        if arg1:
            channel_target = arg1

        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                server_status = await utility.server_status(conn, ctx.guild)
                await self._handle_server_status_response(ctx, server_status)
                if server_status != STATUS.AVAILABLE:
                    return

                if channel_target:
                    channel_status = await utility.channel_status(conn, ctx.channel)
                    await self._handle_channel_status_response(ctx, channel_status)
                    if channel_status != STATUS.AVAILABLE:
                        return

                    df = await self._get_agg_msgs(conn, ctx.guild.id, channel_id=channel_target.id)
                else:
                    df = await self._get_agg_msgs(conn, ctx.guild.id)

        if len(df.index) == 0:
            target_name = '#' + channel_target.name if channel_target else '**' + ctx.guild.name + '**'
            await ctx.send('It looks like there are not enough words said in {}.'.format(target_name))
            return

        interesting_result = await self.bot.loop.run_in_executor(None, partial(self._interesting, df))

        if len(interesting_result) == 0:
            await ctx.send(
                'It looks like there aren\'t enough users that have spoken in this server to generate a report'
            )
            return

        title = ctx.guild.name + '\'s Interesting Words Report'
        if channel_target:
            title += ' (#{})'.format(channel_target.name)
        summary = discord.Embed(colour=discord.Colour(0xff6600), title=title)

        await self._add_interesting_field(summary, interesting_result)

        await ctx.send(embed=summary)

    @vocab_server.command(name='grade')
    async def vocab_server_grade(
            self,
            ctx: commands.Context,
            arg1: Optional[discord.TextChannel]
    ) -> None:
        channel_target = None
        if arg1:
            channel_target = arg1

        async with get_conn(self.bot) as conn:
            async with conn.transaction():
                server_status = await utility.server_status(conn, ctx.guild)
                await self._handle_server_status_response(ctx, server_status)
                if server_status != STATUS.AVAILABLE:
                    return

                if channel_target:
                    channel_status = await utility.channel_status(conn, ctx.channel)
                    await self._handle_channel_status_response(ctx, channel_status)
                    if channel_status != STATUS.AVAILABLE:
                        return

                    df_grade = await self._get_agg_msgs_grade(conn, ctx.guild.id, channel_id=channel_target.id)
                else:
                    df_grade = await self._get_agg_msgs_grade(conn, ctx.guild.id)

        if len(df_grade.index) == 0:
            target_name = '#' + channel_target.name if channel_target else '**' + ctx.guild.name + '**'
            await ctx.send('It looks like there are not enough words said in {}.'.format(target_name))
            return

        grade = await self.bot.loop.run_in_executor(None, partial(self._grade, df_grade))

        title = ctx.guild.name + '\'s Readability Report'
        if channel_target:
            title += ' (#{})'.format(channel_target.name)
        summary = discord.Embed(colour=discord.Colour(0xff6600), title=title)

        await self._add_grade_field(summary, grade, server=True)

        await ctx.send(embed=summary)

    @vocab_server.group(name='global', invoke_without_command=True)
    async def vocab_server_global(self, ctx: commands.Context):
        await ctx.send(inspect.stack()[0][3])

    @vocab_server_global.command(name='ranking')
    async def vocab_server_global_ranking(self, ctx: commands.Context):
        await ctx.send(inspect.stack()[0][3])

    @vocab_server_global.command(name='unique')
    async def vocab_server_global_unique(self, ctx: commands.Context):
        await ctx.send(inspect.stack()[0][3])


async def setup(bot: StatBot) -> None:
    await bot.add_cog(Vocab(bot))
