from contextlib import asynccontextmanager
from enum import Enum
from functools import partial

import asyncpg
import discord
import numpy as np
import pandas as pd
from PIL import Image

from core.statbot import StatBot


def _get_image_array(image_name: str):
    image = Image.open(image_name)
    array = np.array(image)
    image.close()

    return array


async def get_image_array(loop, image_name):
    func = partial(_get_image_array, image_name)
    return await loop.run_in_executor(None, func)


@asynccontextmanager
async def get_conn(bot: StatBot) -> None:
    # ENTER SECTION
    conn = await bot.pool.acquire()

    try:
        yield conn
    finally:
        # EXIT SECTION
        await bot.pool.release(connection=conn)


async def fetch_as_dataframe(con: asyncpg.Connection, query: str, *args: str) -> pd.DataFrame:
    stmt = await con.prepare(query)
    columns = [a.name for a in stmt.get_attributes()]
    data = await stmt.fetch(*args)
    return pd.DataFrame(columns=columns, data=data)


class Status(Enum):
    AVAILABLE = 1
    NOT_ADDED = 2
    IMPORTING = 3


async def server_status(conn: asyncpg.Connection, server: discord.Guild, lock_for_update: bool = False) -> Status:
    if lock_for_update:
        row = await conn.fetchrow(
            'SELECT Importing '
            'FROM statbot_db.SERVERS '
            'WHERE ServerID = $1 FOR UPDATE',
            server.id
        )
    else:
        row = await conn.fetchrow(
            'SELECT Importing '
            'FROM statbot_db.SERVERS '
            'WHERE ServerID = $1 FOR KEY SHARE',
            server.id
        )

    if not row:
        return Status.NOT_ADDED
    elif row['importing'] is True:
        return Status.IMPORTING

    return Status.AVAILABLE


async def channel_status(
        conn: asyncpg.Connection, channel: discord.TextChannel, lock_for_update: bool = False
) -> Status:
    if lock_for_update:
        row = await conn.fetchrow(
            'SELECT Importing '
            'FROM statbot_db.CHANNELS '
            'WHERE ChannelID = $1 FOR UPDATE',
            channel.id
        )
    else:
        row = await conn.fetchrow(
            'SELECT Importing '
            'FROM statbot_db.CHANNELS '
            'WHERE ChannelID = $1 FOR KEY SHARE',
            channel.id
        )

    if not row:
        return Status.NOT_ADDED
    elif row['importing'] is True:
        return Status.IMPORTING

    return Status.AVAILABLE
