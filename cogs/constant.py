import re

# Wordcloud Settings
WC_MARGIN = 2
WC_HEIGHT = 360
WC_WIDTH = 640
WC_SCALE = 2
WC_COLLOCATIONS = True
WC_MAX_WORDS = 800
WC_MAX_FONT_SIZE = None
WC_COLOR_MODE = 'RGB'
WC_FILE_FORMAT = 'png'

WC_IMAGES = {
    'ladybug': {
        'image_path': './wordcloud/ladybug_color.png',
        'color_image_path': None,
        'bg_color': 'white',
        'scale': None
    },

    'egg': {
        'image_path': './wordcloud/egg.png',
        'color_image_path': None,
        'bg_color': None,
        'scale': None
    },

    'sus': {
        'image_path': './wordcloud/sus_mask.png',
        'color_image_path': './wordcloud/sus_color.png',
        'bg_color': None,
        'scale': None
    },

    'burger': {
        'image_path': './wordcloud/burger_color.png',
        'color_image_path': None,
        'bg_color': None,
        'scale': None
    },

    'dog': {
        'image_path': './wordcloud/dog_color.png',
        'color_image_path': None,
        'bg_color': None,
        'scale': None
    },

    'minion': {
        'image_path': './wordcloud/minion_color.png',
        'color_image_path': None,
        'bg_color': None,
        'scale': None
    },

    'monkey': {
        'image_path': './wordcloud/monkey_color.png',
        'color_image_path': None,
        'bg_color': None,
        'scale': None
    },

    'koala': {
        'image_path': './wordcloud/koala_mask.png',
        'color_image_path': './wordcloud/koala_color.png',
        'bg_color': None,
        'scale': None
    },

    'cock': {
        'image_path': './wordcloud/cock_color.png',
        'color_image_path': None,
        'bg_color': None,
        'scale': None
    },

    'frog': {
        'image_path': './wordcloud/frog_color.png',
        'color_image_path': None,
        'bg_color': None,
        'scale': None
    },

    'orangutan': {
        'image_path': './wordcloud/orangutan_color.png',
        'color_image_path': None,
        'bg_color': None,
        'scale': None
    },

    'seal': {
        'image_path': './wordcloud/seal_color.png',
        'color_image_path': None,
        'bg_color': None,
        'scale': None
    },

    'spaghetti': {
        'image_path': './wordcloud/spaghetti_color.png',
        'color_image_path': None,
        'bg_color': None,
        'scale': None
    },
}

WC_MASK_ARGS = [x for x in WC_IMAGES.keys()]

# Common Regular Expressions
REGEX = {
    'urls': re.compile(
        r'https?:\/\/(www\.)?'
        r'[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&\/\/=]*)'
    ),

    'user_mention': re.compile(r'<@!?(\d+)>'),

    'channel_mention': re.compile(r'<#(\d+)>'),

    'emojis': re.compile(r'<a?:[\S]+:([0-9]+)>'),

    'emoji_names': re.compile(r'<a?:([\S]+):[0-9]+>'),

    'punctuation': re.compile(r'(?<=\w)[^\s\w](?![^\s\w])'),
}

# Common bot responses to situations
RESPONSES = {
    'server_importing': 'Sorry! I\'m busy rebuilding the database for this server right now. '
                        'This might take a while, please try again later.',

    'server_not_added': 'Sorry! This server hasn\'t been added to the database yet.',

    'channel_importing': 'Sorry! I\'m busy rebuilding the database for this channel right now. '
                         'This might take a while, please try again later.',

    'channel_not_added': 'I can\'t see this channel! '
                         'This could be because I don\'t have the permissions to see it, or it doesn\'t exist.',
}
