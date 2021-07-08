import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import os
from aq_lib.configuration.configuration import Configuration
from aq_lib.manager.public_handler import PublicAQHandlerLayer
from aq_lib.manager.wsclient import manager_queue

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = ROOT_DIR + '/config.ini'
configuration = Configuration(CONFIG_PATH)

pool = ThreadPoolExecutor()
sheet_queue = Queue()

##################################################################
# Setting
ws_endpoint_md = os.environ.get('WS_OR_URL', 'wss://api.arquants.trading/live/market_data')
ws_endpoint_er = os.environ.get('WS_ER_URL', 'wss://api.arquants.trading/live/or')
api_endpoint = os.environ.get('API_URL', 'https://api.arquants.trading/api/v1/')

aq_username = os.environ.get('USERNAME', None)
aq_password = os.environ.get('PASSWORD', None)

if aq_password is None or aq_password is None:
    raise Exception('The environment variable USERNAME or PASSWORD was not found')


# TODO: hay que definir un único __init__ para ambos manejadores public/private (polimorfismo).
aqhandler = PublicAQHandlerLayer(aq_username, aq_password, api_endpoint, ws_endpoint_er, ws_endpoint_md, manager_queue)


def force_async(fn):
    """
        decorator:
        turns a sync function to async function using threads
    """

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        future = pool.submit(fn, *args, **kwargs)
        return asyncio.wrap_future(future)

    return wrapper
