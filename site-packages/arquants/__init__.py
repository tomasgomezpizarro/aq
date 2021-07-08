import traceback

from .event.event_factory import eventFactory
try:
    from arquants.feed.aggregated_data import *
    from arquants.feed.disaggregated_data import *
    from arquants.feed.top_ok_book_data import *
except ImportError as e:
    print(traceback.print_exc())
    print("Error importing data")

try:
    from arquants.brokers.aq_broker import *
except ImportError:
    print("Error importing broker")

from .strategy import *


"""
     ___   _____    _____     _   _       ___   __   _   _____   _____
    /   | |  _  \  /  _  \   | | | |     /   | |  \ | | |_   _| /  ___/
   / /| | | |_| |  | | | |   | | | |    / /| | |   \| |   | |   | |___
  / / | | |  _  /  | | | |   | | | |   / / | | | |\   |   | |   \___  \
 / /  | | | | \ \  | |_| |_  | |_| |  / /  | | | | \  |   | |    ___| |
/_/   |_| |_|  \_\ \_______| \_____/ /_/   |_| |_|  \_|   |_|   /_____/

"""

__title__ = 'ArQuants Algorithm Engine'
__author__ = 'arquants'
__license__ = 'BSD 2-Clause'





