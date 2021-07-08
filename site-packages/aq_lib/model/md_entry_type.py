from enum import Enum


class MDEntryType(Enum):
    BID = '0'
    OFFER = '1'
    TRADE = '2'
    CLOSE = 'e'
    TRADE_VOLUME = 'B'
