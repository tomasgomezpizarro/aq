from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime
import math
from aq_lib.model.md_entry_type import MDEntryType
from aq_lib.utils.logger import logger
from backtrader import date2num

from arquants import BaseData


class DisaggregatedData(BaseData):

    data_type = 'disaggregated'

    lines = (('bid_px_1'), ('bid_qty_1'), ('offer_px_1'), ('offer_qty_1'), ('last_px'), ('last_qty') ,('volume'),
             ('trade_px'), ('trade_qty'), ('bid_px_2'), ('bid_qty_2'), ('offer_px_2'), ('offer_qty_2'), ('bid_px_3'),
             ('bid_qty_3'), ('offer_px_3'), ('offer_qty_3'), ('bid_px_4'), ('bid_qty_4'), ('offer_px_4'), ('offer_qty_4'),
             ('bid_px_5'), ('bid_qty_5'), ('offer_px_5'), ('offer_qty_5'), ('dummy_line'), ('nominal_volume'),
             ('effective_volume'), ('close_px'))

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.a = 8
        self.bidPxDepth = []
        self.bidQtyDepth = []
        self.offerPxDepth = []
        self.offerQtyDepth = []
        self.bidOrdersDepth = []
        self.offerOrdersDepth = []

        self.bid_orders_1 = list()
        self.bid_orders_2 = list()
        self.bid_orders_3 = list()
        self.bid_orders_4 = list()
        self.bid_orders_5 = list()

        self.offer_orders_1 = list()
        self.offer_orders_2 = list()
        self.offer_orders_3 = list()
        self.offer_orders_4 = list()
        self.offer_orders_5 = list()

        self.init_lines_dept()

    def init_lines_dept(self):

        self.bidPxDepth.append(self.lines.bid_px_1)
        self.bidPxDepth.append(self.lines.bid_px_2)
        self.bidPxDepth.append(self.lines.bid_px_3)
        self.bidPxDepth.append(self.lines.bid_px_4)
        self.bidPxDepth.append(self.lines.bid_px_5)

        self.bidQtyDepth.append(self.lines.bid_qty_1)
        self.bidQtyDepth.append(self.lines.bid_qty_2)
        self.bidQtyDepth.append(self.lines.bid_qty_3)
        self.bidQtyDepth.append(self.lines.bid_qty_4)
        self.bidQtyDepth.append(self.lines.bid_qty_5)

        self.offerPxDepth.append(self.lines.offer_px_1)
        self.offerPxDepth.append(self.lines.offer_px_2)
        self.offerPxDepth.append(self.lines.offer_px_3)
        self.offerPxDepth.append(self.lines.offer_px_4)
        self.offerPxDepth.append(self.lines.offer_px_5)

        self.offerQtyDepth.append(self.lines.offer_qty_1)
        self.offerQtyDepth.append(self.lines.offer_qty_2)
        self.offerQtyDepth.append(self.lines.offer_qty_3)
        self.offerQtyDepth.append(self.lines.offer_qty_4)
        self.offerQtyDepth.append(self.lines.offer_qty_5)

        self.bidOrdersDepth.append(self.bid_orders_1)
        self.bidOrdersDepth.append(self.bid_orders_2)
        self.bidOrdersDepth.append(self.bid_orders_3)
        self.bidOrdersDepth.append(self.bid_orders_4)
        self.bidOrdersDepth.append(self.bid_orders_5)

        self.offerOrdersDepth.append(self.offer_orders_1)
        self.offerOrdersDepth.append(self.offer_orders_2)
        self.offerOrdersDepth.append(self.offer_orders_3)
        self.offerOrdersDepth.append(self.offer_orders_4)
        self.offerOrdersDepth.append(self.offer_orders_5)

    def _load_md(self, md):
        dt = date2num(datetime.datetime.now())
        self.lines.datetime[0] = dt
        loaded = False
        bid_loaded = False
        offer_loaded = False
        bid_position = 0
        offer_position = 0
        bid_last_price = -1
        offer_last_price = -1
        clean_bid = False
        clean_offer = False
        for entry in md.entries:

            if entry.entry_type == MDEntryType.BID.value:
                if not clean_bid:
                    clean_bid = True
                    self.clean_orders(self.bidOrdersDepth)
                if bid_position <= 4:
                    if bid_last_price == -1:
                        bid_last_price = entry.entry_px

                    if bid_last_price == entry.entry_px:
                        self.bidOrdersDepth[bid_position].append(entry.entry_size)
                        if not bid_loaded:
                            self.bidPxDepth[bid_position][0] = entry.entry_px
                            bid_loaded = True
                    else:
                        bid_position += 1
                        if bid_position <= 4:
                            self.bidPxDepth[bid_position][0] = entry.entry_px
                            self.bidOrdersDepth[bid_position].append(entry.entry_size)
                            bid_last_price = entry.entry_px
                            bid_loaded = True

            elif entry.entry_type == MDEntryType.OFFER.value:
                if not clean_offer:
                    self.clean_orders(self.offerOrdersDepth)
                    clean_offer = True
                if offer_position <= 4:

                    if offer_last_price == -1:
                        offer_last_price = entry.entry_px

                    if offer_last_price == entry.entry_px:
                        self.offerOrdersDepth[offer_position].append(entry.entry_size)
                        if not offer_loaded:
                            self.offerPxDepth[offer_position][0] = entry.entry_px
                            offer_loaded = True
                    else:
                        offer_position += 1
                        if offer_position <= 4:
                            self.offerPxDepth[offer_position][0] = entry.entry_px
                            self.offerOrdersDepth[offer_position].append(entry.entry_size)
                            offer_last_price = entry.entry_px
                            offer_loaded = True

            elif entry.entry_type == MDEntryType.TRADE.value:
                if entry.trd_type is not None:
                    self.lines.trade_px[0] = entry.entry_px
                    self.lines.trade_qty[0] = entry.entry_size
                    if not math.isnan(self.lines.volume[-1]):
                        self.lines.volume[0] = self.lines.volume[-1] + entry.entry_size
                    else:
                        self.lines.volume[0] = entry.entry_size
                self.lines.last_px[0] = entry.entry_px
                self.lines.last_qty[0] = entry.entry_size
                logger.debug("saving last price in position for %s" % self.tradecontract)
                self.position.update_on_md(self.tradecontract, entry.entry_px)
                loaded = True
            elif entry.entry_type == MDEntryType.CLOSE.value:
                self.lines.close_px[0] = entry.entry_px
                loaded = True
            elif entry.entry_type == MDEntryType.TRADE_VOLUME.value:
                self.lines.effective_volume[0] = entry.entry_px
                self.lines.nominal_volume[0] = entry.entry_size
                loaded = True

        for i in range(0, 5):
            orders = self.offerOrdersDepth[i]
            if orders:
                self.offerQtyDepth[i] = sum(orders)
            orders = self.bidOrdersDepth[i]
            if orders:
                self.bidQtyDepth[i] = sum(orders)

        if math.isnan(self.lines.volume[0]):
            if not math.isnan(self.lines.volume[-1]):
                self.lines.volume[0] = self.lines.volume[-1]
                loaded = True

        if not (loaded or bid_loaded or offer_loaded):
            self.lines.dummy_line[0] = 0
        return True

    def clean_orders(self, orders_depth):
        for i in range(0, len(orders_depth)):
            orders_depth[i].clear()
