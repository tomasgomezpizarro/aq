from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import datetime

from aq_lib.model.md_entry_type import MDEntryType
from backtrader import date2num

from arquants.feed.base_data import BaseData


class TopOfBookData(BaseData):

    data_type = 'top'

    lines = ('bid_px', 'bid_qty', 'offer_px', 'offer_qty', 'last_px', 'last_qty', 'close_px', 'nominal_volume',
             'effective_volume', 'dummy_line')

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.bidPxDepth = []
        self.bidQtyDepth = []
        self.offerPxDepth = []
        self.offerQtyDepth = []
        self.init_lines_depth()

    def init_lines_depth(self):
        self.bidPxDepth.append(self.lines.bid_px)
        self.bidQtyDepth.append(self.lines.bid_qty)
        self.offerPxDepth.append(self.lines.offer_px)
        self.offerQtyDepth.append(self.lines.offer_qty)

    def _load_md(self, md):
        dt = date2num(datetime.datetime.now())
        self.lines.datetime[0] = dt
        loaded = False
        for entry in md.entries:
            position = 0
            if entry.position is not None:
                position = entry.position - 1
            if entry.entry_type == MDEntryType.BID.value:
                if position <= 4:
                    self.bidPxDepth[position][0] = entry.entry_px
                    self.bidQtyDepth[position][0] = entry.entry_size
                    loaded = True
            elif entry.entry_type == MDEntryType.OFFER.value:
                if position <= 4:
                    self.offerPxDepth[position][0] = entry.entry_px
                    self.offerQtyDepth[position][0] = entry.entry_size
                    loaded = True
            elif entry.entry_type == MDEntryType.TRADE.value:
                self.lines.last_px[0] = entry.entry_px
                self.lines.last_qty[0] = entry.entry_size
                self.position.update_on_md(self.tradecontract, entry.entry_px)
                loaded = True
            elif entry.entry_type == MDEntryType.CLOSE.value:
                self.lines.close_px[0] = entry.entry_px
                loaded = True
            elif entry.entry_type == MDEntryType.TRADE_VOLUME.value:
                self.lines.effective_volume[0] = entry.entry_px
                self.lines.nominal_volume[0] = entry.entry_size
                loaded = True

        if not loaded:
            self.lines.dummy_line[0] = 0
        return True
