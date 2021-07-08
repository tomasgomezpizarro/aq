from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import datetime
import math
from aq_lib.model.md_entry_type import MDEntryType
from backtrader import date2num
from arquants.feed.base_data import BaseData


class AggregatedData(BaseData):

    data_type = 'aggregated'

    lines = (('bid_px'), ('bid_qty'), ('offer_px'), ('offer_qty'), ('last_px'), ('last_qty'),('volume'),('trade_px'),
             ('trade_qty'),('bid_px_2'), ('bid_qty_2'), ('offer_px_2'), ('offer_qty_2'),('bid_px_3'), ('bid_qty_3'),
             ('offer_px_3'), ('offer_qty_3'),('bid_px_4'), ('bid_qty_4'), ('offer_px_4'), ('offer_qty_4'),('bid_px_5'),
             ('bid_qty_5'), ('offer_px_5'), ('offer_qty_5'),('dummy_line'),('nominal_volume'),('effective_volume'),('close_px'))

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.bidPxDepth = []
        self.bidQtyDepth = []
        self.offerPxDepth = []
        self.offerQtyDepth = []
        self.initLinesDept()

    def initLinesDept(self):
        self.bidPxDepth.append(self.lines.bid_px)
        self.bidPxDepth.append(self.lines.bid_px_2)
        self.bidPxDepth.append(self.lines.bid_px_3)
        self.bidPxDepth.append(self.lines.bid_px_4)
        self.bidPxDepth.append(self.lines.bid_px_5)

        self.bidQtyDepth.append(self.lines.bid_qty)
        self.bidQtyDepth.append(self.lines.bid_qty_2)
        self.bidQtyDepth.append(self.lines.bid_qty_3)
        self.bidQtyDepth.append(self.lines.bid_qty_4)
        self.bidQtyDepth.append(self.lines.bid_qty_5)

        self.offerPxDepth.append(self.lines.offer_px)
        self.offerPxDepth.append(self.lines.offer_px_2)
        self.offerPxDepth.append(self.lines.offer_px_3)
        self.offerPxDepth.append(self.lines.offer_px_4)
        self.offerPxDepth.append(self.lines.offer_px_5)

        self.offerQtyDepth.append(self.lines.offer_qty)
        self.offerQtyDepth.append(self.lines.offer_qty_2)
        self.offerQtyDepth.append(self.lines.offer_qty_3)
        self.offerQtyDepth.append(self.lines.offer_qty_4)
        self.offerQtyDepth.append(self.lines.offer_qty_5)

    def _load_md(self, md):
        dt = date2num(datetime.datetime.now())
        # logger.debug("load_md will add data %s" % md.to_dict())
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
                if entry.trd_type is not None:
                    self.lines.trade_px[0] = entry.entry_px
                    self.lines.trade_qty[0] = entry.entry_size
                    # Acumulo el volumen operado visto
                    if not math.isnan(self.lines.volume[-1]):
                        self.lines.volume[0] = self.lines.volume[-1] + entry.entry_size
                    else:
                        self.lines.volume[0] = entry.entry_size
                self.lines.last_px[0] = entry.entry_px
                self.lines.last_qty[0] = entry.entry_size
                self.position.update_on_md(self.tradecontract,entry.entry_px)
                loaded = True
            elif entry.entry_type == MDEntryType.CLOSE.value:
                self.lines.close_px[0] = entry.entry_px
            elif entry.entry_type == MDEntryType.TRADE_VOLUME.value:
                self.lines.effective_volume[0] = entry.entry_px
                self.lines.nominal_volume[0] = entry.entry_size
                loaded = True
        # Intentamos trasladar el volumen del momento anterior al actual para no perder el acumulado
        if math.isnan(self.lines.volume[0]):
            if not math.isnan(self.lines.volume[-1]):
                self.lines.volume[0] = self.lines.volume[-1]
                loaded = True
        if not loaded:
            self.lines.dummy_line[0] = 0
        return True
