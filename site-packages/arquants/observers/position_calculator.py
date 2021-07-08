import json

from backtrader import Position

from arquants.definitions import aqhandler
from arquants import eventFactory
from arquants.definitions import force_async


class PositionCalculator(Position):

    def __init__(self):
        super(PositionCalculator, self).__init__()
        self.last_px = 0
        self.close_position = 0
        self.symbol = None
        self.changed = False

    def update(self, size, price, dt=None):
        self.changed = True
        new_size, new_avg_price, _open, close = super(PositionCalculator, self).update(size, price)
        if close != 0:
            if size < 0:
                close = -close
            else:
                close = abs(close)
            self.close_position += (price - self.price_orig) * close
        return new_size, new_avg_price, _open, close

    def get_position(self):
        return {"symbol": self.symbol, "size": self.size, "price": self.price,
                "open_position": self.get_open_position(), "close_position": self.close_position}

    def get_open_position(self):
        if self.last_px != 0:
            return (self.last_px - self.price) * self.size
        else:
            return 0

    def update_on_md(self, symbol, last_px):
        if self.last_px != last_px:
            self.last_px = last_px
            self.changed = True
        if self.symbol is None:
            self.symbol = symbol
        if self.changed:
            self.publish_position()
            self.changed = False

    @force_async
    def publish_position(self):
        aqhandler.send(json.dumps(eventFactory.createPositionUpdatedEvent(self.get_position()).to_dict()), "worker")
