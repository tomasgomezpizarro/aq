import datetime

import tempfile

import random
from backtrader.feeds import GenericCSVData
import csv


class MockData(GenericCSVData):
    TEMP_FILE = tempfile.mkstemp()

    _days_offset = None
    _LAST_PX = None

    params = (
        ('dataname', TEMP_FILE[1]),
        ('fromdate', datetime.datetime(2000, 1, 1)),
        ('todate', datetime.datetime(2000, 2, 27)),
        ('datetime', 0),
        ('last_px', 1),
        ('high', 2),
        ('low', 3),
        ('open', 4),
        ('close', 5),
        ('volume', 6),
        ('bid_px', 7),
        ('bid_qty', 8),
        ('offer_px', 9),
        ('offer_qty', 10),
        ('last_qty', 11),
        ('trade_px', 12),
        ('trade_qty', 13),
        ('bid_px_2', 14),
        ('bid_qty_2', 15),
        ('offer_px_2', 16),
        ('offer_qty_2', 17),
        ('bid_px_3', 18),
        ('bid_qty_3', 19),
        ('offer_px_3', 20),
        ('offer_qty_3', 21),
        ('bid_px_4', 22),
        ('bid_qty_4', 23),
        ('offer_px_4', 24),
        ('offer_qty_4', 25),
        ('bid_px_5', 26),
        ('bid_qty_5', 27),
        ('offer_px_5', 28),
        ('offer_qty_5', 29),
    )

    lines = (
    ('bid_px'), ('bid_qty'), ('offer_px'), ('offer_qty'), ('last_px'), ('last_qty'), ('trade_px'), ('trade_qty'),
    ('volume'), ('bid_px_2'), ('bid_qty_2'), ('offer_px_2'), ('offer_qty_2'), ('bid_px_3'), ('bid_qty_3'),
    ('offer_px_3'), ('offer_qty_3'), ('bid_px_4'), ('bid_qty_4'), ('offer_px_4'), ('offer_qty_4'), ('bid_px_5'),
    ('bid_qty_5'), ('offer_px_5'), ('offer_qty_5'))
    _params = params

    def __init__(self, tradecontract, *args, **kwargs):
        self.fieldsnames = [self._params[i][0] for i in range(3, len(self._params))]
        self.csv_generator()
        self.tradecontract = 'data' + str(tradecontract)
        super().__init__(*args, **kwargs)

    def csv_generator(self):
        with open(self._params[0][1], 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldsnames)
            writer.writeheader()
            for day in range(self.get_total_days()):
                line = self.make_lines(day)
                writer.writerow(line)

    def get_total_days(self):
        return (self._params[2][1] - self._params[1][1]).days

    def make_lines(self, day):
        self._days_offset = day
        result_dic = dict()
        for field in self.fieldsnames:
            result_dic[field] = getattr(self, '_' + field)()

        return result_dic

    def _datetime(self):
        return self._params[1][1] - datetime.timedelta(days=-self._days_offset)

    def _last_px(self):
        if self._LAST_PX:
            self._LAST_PX += random.random() * random.randrange(-1, 2)
            return self._LAST_PX
        else:
            self._LAST_PX = random.randint(22, 1509) + random.random()
            return self._LAST_PX

    def _high(self):
        return self._LAST_PX + 3

    def _low(self):
        return self._LAST_PX - 3

    def _open(self):
        return self._LAST_PX - 2

    def _close(self):
        return self._LAST_PX + 2

    @staticmethod
    def _volume():
        return random.randint(22, 100)

    def _bid_px(self):
        return self._LAST_PX - random.random()

    def _bid_px_2(self):
        return self._LAST_PX - (1 + random.random())

    def _bid_px_3(self):
        return self._LAST_PX - (2 + random.random())

    def _bid_px_4(self):
        return self._LAST_PX - (3 + random.random())

    def _bid_px_5(self):
        return self._LAST_PX - (4 + random.random())

    @staticmethod
    def _bid_qty():
        return random.randint(5, 10)

    @staticmethod
    def _bid_qty_2():
        return random.randint(5, 10)

    @staticmethod
    def _bid_qty_3():
        return random.randint(5, 10)

    @staticmethod
    def _bid_qty_4():
        return random.randint(5, 10)

    @staticmethod
    def _bid_qty_5():
        return random.randint(5, 10)

    def _offer_px(self):
        return self._LAST_PX + random.random()

    def _offer_px_2(self):
        return self._LAST_PX + (1 + random.random())

    def _offer_px_3(self):
        return self._LAST_PX + (2 + random.random())

    def _offer_px_4(self):
        return self._LAST_PX + (3 + random.random())

    def _offer_px_5(self):
        return self._LAST_PX + (4 + random.random())

    @staticmethod
    def _offer_qty():
        return random.randint(5, 10)

    @staticmethod
    def _offer_qty_2():
        return random.randint(5, 10)

    @staticmethod
    def _offer_qty_3():
        return random.randint(5, 10)

    @staticmethod
    def _offer_qty_4():
        return random.randint(5, 10)

    @staticmethod
    def _offer_qty_5():
        return random.randint(5, 10)

    @staticmethod
    def _last_qty():
        return random.randint(5, 10)

    @staticmethod
    def _trade_px():
        return random.randint(5, 10)

    @staticmethod
    def _trade_qty():
        return random.randint(5, 10)
