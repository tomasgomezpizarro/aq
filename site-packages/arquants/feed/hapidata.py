import csv
import random
import tempfile
import dateparser

import jsonpickle
from aq_lib.utils import byte_utils
from backtrader.feeds import GenericCSVData
from backtrader.utils import py3 as util
from dateutil.parser import parse


class HAPIData(GenericCSVData):
    TEMP_FILE = tempfile.mkstemp()

    params = (
        ('dataname', TEMP_FILE[1]),
        ('baseurl', 'http://h-api.primary.com.ar/MHD/TradesOHLC'),
        ('fromhour', "00:00:00"),
        ('tohour', "00:00:00"),

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
        ('trade_qty', 12),
        ('trade_px', 13),
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

    lines = (('bid_px'), ('bid_qty'), ('offer_px'), ('offer_qty'), ('last_px'), ('last_qty'),('trade_px'), ('trade_qty'), ('volume'), ('bid_px_2'), ('bid_qty_2'), ('offer_px_2'), ('offer_qty_2'), ('bid_px_3'), ('bid_qty_3'),
    ('offer_px_3'), ('offer_qty_3'), ('bid_px_4'), ('bid_qty_4'), ('offer_px_4'), ('offer_qty_4'), ('bid_px_5'),
    ('bid_qty_5'), ('offer_px_5'), ('offer_qty_5'))
    _params = params

    def __init__(self, dataname, fromdate, todate):
        self.dataname = dataname
        self.tradecontract = dataname
        self.fieldsnames = [self._params[i][0] for i in range(4, len(self._params))]

        self.fromdate = dateparser.parse(fromdate, date_formats=['%Y-%m-%d'])
        self.todate = dateparser.parse(todate, date_formats=['%Y-%m-%d'])

        url = self.params.baseurl
        url += '/%s' % util.urlquote(dataname)
        # se saca solo el dia desde el string ya que viene en formato yyyy-mm-dd hh:mm:ss
        url += '/%s' % util.urlquote(str(self.fromdate).split(' ')[0])
        url += '/%s' % util.urlquote(str(self.todate).split(' ')[0])
        url += '/%s' % self.params.fromhour
        url += '/%s' % self.params.tohour

        datafile = util.urlopen(url)

        data = datafile.read()
        decodedata = jsonpickle.decode(byte_utils.is_byte_decode(data))
        self.csv_generator(decodedata)

    def csv_generator(self, data):
        with open(self._params[0][1], 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldsnames)
            writer.writeheader()
            if data['status'] == 'ERROR':
                raise Exception("No data for this contract")
            else:
                for i in range(len(data['marketDataH'])):
                    partial_line = data['marketDataH'][i]
                    line = self.make_lines(partial_line)
                    writer.writerow(line)

    def make_lines(self, partial_line):
        result_dic = dict()
        for field in self.fieldsnames:
            result_dic[field] = getattr(self, '_' + field)(partial_line)

        return result_dic

    @staticmethod
    def _datetime(line):
        date = line['datetime'].split('.')[0]
        return parse(date)

    @staticmethod
    def _last_px(line):
        return line['price']

    @staticmethod
    def _last_qty(line):
        return line['size']

    @staticmethod
    def _trade_px(line):
        return line['price']

    @staticmethod
    def _trade_qty(line):
        return line['size']

    @staticmethod
    def _high(line):
        return str(float(line['price']) + 1)

    @staticmethod
    def _low(line):
        return str(float(line['price']) - 1)

    @staticmethod
    def _open(line):
        signo = random.random()
        if signo < 0.5:
            return str(float(line['price']) - random.randint(1, 3))
        return str(float(line['price']) + random.randint(1, 3))

    @staticmethod
    def _close(line):
        signo = random.random()
        if signo < 0.5:
            return str(float(line['price']) - random.randint(1, 3))
        return str(float(line['price']) + random.randint(1, 3))

    @staticmethod
    def _volume(line):
        return random.randint(22, 100)

    @staticmethod
    def _bid_px(line):
        return str(float(line['price']) - random.randint(1, 3))

    @staticmethod
    def _bid_px_2(line):
        return str(float(line['price']) - (1 + random.randint(1, 3)))

    @staticmethod
    def _bid_px_3(line):
        return str(float(line['price']) - (2 + random.randint(1, 3)))

    @staticmethod
    def _bid_px_4(line):
        return str(float(line['price']) - (3 + random.randint(1, 3)))

    @staticmethod
    def _bid_px_5(line):
        return str(float(line['price']) - (4 + random.randint(1, 3)))

    @staticmethod
    def _bid_qty(line):
        signo = random.random()
        if signo < 0.5:
            return str(float(line['size']) - random.randint(1, 3))
        return str(float(line['size']) + random.randint(1, 3))

    @staticmethod
    def _bid_qty_2(line):
        signo = random.random()
        if signo < 0.5:
            return str(float(line['size']) - random.randint(1, 3))
        return str(float(line['size']) + random.randint(1, 3))

    @staticmethod
    def _bid_qty_3(line):
        signo = random.random()
        if signo < 0.5:
            return str(float(line['size']) - random.randint(1, 3))
        return str(float(line['size']) + random.randint(1, 3))

    @staticmethod
    def _bid_qty_4(line):
        signo = random.random()
        if signo < 0.5:
            return str(float(line['size']) - random.randint(1, 3))
        return str(float(line['size']) + random.randint(1, 3))

    @staticmethod
    def _bid_qty_5(line):
        signo = random.random()
        if signo < 0.5:
            return str(float(line['size']) - random.randint(1, 3))
        return str(float(line['size']) + random.randint(1, 3))

    @staticmethod
    def _offer_px(line):
        return str(float(line['price']) + random.randint(1, 3))

    @staticmethod
    def _offer_px_2(line):
        return str(float(line['price']) + (1 + random.randint(1, 3)))

    @staticmethod
    def _offer_px_3(line):
        return str(float(line['price']) + (2 + random.randint(1, 3)))

    @staticmethod
    def _offer_px_4(line):
        return str(float(line['price']) + (3 + random.randint(1, 3)))

    @staticmethod
    def _offer_px_5(line):
        return str(float(line['price']) + (4 + random.randint(1, 3)))

    @staticmethod
    def _offer_qty(line):
        signo = random.random()
        if signo < 0.5:
            return str(float(line['size']) - random.randint(1, 3))
        return str(float(line['size']) + random.randint(1, 3))

    @staticmethod
    def _offer_qty_2(line):
        signo = random.random()
        if signo < 0.5:
            return str(float(line['size']) - random.randint(1, 3))
        return str(float(line['size']) + random.randint(1, 3))

    @staticmethod
    def _offer_qty_3(line):
        signo = random.random()
        if signo < 0.5:
            return str(float(line['size']) - random.randint(1, 3))
        return str(float(line['size']) + random.randint(1, 3))

    @staticmethod
    def _offer_qty_4(line):
        signo = random.random()
        if signo < 0.5:
            return str(float(line['size']) - random.randint(1, 3))
        return str(float(line['size']) + random.randint(1, 3))

    @staticmethod
    def _offer_qty_5(line):
        signo = random.random()
        if signo < 0.5:
            return str(float(line['size']) - random.randint(1, 3))
        return str(float(line['size']) + random.randint(1, 3))
