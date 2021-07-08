from enum import Enum
from time import sleep
from multiprocessing import Queue
from arquants.cerebro import AQCerebro
from arquants.definitions import aqhandler
from arquants.stores.aq_store import AQStore


class MDSubsType(Enum):
    AGGREGATED = 'aggregated'
    DISAGGREGATED = 'disaggregated'
    TOP = 'top'


class Symbol:

    def __init__(self, market=None, symbol=None, md_type: MDSubsType = None):
        self.market = market
        self.symbol = symbol
        self.md_type = md_type


class Initiator:

    def __init__(self, symbols=None, strat_cls=None):
        aqhandler.init_public_aq()
        if not aqhandler.get_account():
            raise Exception("No account")

        queue = Queue()
        store = AQStore(queue)
        broker = store.getbroker()

        self.cerebro = AQCerebro(queue, quicknotify=True, stdstats=False)
        self.cerebro.setbroker(broker)

        for item in symbols:
            market = item.market
            symbol = item.symbol
            md_type = item.md_type

            data_cls = store.getdataclass(md_type.value)
            data = data_cls(dataname=symbol, account=aqhandler.get_account(), routingkey='rk-rofx-income',
                             market=market, price_size=1)

            self.cerebro.adddata(data)

        self.cerebro.addstrategy(strat_cls)

        # Le doy unos segundos para que se conecte a la api y al websocket.
        sleep(2)

    def start(self):
        sleep(2)
        self.cerebro.run()
