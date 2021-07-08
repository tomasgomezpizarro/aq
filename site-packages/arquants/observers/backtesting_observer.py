import json

from backtrader import Observer
from arquants.definitions import aqhandler
from arquants.event.event_factory import eventFactory


class BacktestingObserver(Observer):

    def __init__(self, *args, **kwargs):
        self.strategy_id = kwargs.get('strategy_id', None)
        self.strategy_version = kwargs.get('strategy_version', None)
        self.execution_id = kwargs.get('execution_id', None)
        self.arquants_user = kwargs.get('arquants_user', None)
        self.delivery_routing_key = kwargs.get('delivery_routing_key', None)
        self.execution_type = kwargs.get('execution_type', None)
        super(BacktestingObserver, self).__init__(self, *args, **kwargs)

    # No storeo nada en la estrategia
    plotinfo = dict(plot=False, subplot=False, plotlinelabels=False)
    _stclock = False

    # requerido por cerebrodockerfi
    lines = ('',)

    def next(self):
        positions = dict()
        for data in self._owner.datas:
            positions[data.tradecontract] = self._owner.broker.getposition(data, True).get_position()
        aqhandler.send(json.dumps(eventFactory.createPositionUpdatedEvent(positions).to_dict()),
                       self.delivery_routing_key)
