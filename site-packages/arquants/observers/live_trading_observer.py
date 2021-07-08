from backtrader import Observer


class LiveTradingObserver(Observer):

    def __init__(self, *args, **kwargs):
        self.delivery_routing_key = kwargs.get('delivery_routing_key', None)
        super(LiveTradingObserver, self).__init__(self, *args, **kwargs)

    # No storeo nada en la estrategia
    plotinfo = dict(plot=False, subplot=False, plotlinelabels=False)
    _stclock = False

    # requerido por cerebrodockerfi
    lines = ('',)

    def next(self):
        positions = dict()
        for data in self._owner.datas:
            position = self._owner.broker.getposition(data, True)
            if position.updated:
                positions[data.tradecontract] = position.get_position()
                position.updated = False
        #  aqhandler.send(json.dumps(eventFactory.createPositionUpdatedEvent(positions).to_dict()),
                       #  self.delivery_routing_key)
