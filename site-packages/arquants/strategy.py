import json
import logging
from datetime import date
from aq_lib.event.event import StrategyStatusChangeEvent
from aq_lib.event.event_type import EventType
from aq_lib.model.strategy_status import StrategyStatus
from backtrader import Strategy as btStrategy, LineIterator
from arquants.definitions import sheet_queue, aqhandler
from arquants import sheet_envs
from arquants.event.event_factory import eventFactory
from arquants.helpers import strategy_kwargs_list
from arquants.manager.logger import logger

import arquants.definitions


class Strategy(btStrategy):
    """
    Clase base que presenta la interfaz que deben o pueden implementar las estrategias que extiendan Strategy """
    _callnext = True

    """Routing key in which the PRM workers listen for incoming events."""
    prm_rk = arquants.definitions.configuration.get_property('RABBITMQ', 'PrmConsumerRoutingKey')

    sheet_queue = None
    sheet_reader = None

    def logs(self, message):
        if aqhandler.public:
            logger.info(message)
        else:
            aqhandler.send(json.dumps(eventFactory.createLogsEvent(message).to_dict()), self.prm_rk)

    def log(self, message):
        self.logs(message)

    @staticmethod
    def do_list_of_list(item):
        if type(item.values).__name__ == 'dict':
            row = list()
            for value in item.values.values():
                row.append(value)
            new_values = list()
            new_values.append(row)
            item.values = new_values

    def sheet_log(self, item):
        if sheet_envs.SHEET_INIT:
            self.do_list_of_list(item)
            sheet_envs.LOGGER_QUEUE.put(item)
        else:
            self.logs('[Warning] Intendo loguear sin tener google sheet configurado')

    def row_values(self, sheet_name=None, row=None):
        if sheet_envs.SHEET_INIT:
            return sheet_envs.GSHEET_READER.row_values(sheet_name, row)
        else:
            self.log("[Warning] No hay una sheet configurada")

    def col_values(self, sheet_name=None, col=None):
        if sheet_envs.SHEET_INIT:
            return sheet_envs.GSHEET_READER.col_values(sheet_name, col)
        else:
            self.log("[Warning] No hay una sheet configurada")

    def get_all_values(self, sheet_name):
        if sheet_envs.SHEET_INIT:
            return sheet_envs.GSHEET_READER.get_all_values(sheet_name)
        else:
            self.log("[Warning] No hay una sheet configurada")

    def get_all_records(self, sheet_name):
        if sheet_envs.SHEET_INIT:
            return sheet_envs.GSHEET_READER.get_all_records(sheet_name)
        else:
            self.log("[Warning] No hay una sheet configurada")

    def _next(self):
        try:
            clock_len = self._clk_update()

            for indicator in self._lineiterators[LineIterator.IndType]:
                indicator._next()

            self._notify()

            if self._ltype == LineIterator.StratType:
                # supporting datas with different lengths
                minperstatus = self._getminperstatus()
                if self._callnext:
                    if minperstatus < 0:
                        self.next()
                    elif minperstatus == 0:
                        self.nextstart()  # only called for the 1st value
                    else:
                        self.prenext()
            else:
                # assume indicators and others operate on same length datas
                # although the above operation can be generalized
                if self._callnext:
                    if clock_len > self._minperiod:
                        self.next()
                    elif clock_len == self._minperiod:
                        self.nextstart()  # only called for the 1st value
                    elif clock_len:
                        self.prenext()

            minperstatus = self._getminperstatus()
            self._next_analyzers(minperstatus)
            self._next_observers(minperstatus)

            self.clear()
        except Exception as e:
            self.on_error()
            raise e

    def _start(self):
        super(Strategy, self)._start()
        self.logs("Initializing Strategy...")
        if eventFactory.execution_type == 'live_trading':
            aqhandler.send(json.dumps(eventFactory.createLiveTradingStartedEvent().to_dict()), 'worker')
            logging.getLogger('Strategy')
            logging.info(eventFactory.createLiveTradingStartedEvent().to_dict())
            aqhandler.register_callback(EventType.PARAMETER_CHANGE_EVENT.value, self.process_parameter_change)
            aqhandler.register_callback(EventType.STRATEGY_CHANGE_STATE.value, self.process_status_update)
        self.validate()
        # TODO: utilizar StrategyStatusChengeEvent en lugar de LiveTradingStartedEvent

    def on_pause(self):
        pass

    def on_error(self):
        pass

    def stop(self):
        """
        Averiguar que hace
        :return:
        """
        super(Strategy, self).stop()

    def finish(self):
        """
        Averiguar bien que hace
        :return:
        """
        self.env.runstop()

    def process_parameter_change(self, event):
        """
        Procesa un evento de cambio
        :param event:
        :return:
        """
        for params in event.parameters:
            for name, value in params.items():
                self.__setattr__(name, value)

    def process_status_update(self, event: StrategyStatusChangeEvent):
        """
        Setea _callnext a True o False dependiendo de si el StrategyStatusChangeEvent.status es PAUSED o no.
        :param event:
        :return:
        """
        self._callnext = (event.status != StrategyStatus.PAUSED.value)
        if event.status == StrategyStatus.PAUSED.value:
            self.on_pause()

    def validate(self):
        return True

    def pause(self):
        """
        Pausa la estrategia y publica un evento con su estado
        :return:
        """
        self._callnext = False
        self.on_pause()
        event = eventFactory.createStrategyStatusChangeEvent(status=StrategyStatus.PAUSED.value)
        aqhandler.send(json.dumps(event.to_dict()), routing_key=self.prm_rk)

    def resume(self):
        """
        Resumen al ejecucion de la estrategia y publica un evento con su estado
        :return:
        """
        self._callnext = True
        event = eventFactory.createStrategyStatusChangeEvent(status=StrategyStatus.ACTIVE.value)
        aqhandler.send(json.dumps(event.to_dict()), routing_key=self.prm_rk)

    def __setattr__(self, key, value):
        if key in strategy_kwargs_list.get_kwargs_list():
            value = self._process_parameter(value)
            event = eventFactory.createParameterChangeEvent(parameters={'name': key, 'value': str(value)})
            aqhandler.send(json.dumps(event.to_dict()), self.prm_rk)
        return super().__setattr__(key, value)

    def replace(self, size=None, price=None, order=None, **kwargs):
        """
        Reemplaza una orden
        :param size:
        :param price:
        :param order:
        :param kwargs:
        :return:
        """
        size = size if size is not None else order.size
        price = price if price is not None else order.price

        if size:
            return self.broker.replace(self, size=abs(size), price=price, order=order, **kwargs)

        return None

    def sendOrders(self, orders):
        """  Metodo legacy """
        self.send_orders(orders)

    def send_orders(self, orders):
        self.broker.send_bulk_new_orders(orders)

    @staticmethod
    def _process_parameter(value):
        if isinstance(value, bool) or isinstance(value, float) or isinstance(value, int):
            _value = value
        elif isinstance(value, date):
            _value = value
        elif value == 'True':
            _value = True
        elif value == 'False' or value == 'false':
            _value = False
        else:
            try:
                if "." in value:
                    _value = float(value)
                else:
                    _value = int(value)
            except ValueError:
                _value = value

        return _value

    def alert(self, title=None, body=None):
        event = eventFactory.createPushNotificationsEvent(title=title, body=body)
        aqhandler.send(json.dumps(event.to_dict()), self.prm_rk)

    def mass_cancel(self, routingKey=None, segment=None, account=None):
        self.broker.mass_cancel(routing_key=routingKey, account=account, segment=segment)

    def _notify(self, qorders=[], qtrades=[]):
        try:
            super()._notify(qorders=qorders, qtrades=qtrades)
        except Exception as e:
            self.on_error()
            raise e
