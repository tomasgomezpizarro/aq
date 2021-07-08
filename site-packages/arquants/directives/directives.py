import traceback

from aq_lib.event.event_type import EventType
from aq_lib.model.strategy_status import StrategyStatus

from arquants.definitions import aqhandler
from arquants import logger


class Directives:
    def __init__(self):
        aqhandler.register_callback(EventType.STRATEGY_STATUS_CHANGE.value, self.process_stop)
        aqhandler.register_callback(EventType.MANUAL_ORDER_CANCEL_REQUEST_EVENT.value, self.manual_cancel)

    def process_stop(self, evt):
        try:
            if evt.status == StrategyStatus.FINISHED.value:
                self.cerebro.runstop()
            else:
                self.cerebro.runningstrats[0].process_status_update(evt)
        except:
            just_the_string = traceback.format_exc()
            logger.debug(just_the_string)
            logger.exception("hubo un error al cancelar")

    def manual_cancel(self, evt):
        self.cerebro.broker.manual_cancel(evt.order_id)


directives = Directives()
