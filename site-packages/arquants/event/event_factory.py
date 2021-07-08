import math
import uuid

from aq_lib.event.event import *
from aq_lib.event.event_type import EventType
from aq_lib.model.strategy_status import StrategyStatus

from arquants.brokers.bt_order_status_translator import btstatustranslator


class EventFactory:
    def __init__(self, strategy_id=None, strategy_version=None, execution_id=None, arquants_user=None,
                 execution_type=None, routing_key=None):
        self.strategy_id = strategy_id
        self.strategy_version = strategy_version
        self.execution_id = execution_id
        self.arquants_user = arquants_user
        self.execution_type = execution_type
        self.routing_key = routing_key

    # Este evento se crea para backtest, y en live trading solo cuando se envia la orden al mercado
    def createOrderUpdatedEventFromOrder(self, order):
        event = OrderUpdatedEvent(strategy_id=self.strategy_id,
                                  strategy_version=self.strategy_version,
                                  execution_id=self.execution_id,
                                  arquants_user=self.arquants_user,
                                  execution_type=self.execution_type)
        event.account = order.data.account if hasattr(order.data, 'account') else "BT001"
        event.symbol = order.data.tradecontract
        event.side = str(order.ordtype + 1)
        event.orderStatus = btstatustranslator.get(order.getstatusname(order.status), "")
        event.price = order.price if not math.isnan(order.price) else None
        event.orderQty = abs(order.size)
        event.cumQty = 0 if event.orderStatus != "2" else abs(order.size)
        # ref for backtesting, "none" for live trading because it's not defined yet
        event.orderID = order.serverOrderId if hasattr(order, 'serverOrderId') else None
        event.clientOrderID = order.m_orderId if hasattr(order, 'm_orderId') else None
        event.transactTime = str(datetime.now())
        return event

    def createBatchOrderUpdatedEvent(self, order_updated_events):
        order_updated_event = OrderUpdatedEvent(strategy_id=self.strategy_id, strategy_version=self.strategy_version,
                                                execution_id=self.execution_id, arquants_user=self.arquants_user,
                                                topic=EventType.LT_ORDER_UPDATED_EVENT.value,
                                                execution_type=self.execution_type)
        ers = list()
        for savedEvent in order_updated_events.values():
            event = dict()
            event["account"] = "BT001"
            event["symbol"] = savedEvent.symbol
            event["side"] = savedEvent.side
            event["orderStatus"] = savedEvent.orderStatus
            event["price"] = savedEvent.price if not math.isnan(savedEvent.price) else None
            event["orderQty"] = savedEvent.orderQty
            event["cumQty"] = savedEvent.cumQty
            event["orderID"] = savedEvent.orderID
            event["clientOrderID"] = savedEvent.clientOrderID
            event["transactTime"] = savedEvent.transactTime
            ers.append(event)
        order_updated_event.execreports = ers
        return order_updated_event

    def createOrderUpdatedEventFromExecReport(self, exec_report, order):
        event = OrderUpdatedEvent(strategy_id=self.strategy_id, strategy_version=self.strategy_version,
                                  execution_id=self.execution_id, arquants_user=self.arquants_user,
                                  topic=EventType.LT_ORDER_UPDATED_EVENT.value, execution_type=self.execution_type)

        event.account = exec_report.account if exec_report.account is not None else order.data.account
        event.clientOrderID = exec_report.originalClientOrderId if exec_report.originalClientOrderId is not None and exec_report.execType == '4' else exec_report.clientOrderID
        event.cumQty = exec_report.cumQty
        event.execID = exec_report.execID
        event.execType = exec_report.execType
        event.leavesQty = exec_report.leavesQty if exec_report.leavesQty is not None else abs(order.size)
        event.orderID = exec_report.orderID
        event.orderQty = exec_report.orderQty if exec_report.orderQty is not None else abs(order.size)
        event.orderStatus = exec_report.orderStatus
        event.price = exec_report.price if exec_report.price is not None and not math.isnan(exec_report.price) else order.price
        event.securityExchange = exec_report.securityExchange
        event.side = exec_report.side if exec_report.side is not None else str(order.ordtype + 1)
        event.symbol = exec_report.symbol if exec_report.symbol is not None else order.data.tradecontract
        event.text = exec_report.text
        event.timeInForce = exec_report.timeInForce
        event.transactTime = str(exec_report.transactTime)
        return event

    def createBulkNewOrderEvent(self, orders):
        event = BulkNewOrderEvent(EventType.BULK_NEW_ORDER_EVENT.value)
        _orders = []
        for order in orders:
            _orders.append(self.createNewOrderEventFromOrder(order))
        event.orders = _orders
        return event

    def createNewOrderEventFromOrder(self, order):
        event = NewOrderEvent(EventType.NEW_ORDER_EVENT.value)
        event.account = order.data.account
        event.cl_ord_id = order.m_orderId
        event.order_qty = order.abs_size
        event.order_type = str(order.exectype)
        event.price = order.price if order.price is not None and not math.isnan(order.price) else None
        event.side = str(order.ordtype + 1)
        event.symbol = order.data.tradecontract
        event.time_in_force = '0'
        event.routing_key = self.routing_key
        event.settlement_date = order.data.settlement_date
        event.settlement_type = str(order.data.settlement)
        event.security_type = order.data.security_type
        event.currency = order.data.currency
        event.execution_id = self.execution_id
        event.negotiation_agent_id = order.data.negotiation_agent_id
        # if hasattr(order, 'm_short_sell'):
        # event.short_sell = order.short_sell
        if hasattr(order, 'm_execinst'):
            event.exec_inst = order.m_execinst
        return event

    def createCancelOrderEventFromOrder(self, order):
        event = OrderCancelRequestEvent(EventType.ORDER_CANCEL_REQUEST_EVENT.value)
        event.account = order.data.account
        event.cl_ord_id = self.generate_id()
        event.order_id = order.m_serverOrderId  # En las replace no tiene el server_order_id
        event.order_qty = abs(order.size)
        event.orig_cl_ord_id = order.m_orderId
        event.side = str(order.ordtype + 1)
        event.symbol = order.data.tradecontract
        event.routing_key = self.routing_key
        event.settlement_date = order.data.settlement_date
        event.settlement_type = str(order.data.settlement)
        event.security_type = order.data.security_type
        event.currency = order.data.currency
        event.execution_id = self.execution_id
        event.negotiation_agent_id = order.data.negotiation_agent_id
        # if hasattr(order, 'm_short_sell'):
        # event.short_sell = order.short_sell
        return event

    def createMarketDataRequestEvent(self, contract=None, currency=None, md_request_id=None, data_type=None,
                                     settlement=None, settlement_date=None, security_type=None,
                                     negotiation_agent_id=None, market=None):
        return MarketDataRequestEvent(topic=EventType.MARKET_DATA_REQUEST_EVENT.value, symbols=[contract],
                                      md_req_id=md_request_id, currency=currency, routing_key=self.routing_key,
                                      settlement=str(settlement), settlement_date=settlement_date,
                                      security_type=security_type, md_type=data_type, execution_id=self.execution_id,
                                      negotiation_agent_id=negotiation_agent_id,market=market)

    def createExecutionErrorEvent(self, error):
        event = BuildStrategyErrorEvent(execution_id=self.execution_id, arquants_user=self.arquants_user,
                                        strategy_id=self.strategy_id, strategy_version=self.strategy_version,
                                        execution_type=self.execution_type)
        event.error = error
        return event

    # TODO este evento deberia boletearse
    #@staticmethod
    #def createInfoResponseEvent(connectors_rks):
        #    event = InfoResponseEvent()
        #event.connectors_rks = connectors_rks
        #return event

    def createLogsEvent(self, logline):
        event = LogsEvent(arquants_user=self.arquants_user, execution_id=self.execution_id,
                          execution_type=self.execution_type, strategy_id=self.strategy_id)
        event.log_line = logline
        return event

    def createPushNotificationsEvent(self, body=None, title=None):
        event = PushNotificationEvent(body=body, title=title, user=self.arquants_user, execution_id=self.execution_id,
                                      strategy_id=self.strategy_id)
        return event

    # def createLiveTradingEvent(self, datas_positions):
    #     event = LiveTradingEvent(strategy_id=self.strategy_id, strategy_version=self.strategy_version,
    #                              execution_id=self.execution_id, arquants_user=self.arquants_user,
    #                              execution_type=self.execution_type)
    #     event.positions = datas_positions
    #     total_open = 0
    #     total_close = 0
    #     for position in datas_positions.values():
    #         total_open += position.get('open_position')
    #         total_close += position.get('close_position')
    #     event.total_close = total_close
    #     event.total_open = total_open
    #     return event

    def createPositionUpdatedEvent(self, data):
        event = PositionUpdatedEvent(strategy_id=self.strategy_id, strategy_version=self.strategy_version,
                                     execution_id=self.execution_id, arquants_user=self.arquants_user,
                                     execution_type=self.execution_type)
        event.symbol = data["symbol"]
        event.size = data["size"]
        event.price = data["price"]
        event.open_result = data["open_position"]
        event.close_result = data["close_position"]

        return event

    @staticmethod
    def generate_id():
        cl_ord_id = uuid.uuid4().hex[:16]
        return cl_ord_id

    def createTradeExecutedEvent(self, execReport):
        event = TradeExecutedEvent(strategy_id=self.strategy_id, strategy_version=self.strategy_version,
                                   execution_id=self.execution_id, arquants_user=self.arquants_user,
                                   execution_type=self.execution_type)

        event.ts = execReport.transactTime
        event.symbol = execReport.symbol
        event.side = execReport.side
        event.size = execReport.lastQty
        event.price = execReport.lastPrice if not math.isnan(execReport.lastPrice) else None
        event.account = execReport.account
        event.orderid = execReport.orderID
        event.tradeid = execReport.execID
        event.settlType = str(execReport.settlType)
        return event

    def createParameterChangeEvent(self, parameters):
        event = ParameterChangeEvent(parameters=parameters)
        event.execution_id = self.execution_id
        event.arquants_user = self.arquants_user
        event.strategy_id = self.strategy_id
        event.strategy_version = self.strategy_version
        event.execution_type = self.execution_type
        event.from_api = False
        return event

    def createStrategyStatusChangeEvent(self, status: StrategyStatus):
        event = StrategyStatusChangeEvent(status=status, from_api=False, execution_id=self.execution_id)
        return event

    def createLiveTradingStartedEvent(self):
        return LiveTradingStartedEvent(execution_id=self.execution_id, arquants_user=self.arquants_user,
                                        status='active')

    def createOrderCancelReplaceEvent(self, order):
        event = OrderCancelReplaceRequestEvent()

        event.account = order.data.account
        event.cl_ord_id = order.m_orderId
        event.orig_order_id = order.m_originalOrderId
        event.order_qty = abs(order.size)
        event.order_type = str(order.exectype)
        event.orig_client_ord_id = order.m_originalClientOrderId
        event.price = order.price
        event.side = str(order.ordtype + 1)
        event.symbol = order.data.tradecontract
        event.time_in_force = '0'
        event.routing_key = self.routing_key

        event.settlement_type = str(order.data.settlement)
        event.settlement_date = order.data.settlement_date
        event.security_type = order.data.security_type
        event.currency = order.data.currency
        event.execution_id = self.execution_id
        event.negotiation_agent_id = order.data.negotiation_agent_id
        # if hasattr(order, 'm_short_sell'):
        # event.short_sell = order.short_sell
        return event

    # TODO revisar esto
    def createOrderMassCancelRequestEvent(self, segment=None, account=None, clientOrderId=None):
        return OrderMassCancelRequestEvent(topic=EventType.ORDER_MASS_CANCEL_REQUEST_EVENT.value, account=account,
                                           segment=segment, clientOrderId=clientOrderId)


eventFactory = EventFactory()
