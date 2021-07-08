from datetime import datetime

from aq_lib.event.event_type import EventType
from aq_lib.model.md_entry import MDEntry
from aq_lib.transport.serializable import Serializable


class Event(Serializable):
    def __init__(self, topic):
        self.topic = topic


# Event sent by cerebro to subscribe to MD
class MarketDataRequestEvent(Event):
    def __init__(self, topic=EventType.MARKET_DATA_REQUEST_EVENT.value, on_behalf_of_comp_id=None,
                 deliver_to_comp_id=None, symbols=None, md_req_id=None, subscription_request_type=None,
                 settlement_type=None, security_type=None, settlement_date=None, security_id=None, md_type=None,
                 routing_key=None, currency=None, settlement=None, market=None, description=None, execution_id=None,
                 negotiation_agent_id=None, trading_session=None, maturity_date=None, **kwargs):
        Event.__init__(self, topic)
        self.on_behalf_of_comp_id = on_behalf_of_comp_id
        self.deliver_to_comp_id = deliver_to_comp_id
        self.symbols = symbols
        self.md_req_id = md_req_id
        self.subscription_request_type = subscription_request_type
        self.routing_key = routing_key
        self.md_type = md_type
        self.settlement = settlement
        self.currency = currency
        self.security_id = security_id
        self.security_type = security_type
        self.settlement_date = settlement_date
        self.market = market
        self.description = description
        self.execution_id = execution_id
        self.negotiation_agent_id = negotiation_agent_id
        self.trading_session = trading_session
        self.maturity_date = maturity_date


class MarketDataSnapshotFullRefreshEvent(Event):
    def __init__(self, topic=EventType.MARKET_DATA_SNAPSHOT_FULL_REFRESH_EVENT.value, mdReqId=None, symbol=None,
                 md_type=None, securityId=None, securityType=None, settlType=None, settlDate=None, currency=None,
                 trading_session=None, entries=[], timestamp=None, negotiation_agent=None, **kwargs):
        Event.__init__(self, topic)
        self.mdReqId = mdReqId
        self.symbol = symbol
        self.md_type = md_type
        self.securityId = securityId
        self.securityType = securityType
        self.settlType = settlType
        self.settlDate = settlDate
        self.entries = self.build_entries(entries)
        self.timestamp = timestamp
        self.negotiation_agent = negotiation_agent
        self.trading_session = trading_session
        self.currency = currency

    @staticmethod
    def build_entries(new_entries):
        entries = []
        for entry in new_entries:
            entries.append(MDEntry(**entry))
        return entries

    def get_entry(self, entry_type):
        for entry in self.entries:
            if entry.entry_type == entry_type:
                return entry
        return None

    def addEntry(self, entry):
        self.entries.append(entry)

    def setEntries(self, entries):
        self.entries = entries


class ExecutionReportEvent(Event):
    def __init__(self, topic=EventType.EXECUTION_REPORT_EVENT.value, securityExchange=None, lastQty=None,
                 lastPrice=None, leavesQty=None, execType=None, settlType=None, transactTime=None, timeInForce=None,
                 text=None, symbol=None, side=None, price=None, avgPx=None, orderType=None, orderStatus=None,
                 orderQty=None, orderID=None, execID=None, settlement_date=None, currency=None, cumQty=None,
                 originalClientOrderId=None, account=None, clientOrderID=None, **kwargs):
        Event.__init__(self, topic)
        self.securityExchange = securityExchange
        self.lastQty = lastQty
        self.lastPrice = lastPrice
        self.leavesQty = leavesQty
        self.execType = execType
        self.settlType = settlType
        self.settlement_date = settlement_date
        self.transactTime = transactTime
        self.timeInForce = timeInForce
        self.text = text
        self.symbol = symbol
        self.side = side
        self.price = price
        self.avgPx = avgPx
        self.orderType = orderType
        self.orderStatus = orderStatus
        self.orderQty = orderQty
        self.orderID = orderID
        self.execID = execID
        self.currency = currency
        self.cumQty = cumQty
        self.originalClientOrderId = originalClientOrderId
        self.account = account
        self.clientOrderID = clientOrderID


class NewOrderEvent(Event):
    def __init__(self, topic=EventType.NEW_ORDER_EVENT.value, account=None, cl_ord_id=None, order_qty=None,
                 order_type=None, price=None, side=None, symbol=None, time_in_force=None, currency=None,
                 routing_key=None, exec_inst=None, security_type=None, settlement_type=None, settlement_date=None,
                 execution_id=None, negotiation_agent_id=None, min_qty=None, short_sell=None, **kwargs):
        Event.__init__(self, topic)
        self.account = account
        self.cl_ord_id = cl_ord_id
        self.order_qty = order_qty
        self.order_type = order_type
        self.price = price
        self.side = side
        self.symbol = symbol
        self.time_in_force = time_in_force
        self.routing_key = routing_key
        self.exec_inst = exec_inst
        self.currency = currency
        self.security_type = security_type
        self.settlement_type = settlement_type
        self.settlement_date = settlement_date
        self.execution_id = execution_id
        self.negotiation_agent_id = negotiation_agent_id
        self.min_qty = min_qty
        self.short_sell = short_sell


class OrderCancelReplaceRequestEvent(Event):
    def __init__(self, topic=EventType.ORDER_CANCEL_REPLACE_REQUEST_EVENT.value, account=None, cl_ord_id=None,
                 order_qty=None, order_type=None, orig_client_ord_id=None, orig_order_id=None, price=None, side=None,
                 symbol=None, time_in_force=None, security_type=None, settlement_type=None, currency=None,
                 settlement_date=None, routing_key=None, execution_id=None, negotiation_agent_id=None, short_sell=None,
                 **kwargs):
        Event.__init__(self, topic)
        self.account = account
        self.cl_ord_id = cl_ord_id
        self.order_qty = order_qty
        self.order_type = order_type
        self.orig_client_ord_id = orig_client_ord_id
        self.orig_order_id = orig_order_id
        self.price = price
        self.side = side
        self.symbol = symbol
        self.time_in_force = time_in_force
        self.order_type = order_type
        self.settlement_date = settlement_date
        self.security_type = security_type
        self.settlement_type = settlement_type
        self.routing_key = routing_key
        self.currency = currency
        self.execution_id = execution_id
        self.negotiation_agent_id = negotiation_agent_id
        self.short_sell = short_sell


class BulkNewOrderEvent(Event):
    def __init__(self, topic=EventType.BULK_NEW_ORDER_EVENT.value, orders=[]):
        Event.__init__(self, topic)
        self.orders = orders


class OrderCancelRequestEvent(Event):
    def __init__(self, topic=EventType.ORDER_CANCEL_REQUEST_EVENT.value, account=None, cl_ord_id=None, order_id=None,
                 order_qty=None, orig_cl_ord_id=None, side=None, symbol=None, security_type=None,
                 settlement_type=None, currency=None, settlement_date=None, routing_key=None, execution_id=None,
                 negotiation_agent_id=None, short_sell=None, **kwargs):
        Event.__init__(self, topic)
        self.account = account
        self.cl_ord_id = cl_ord_id
        self.order_id = order_id
        self.order_qty = order_qty
        self.orig_cl_ord_id = orig_cl_ord_id
        self.side = side
        self.symbol = symbol
        self.routing_key = routing_key
        self.security_type = security_type
        self.settlement_type = settlement_type
        self.settlement_date = settlement_date
        self.currency = currency
        self.execution_id = execution_id
        self.negotiation_agent_id = negotiation_agent_id
        self.short_sell = short_sell


class OrderMassCancelRequestEvent(Event):
    def __init__(self, topic=EventType.ORDER_MASS_CANCEL_REQUEST_EVENT.value, account=None, segment=None,
                 clientOrderId=None, **kwargs):
        Event.__init__(self, topic)
        self.account = account
        self.segment = segment
        self.clientOrderId = clientOrderId


class PositionUpdatedEvent(Event):
    def __init__(self, topic=EventType.POSITION_UPDATED_EVENT.value, symbol=None, account=None, size=None, price=None,
                 open_result=None, close_result=None, strategy_id=None, strategy_version=None, execution_id=None,
                 arquants_user=None, execution_type=None):
        Event.__init__(self, topic)
        self.symbol = symbol
        self.account = account
        self.size = size
        self.price = price
        self.open_result = open_result
        self.close_result = close_result
        self.strategy_id = strategy_id
        self.strategy_version = strategy_version
        self.execution_id = execution_id
        self.arquants_user = arquants_user
        self.execution_type = execution_type


class OrderUpdatedEvent(Event):
    def __init__(self, topic=EventType.LT_ORDER_UPDATED_EVENT.value, strategy_id=None, strategy_version=None,
                 execution_id=None, arquants_user=None, execution_type=None, account=None, symbol=None, side=None,
                 orderStatus=None, price=None, orderQty=None, cumQty=None, orderID=None, clientOrderID=None,
                 transactTime=None, ers=[], timeInForce=None, text=None, securityExchange=None, leavesQty=None,
                 execType=None, execID=None, **kwargs):
        Event.__init__(self, topic)
        self.strategy_id = strategy_id
        self.strategy_version = strategy_version
        self.execution_id = execution_id
        self.arquants_user = arquants_user
        self.execution_type = execution_type
        self.account = account
        self.symbol = symbol
        self.side = side
        self.orderStatus = orderStatus
        self.price = price
        self.orderQty = orderQty
        self.cumQty = cumQty
        self.orderID = orderID
        self.clientOrderID = clientOrderID
        self.transactTime = transactTime
        self.execID = execID
        self.execType = execType
        self.leavesQty = leavesQty
        self.securityExchange = securityExchange
        self.text = text
        self.timeInForce = timeInForce
        self.ers = ers
        self.time = str(datetime.now())


class LogsEvent(Event):
    def __init__(self, topic=EventType.LOGS_EVENT.value, arquants_user=None, execution_id=None, execution_type=None,
                 strategy_id=None, log_line=None, **kwargs):
        Event.__init__(self, topic)
        self.arquants_user = arquants_user
        self.execution_id = execution_id
        self.execution_type = execution_type
        self.strategy_id = strategy_id
        self.log_line = log_line


class ParameterChangeEvent(Event):
    def __init__(self, topic=EventType.PARAMETER_CHANGE_EVENT.value, parameters=dict(), execution_id=None,
                 arquants_user=None, strategy_id=None, strategy_version=None, execution_type=None, from_api=False,
                 **kwargs):
        Event.__init__(self, topic)
        self.parameters = parameters
        self.execution_id = execution_id
        self.arquants_user = arquants_user
        self.strategy_id = strategy_id
        self.strategy_version = strategy_version
        self.execution_type = execution_type
        self.from_api = from_api


class ManualOrderCancelRequestEvent(Event):
    def __init__(self, topic=EventType.MANUAL_ORDER_CANCEL_REQUEST_EVENT.value, order_id=None, **kwargs):
        Event.__init__(self, topic)
        self.order_id = order_id


class TradeExecutedEvent(Event):
    def __init__(self, topic=EventType.TRADE_EXECUTED_EVENT.value, strategy_id=None, strategy_version=None,
                 execution_id=None, arquants_user=None, execution_type=None, ts=None, symbol=None, side=None,
                 size=None, price=None, account=None, orderid=None, tradeid=None, settlType=None, **kwargs):
        Event.__init__(self, topic)

        self.strategy_id = strategy_id
        self.strategy_version = strategy_version
        self.execution_id = execution_id
        self.arquants_user = arquants_user
        self.execution_type = execution_type
        self.ts = ts
        self.symbol = symbol
        self.side = side
        self.size = size
        self.price = price
        self.account = account
        self.orderid = orderid
        self.tradeid = tradeid
        self.settlType = settlType


class BuildStrategyErrorEvent(Event):
    def __init__(self, topic=EventType.BUILD_STRATEGY_ERROR_EVENT.value, execution_id=None, arquants_user=None,
                 strategy_id=None, strategy_version=None, execution_type=None, error=None, **kwargs):
        Event.__init__(self, topic)
        self.execution_id = execution_id
        self.arquants_user = arquants_user
        self.strategy_id = strategy_id
        self.strategy_version = strategy_version
        self.execution_type = execution_type
        self.error = error


class LiveTradingStartedEvent(Event):
    def __init__(self, topic=EventType.LIVE_TRADING_STARTED_EVENT.value, execution_id=None, arquants_user=None,
                 status=None, **kwargs):
        Event.__init__(self, topic)
        self.execution_id = execution_id
        self.arquants_user = arquants_user
        self.status = status

class OrderStatusRequestEvent(Event):
    def __init__(self, topic=EventType.ORDER_STATUS_REQUEST_EVENT.value, client_order_id=None, side=None, **kwargs):
        Event.__init__(self, topic)
        self.client_order_id = client_order_id
        self.side = side

class PushNotificationEvent(Event):
    def __init__(self, topic=EventType.PUSH_NOTIFICATION_EVENT.value, title=None, body=None, user=None, device=None,
                 execution_id=None, strategy_id=None):
        Event.__init__(self, topic)
        self.title = title
        self.body = body
        self.user = user
        self.device = device
        self.execution_id = execution_id
        self.strategy_id = strategy_id


class StrategyStartedEvent(Event):
    """
    Evento para el risk module

    """

    def __init__(self, topic=EventType.STRATEGY_STARTED_EVENT.value,
                 execution_id=None,
                 strategy_version=None,
                 rate_limit_orders=10,
                 rate_limit_logs=5,
                 tickers=None,
                 user=None,
                 routing_key=None,
                 type=None,
                 strategy_id=None
                 ):
        Event.__init__(self, topic)
        self.strategy_version = strategy_version
        self.user = user
        self.tickers = tickers
        self.rate_limit_logs = rate_limit_logs
        self.rate_limit_orders = rate_limit_orders
        self.execution_id = execution_id
        self.routing_key = routing_key
        self.type = type
        self.strategy_id = strategy_id


class StrategyStatusChangeEvent(Event):
    """
    Evento enviado por las estrategias, y tambien por la API.

    El evento tiene un significado distinto en cada uno de estos dos casos:
    -Cuando el evento es enviado por la estrategia, se usa para notificar que hubo un cambio de estado en la estrategia.
    -Cuando el evento es enviado por la API, se usa para ordenarle a una estrategia en particular que cambie su estado.

    Posibles valores de estado:
     - ACTIVE
     - STOPPED
     - FINISHED
     - ERROR

    """

    def __init__(self, topic=EventType.STRATEGY_STATUS_CHANGE.value, status=None, from_api=None, execution_id=None):
        Event.__init__(self, topic)
        self.status = status
        self.from_api = from_api
        self.execution_id = execution_id
