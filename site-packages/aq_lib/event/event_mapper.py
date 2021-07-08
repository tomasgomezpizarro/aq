from aq_lib.event.event import *


class EventMapper(dict):
    def __init__(self, *args, **kwargs):

        super(dict, self).__init__(*args, **kwargs)

        self[EventType.MARKET_DATA_REQUEST_EVENT.value] = MarketDataRequestEvent
        self[EventType.MARKET_DATA_SNAPSHOT_FULL_REFRESH_EVENT.value] = MarketDataSnapshotFullRefreshEvent
        self[EventType.EXECUTION_REPORT_EVENT.value] = ExecutionReportEvent
        self[EventType.NEW_ORDER_EVENT.value] = NewOrderEvent
        self[EventType.ORDER_CANCEL_REQUEST_EVENT.value] = OrderCancelRequestEvent
        self[EventType.ORDER_MASS_CANCEL_REQUEST_EVENT.value] = OrderMassCancelRequestEvent
        self[EventType.ORDER_CANCEL_REPLACE_REQUEST_EVENT.value] = OrderCancelReplaceRequestEvent
        self[EventType.LT_ORDER_UPDATED_EVENT.value] = OrderUpdatedEvent
        self[EventType.LOGS_EVENT.value] = LogsEvent
        self[EventType.PARAMETER_CHANGE_EVENT.value] = ParameterChangeEvent
        self[EventType.MANUAL_ORDER_CANCEL_REQUEST_EVENT.value] = ManualOrderCancelRequestEvent
        self[EventType.TRADE_EXECUTED_EVENT.value] = TradeExecutedEvent
        self[EventType.BUILD_STRATEGY_ERROR_EVENT.value] = BuildStrategyErrorEvent
        self[EventType.LIVE_TRADING_STARTED_EVENT.value] = LiveTradingStartedEvent
        self[EventType.BULK_NEW_ORDER_EVENT.value] = BulkNewOrderEvent
        self[EventType.PUSH_NOTIFICATION_EVENT.value] = PushNotificationEvent
        self[EventType.POSITION_UPDATED_EVENT.value] = PositionUpdatedEvent
        self[EventType.STRATEGY_STARTED_EVENT.value] = StrategyStartedEvent
        self[EventType.STRATEGY_STATUS_CHANGE.value] = StrategyStatusChangeEvent
        self[EventType.ORDER_STATUS_REQUEST_EVENT.value] = OrderStatusRequestEvent


