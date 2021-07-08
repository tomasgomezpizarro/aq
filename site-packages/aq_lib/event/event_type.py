from enum import Enum


class EventType(Enum):
    MARKET_DATA_REQUEST_EVENT = 'market_data_request_event'
    MARKET_DATA_SNAPSHOT_FULL_REFRESH_EVENT = 'market_data_snapshot_full_refresh_event'
    EXECUTION_REPORT_EVENT = 'execution_report_event'
    NEW_ORDER_EVENT = 'new_order_event'
    ORDER_CANCEL_REQUEST_EVENT = 'order_cancel_request_event'
    ORDER_MASS_CANCEL_REQUEST_EVENT = 'order_mass_cancel_request_event'
    ORDER_CANCEL_REPLACE_REQUEST_EVENT = 'order_cancel_replace_request_event'
    BUILD_STRATEGY_ERROR_EVENT = 'build_strategy_error_event'
    LT_ORDER_UPDATED_EVENT = 'live_trading_order_updated_event'
    LOGS_EVENT = 'logs_event'
    PARAMETER_CHANGE_EVENT = 'parameter_change_event'
    MANUAL_ORDER_CANCEL_REQUEST_EVENT = 'manual_order_cancel_request_event'
    TRADE_EXECUTED_EVENT = 'trade_executed_event'
    LIVE_TRADING_STARTED_EVENT = 'live_trading_started_event'
    BULK_NEW_ORDER_EVENT = 'bulk_new_order_event'
    PUSH_NOTIFICATION_EVENT = 'push_notification_event'
    POSITION_UPDATED_EVENT = 'position_updated_event'
    STRATEGY_STARTED_EVENT = 'strategy_started_event'
    STRATEGY_STATUS_CHANGE = 'strategy_status_change'
    ORDER_STATUS_REQUEST_EVENT = 'order_status_request_event'
