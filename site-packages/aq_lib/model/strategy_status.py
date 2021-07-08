from enum import Enum


class StrategyStatus(Enum):

    """
    Esto esta duplicado en el modelo de la API. LiveTrading.

    """
    ACTIVE = 'active'
    PAUSED = 'paused'
    FINISHED = 'finished'
    ERROR = 'error'
    INITIALIZING = 'initializing'
    DELETED = 'deleted'
