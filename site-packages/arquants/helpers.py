import ast
import os


class StrategyKwargsKeys:
    def __init__(self):
        self.kwargs_list = list()
        self._populate_kwargs_list()

    def _populate_kwargs_list(self):
        _KWARGS_KEYS = list()
        if os.getenv('KWARGS', None):
            _KWARGS = ast.literal_eval(os.getenv('KWARGS', None))
            if _KWARGS is not None:
                for _kwarg in _KWARGS:
                    print(_kwarg)
                    _KWARGS_KEYS.append(_kwarg)
        self.kwargs_list = _KWARGS_KEYS

    def get_kwargs_list(self):
        return self.kwargs_list


strategy_kwargs_list = StrategyKwargsKeys()
