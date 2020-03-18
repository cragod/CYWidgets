import time
from .order_exec import *
from ..logger.trading import TraderLogger
from ..exchange.provider import CCXTProvider
from ..exchange.order import *


class ExchangeTrader:
    """现货交易的统一下单流程"""

    def __init__(self, ccxt_provider: CCXTProvider, order: Order, logger: TraderLogger):
        self.__ccxt_provider = ccxt_provider
        self.__order = order
        self.__logger = logger
        self.__order_executor = eval(order.exchange_name + 'ExchangeOrderExecutor')(ccxt_provider, order)

    # Public Functions

    def handle_signal_to_order(self, signal):
        """handle signals:
        1: Buy
        -1: Sell
        0: Close
        return:
            - None: When Exception Occurs
            - Formatted Order: Success"""
        retry_duration = 1
        if self.debug:
            self._log_procedure('Place Order', """Won't place order, signal: {}""".format(signal))
            return None
        # retry 5 times
        for _ in range(1):
            try:
                if not isinstance(signal, int):
                    return None
                return self._dispatch_signal(signal)
            except Exception:
                self._log_exception('Order')
                time.sleep(retry_duration)
                retry_duration += 2
