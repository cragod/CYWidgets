import time
from .exchange_order_exec import *
from ..logger.trading import TraderLogger
from ..exchange.provider import CCXTProvider
from ..exchange.order import *
from ..exchange.signal import ExchangeSignal


class ExchangeTrader:
    """现货交易的统一下单流程"""
    __order_executor: BaseExchangeOrderExecutor

    def __init__(self, ccxt_provider: CCXTProvider, order: Order, logger: TraderLogger):
        self.__ccxt_provider = ccxt_provider
        self.__order = order
        self.__logger = logger
        self.__order_executor = eval(order.exchange_name + 'ExchangeOrderExecutor')(ccxt_provider, order)

    def fetch_and_hold_balance(self):
        """获取余额，相关信息保存到 Order 中"""
        self.__order_executor.fetch_balance()

    def place_order_with_signal(self, signal: ExchangeSignal):
        """解析信号进行下单"""
        if signal == ExchangeSignal.LONG:
            order = self.__order_executor.handle_long_order_request()
        elif signal == ExchangeSignal.SHORT:
            order = self.__order_executor.handle_short_order_request()
        elif signal == ExchangeSignal.CLOSE:
            order = self.__order_executor.handle_close_order_request()
        elif signal == ExchangeSignal.CLOSE_THEN_LONG:
            order = self.__order_executor.handle_close_order_request()
            order_1 = self.__order_executor.handle_long_order_request()
        elif signal == ExchangeSignal.CLOSE_THEN_SHORT:
            order = self.__order_executor.handle_close_order_request()
            order_1 = self.__order_executor.handle_short_order_request()
        else:
            self.__logger.log_phase_info("Analysis Signal", "Invalid signal({})".format(signal.value))
        # 保存结果到 Order 里
        if order is not None:
            self.__order.result_orders.append(order)
        if order_1 is not None:
            self.__order.result_orders.append(order_1)
        # 这里交易流程结束，过程中大部分中间数据都在 Order 里了
        return self.__order