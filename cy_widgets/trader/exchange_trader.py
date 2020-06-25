from .exchange_order_exec import *
from ..logger.trading import TraderLogger
from ..exchange.provider import CCXTProvider
from ..exchange.order import *
from ..exchange.signal import ExchangeSignal


class ExchangeTrader:
    """现货交易的统一下单流程"""
    __order_executor: BaseExchangeOrderExecutor

    def __init__(self, ccxt_provider: CCXTProvider, order: Order, logger: TraderLogger):
        assert ccxt_provider is not None
        assert order is not None

        self.__ccxt_provider = ccxt_provider
        self.__order = order
        self.__logger = logger
        self.__order_executor = ExchangeOrderExecutorFactory.executor(ccxt_provider.exchange_type)

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
            self.__order.append_order(order)
        if order_1 is not None:
            self.__order.append_order(order_1)
        # 这里交易流程结束，过程中大部分中间数据都在 Order 里了
        return self.__order
