import time
from abc import ABC
from .provider import CCXTProvider
from .signal import ExchangeSignal
from cy_components.utils.coin_pair import CoinPair


class BaseExchangeBalance(ABC):
    """现货余额逻辑类
    1. 结合策略信号产生实际下单信号;
    2. 计算下单数量;
    """
    _base_coin_amount = 0.0
    _trade_coin_amount = 0.0

    def __init__(self, ccxt_provider: CCXTProvider, coin_pair: CoinPair):
        self._ccxt_provider = ccxt_provider
        self._coin_pair = coin_pair

    def fetch_balance(self, failed_logger):
        """请求当前币对的各自数量

        Parameters
        ----------
        failed_logger : func(phase_name: str)
            错误日志记录方法

        Returns
        -------
        (base_coin_amount, trade_coin_amount)
            币对对应的数量
        """
        failed_times = 0
        while True:
            try:
                balance = self._ccxt_provider.ccxt_object_for_order.fetch_balance()['free']
                self._base_coin_amount = float(balance[self._coin_pair.base_coin])
                self._trade_coin_amount = float(balance[self._coin_pair.trade_coin])
                # 记录余额
                return self._base_coin_amount, self._trade_coin_amount
            except Exception:
                failed_times += 1
                if failed_times % 5 == 0:  # 5 次记录一次
                    failed_logger('Fetch Balance')
                time.sleep(5)

    def formatted_balance_info(self):
        """格式化的余额数据"""
        return """
- {}: {}
- {}: {}""".format(self._coin_pair.trade_coin, self._trade_coin_amount,
                   self._coin_pair.base_coin, self._base_coin_amount)

    def process_signal(self, signal: ExchangeSignal):
        """根据仓位处理信号"""
        return signal
