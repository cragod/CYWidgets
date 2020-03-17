from abc import ABC
from cy_components.utils.one_token import OneToken

from ..exchange.order import *
from ..exchange.provider import CCXTProvider


class BaseOrderExecutor(ABC):
    """订单执行"""

    def __init__(self, ccxt_provider: CCXTProvider, order: Order):
        # API 对象都用外部传入
        self._ccxt_provider = ccxt_provider
        self._order = Order

    def _fetch_first_ticker(self):
        """获取当前盘口价格 (ASK, BID)"""
        ticker = self._ccxt_provider.ccxt_object_for_query.fetch_ticker(self._order.coin_pair.pair())
        return (ticker['ask'], ticker['bid'])

    def _fetch_min_order_amount(self):
        """获取最小交易数量"""
        return self._ccxt_provider.ccxt_object_for_query.load_markets()[self._order.coin_pair.pair()]['limits']['amount']['min']

    def _fetch_min_cost(self):
        """获取最小交易金额"""
        return self._ccxt_provider.ccxt_object_for_query.load_markets()[self._order.coin_pair.pair()]['limits']['cost']['min']
