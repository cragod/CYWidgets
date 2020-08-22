import time

from abc import ABC, abstractmethod
from ..exchange.order import *
from ..exchange.provider import CCXTProvider, ExchangeType
from ..logger.trading import TraderLogger


class ExchangeOrderExecutorFactory:
    """用来创建具体的订单执行对象"""
    @staticmethod
    def executor(ccxt_provider: CCXTProvider, order: Order, logger: TraderLogger):
        if ccxt_provider.exchange_type == ExchangeType.Okex:
            return OkexExchangeOrderExecutor(ccxt_provider, order, logger)
        elif ccxt_provider.exchange_type == ExchangeType.Binance:
            return BinanceExchangeOrderExecutor(ccxt_provider, order, logger)
        # TODO: other exchange
        return None


class BaseExchangeOrderExecutor(ABC):
    """现货订单执行抽象基类
    1. 获取余额信息
    2. 下单
    """

    def __init__(self, ccxt_provider: CCXTProvider, order: Order, logger: TraderLogger):
        # API 对象都用外部传入
        self._ccxt_provider = ccxt_provider
        self._order = order
        self._logger = logger

    # MARK: Protect

    def _create_order(self, amount, price=None, params={}):
        """最终统一的下单入口

        Parameters
        ----------
        amount : double
            下单数量
        price : double, optional
            价格 by default None
        params : dict, optional
            额外参数, by default {}

        Returns
        -------
        dict
            订单信息

        Raises
        ------
        NotImplementedError
            不支持的订单类型
        """
        symbol = self._order.coin_pair.formatted()
        order_info = None
        # Limit
        if self._order.type == OrderType.LIMIT:
            # Buy
            if self._order.side == OrderSide.BUY:
                order_info = self._ccxt_provider.ccxt_object_for_order.create_limit_buy_order(
                    symbol, amount, price, params)
            # Sell
            elif self._order.side == OrderSide.SELL:
                order_info = self._ccxt_provider.ccxt_object_for_order.create_limit_sell_order(
                    symbol, amount, price, params)
        # Market
        elif self._order.type == OrderType.MARKET:
            # Buy
            if self._order.side == OrderSide.BUY:
                order_info = self._ccxt_provider.ccxt_object_for_order.create_market_buy_order(symbol, amount, params)
            # Sell
            elif self._order.side == OrderSide.SELL:
                order_info = self._ccxt_provider.ccxt_object_for_order.create_market_sell_order(symbol, amount, params)
        # Others
        else:
            raise NotImplementedError('Not Supported Order Type')

        return order_info

    # MARK: Private

    def _track_order(self, order_info, interval=1, fetch_times=5):
        """监控订单，直到交易完成或尝试次数满时取消"""
        try:
            order_id = order_info['id']
            # 完全成交，返回
            if Order.all_filled(order_info):
                return order_info
            # 检查
            while fetch_times > 0:
                fetch_times -= 1
                # 等待下次检查
                time.sleep(interval)
                self._logger.log_phase_info('Track Order[Processing]', 'Remaining: {}'.format(order_info['remaining']))
                # 抓取订单信息
                order_info = self._ccxt_provider.ccxt_object_for_order.fetch_order(
                    order_id, self._order.coin_pair.formatted())
                self._logger.log_phase_info('Track Order[Processing]', 'Fetched OrderInfo: {}'.format(order_info))
                # 完全成交则结束
                if Order.all_filled(order_info):
                    return order_info
            # 最终没有完全成交，取消订单
            return self._ccxt_provider.ccxt_object_for_order.cancel_order(order_id, self._order.coin_pair.formatted())
        except Exception:
            self._logger.log_exception('Track Order')
            return None

    def _buying_order(self, retry_times=3):
        """下单买入，计算一个买入总量，循环下单，直到剩余需要买入的量低于最小下单量为止"""
        minimum_cost = self.fetch_min_cost()
        ask_price, bid_price = self.fetch_first_ticker()
        remaining_base_coin_to_cost = self._order.base_coin_amount * self._order.leverage
        if remaining_base_coin_to_cost < minimum_cost:
            # 不够交易
            self._logger.log_phase_info('Buying Signal', '{}({}) not enough to cost.'.format(
                self._order.coin_pair.base_coin, self._order.base_coin_amount))
            return None
        # 如果目标币已经超出最小交易额，视为已经持仓，不买入
        if bid_price * self._order.trade_coin_amount * self._order.coin_pair.estimated_value_of_base_coin > minimum_cost:
            self._logger.log_phase_info('Buying Signal', 'Already hold {}({})'.format(
                self._order.coin_pair.trade_coin, self.trade_coin_amount))
            return None

        # 正式下单流程
        order_infos = []  # 结果订单数组
        self._logger.log_phase_info("Order Check", "to_cost: {} minimum_cost: {} retry_times: {}".format(
            remaining_base_coin_to_cost, minimum_cost, retry_times))
        # 剩余待下单的量大于最小交易量
        while remaining_base_coin_to_cost > minimum_cost and retry_times >= 0:
            try:
                ask_price, _ = self.fetch_first_ticker()
                # 比卖一价高一点的价格下单
                bid_order_price = ask_price * self._order.bid_order_price_coefficient
                # 买单数量
                bid_order_amount = remaining_base_coin_to_cost / bid_order_price
                # 下单
                order_info = self._create_order(bid_order_amount, bid_order_price)
                self._logger.log_phase_info('Place Order', order_info)
                # 追踪订单
                order_info = self._track_order(order_info)
                self._logger.log_phase_info('Track Order[Finished]', order_info)
            except Exception:
                # 报错，直接等待进入下一次尝试
                self._logger.log_exception('Place Order')
                retry_times -= 1
                time.sleep(5)
                continue
            # 获取已经花费的数量
            cost_amount = Order.fetch_cost_amount(order_info)
            if cost_amount > 0:
                # 计算剩余需要交易的数量
                remaining_base_coin_to_cost = remaining_base_coin_to_cost - cost_amount
                # 当前订单结束，追加到结果列表
                order_infos.append(order_info)
                self._logger.log_phase_info('Remaining Amount [BUY]', remaining_base_coin_to_cost)
            else:
                # 没有花费的订单算失败了
                self._logger.log_phase_info('Order Timeout', '{}'.format(retry_times))
                retry_times -= 1
            # 等待下一次尝试
            time.sleep(1.5)
        # 整合所有订单
        result_order = self._order.integrate_orders(order_infos, self.fetch_first_ticker)
        return result_order

    def _selling_order(self, retry_times=3, leverage=1):
        """下单卖出，循环直到剩余的数量无法交易，这里用传入的 leverage 而不用 order 里的，
        是因为这个接口可用于做空的卖出，也可以作为平仓的卖出，这两种情况的 leverage 意义不一样"""
        minimum_cost = self.fetch_min_cost()
        _, bid_price = self.fetch_first_ticker()
        trade_coin_amount_to_sell = self._order.trade_coin_amount
        # 使用买一价模拟总成交价，低于最小交易值，无法下单
        if bid_price * trade_coin_amount_to_sell < minimum_cost:
            self._logger.log_phase_info('Close Signal', '{}({}) amount is too little to sell'.format(
                self._order.coin_pair.trade_coin, self._order.trade_coin_amount))
            return None

        # 正式交易流程
        order_infos = []  # 订单列表
        # 尝试下单
        while bid_price * trade_coin_amount_to_sell > minimum_cost and retry_times >= 0:
            try:
                _, bid_price = self.fetch_first_ticker()
                # 用比买一价更低的价格下单
                ask_order_price = bid_price * self._order.ask_order_price_coefficient
                # 创建订单
                order_info = self._create_order(trade_coin_amount_to_sell, ask_order_price)
                self._logger.log_phase_info('Place Order', order_info)
                # 追踪订单
                order_info = self._track_order(order_info)
                self._logger.log_phase_info('Track Order', order_info)
            except Exception:
                # 出错，等待后继续尝试
                self._logger.log_exception('Place Order')
                retry_times -= 1
                time.sleep(5)
                continue
            # 检查剩余需要买的数量
            remaining = order_info['remaining'] if order_info is not None else 0
            if math.isclose(remaining, 0):
                trade_coin_amount_to_sell = remaining
                # 添加到结果
                order_infos.append(order_info)
                self._logger.log_phase_info('Remaining Amount [SELL]', trade_coin_amount_to_sell)
            else:
                self._logger.log_phase_info('Order Timeout', '{}'.format(retry_times))
                retry_times -= 1
            # 等待1.5后继续
            time.sleep(1.5)
        # 整合订单
        order_info = self._order.integrate_orders(order_infos, self.fetch_first_ticker)
        return order_info

    # MARK: Public

    def fetch_first_ticker(self):
        """获取当前盘口价格 (ASK, BID)"""
        ticker = self._ccxt_provider.ccxt_object_for_query.fetch_ticker(self._order.coin_pair.formatted())
        return (ticker['ask'], ticker['bid'])

    def fetch_min_order_amount(self):
        """获取最小交易数量"""
        return self._ccxt_provider.ccxt_object_for_query.load_markets()[self._order.coin_pair.formatted()]['limits']['amount']['min']

    def fetch_min_cost(self):
        """获取最小交易金额"""
        if self._order.coin_pair.custom_min_cost is not None:
            # 优先取自定义的最小下单金额
            return self._order.coin_pair.custom_min_cost
        return self._ccxt_provider.ccxt_object_for_query.load_markets()[self._order.coin_pair.formatted()]['limits']['cost']['min']

    @abstractmethod
    def handle_long_order_request(self):
        pass

    @abstractmethod
    def handle_short_order_request(self):
        pass

    @abstractmethod
    def handle_close_order_request(self):
        pass


class OkexExchangeOrderExecutor(BaseExchangeOrderExecutor):
    """OK 现货下单逻辑"""

    def handle_long_order_request(self):
        return self._buying_order(retry_times=0)

    def handle_close_order_request(self):
        return super().handle_close_order_request()

    def handle_short_order_request(self):
        return super().handle_short_order_request()


class BinanceExchangeOrderExecutor(BaseExchangeOrderExecutor):
    """Binance 现货下单逻辑"""

    def handle_long_order_request(self):
        return self._buying_order(retry_times=0)

    def handle_close_order_request(self):
        return self._selling_order(retry_times=0)

    def handle_short_order_request(self):
        return super().handle_short_order_request()
