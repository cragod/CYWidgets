import time
from functools import reduce
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

    def _place_buying_orders(self, base_coin_to_cost, retry_times=3):
        """Using a order strategy, then integrate all orders to one to return"""
        minimum_cost = self._fetch_min_cost()
        order_infos = []  # all orders
        # Continue to buy if enough
        while base_coin_to_cost > minimum_cost and retry_times >= 0:
            try:
                ask_price, _ = self._fetch_first_ticker()
                # bid order price, a little higher then the first ask price
                bid_order_price = ask_price * self.coin_pair.bid_order_price_coefficient
                # bid order amount
                bid_order_amount = base_coin_to_cost / bid_order_price
                order_info = self._create_order(OrderType.LIMIT, OrderSide.BUY, bid_order_amount, bid_order_price)
                self._log_procedure('Place Order', order_info)
                # track order
                order_info = self._track_order(order_info)
                self._log_procedure('Track Order', order_info)
            except Exception:
                self._log_exception('Place Order')
                retry_times -= 1
                time.sleep(5)
                continue
            # check filled amount
            cost_amount = self._fetch_cost_amount(order_info)
            if cost_amount > 0:
                # minus cost amount
                base_coin_to_cost = base_coin_to_cost - cost_amount
                # append to list
                order_infos.append(order_info)
                self._log_procedure('Remaining Amount [BUY]', base_coin_to_cost)
            else:
                self._log_procedure('Order Timeout', '{}'.format(retry_times))
                retry_times -= 1
            # sleep before next loop
            time.sleep(1.5)
        # Integrate all order_infos
        result_order = self._integrate_orders(order_infos, OrderSide.BUY)
        return result_order

    def _place_selling_orders(self, trade_coin_amount_to_sell, retry_times=3):
        """Using a order strategy, then integrate all orders to one to return"""
        minimum_cost = self._fetch_min_cost()
        order_infos = []  # all orders

        _, bid_price = self._fetch_first_ticker()
        # Continue to buy if could
        while bid_price * trade_coin_amount_to_sell > minimum_cost and retry_times >= 0:
            try:
                _, bid_price = self._fetch_first_ticker()
                # ask order price, a little lower then the first bid price
                ask_order_price = bid_price * self.coin_pair.ask_order_price_coefficient
                # create order
                order_info = self._create_order(OrderType.LIMIT, OrderSide.SELL,
                                                trade_coin_amount_to_sell, ask_order_price)
                self._log_procedure('Place Order', order_info)
                # track order
                order_info = self._track_order(order_info)
                self._log_procedure('Track Order', order_info)
            except Exception:
                self._log_exception('Place Order')
                retry_times -= 1
                time.sleep(5)
                continue
            # check filled amount
            remaining = order_info['remaining']
            if math.isclose(remaining, 0):
                trade_coin_amount_to_sell = remaining
                # append to list
                order_infos.append(order_info)
                self._log_procedure('Remaining Amount [SELL]', trade_coin_amount_to_sell)
            else:
                self._log_procedure('Order Timeout', '{}'.format(retry_times))
                retry_times -= 1
            # sleep before next loop
            time.sleep(1.5)
        # Integrate all order_infos
        result_order = self._integrate_orders(order_infos, OrderSide.SELL)
        return result_order

    def _handle_buy_signal(self):
        """Buying long"""
        minimum_cost = self._fetch_min_cost()
        _, bid_price = self._fetch_first_ticker()
        cost_base_coin_amount = self.base_coin_amount * self.leverage
        # Check min cost
        if cost_base_coin_amount < minimum_cost:
            self._log_procedure('Buying Signal', '{}({}) not enough to trade.'.format(
                self.coin_pair.base_coin, self.base_coin_amount))
            return None
        # if the trade coin that already hold worth more then minimum cost, ignore the signal
        if bid_price * self.trade_coin_amount * self.coin_pair.estimated_value_of_base_coin > minimum_cost:
            self._log_procedure('Buying Signal', 'Already hold {}({})'.format(
                self.coin_pair.trade_coin, self.trade_coin_amount))
            return None

        return self._place_buying_orders(cost_base_coin_amount)

    def _handle_sell_signal(self):
        """Short selling"""
        raise NotImplementedError('Not supported short selling')

    def _handle_close_signal(self):
        """Close"""
        minimum_cost = self._fetch_min_cost()
        _, bid_price = self._fetch_first_ticker()
        # if the trade coin that already hold worth less then minimum cost, ignore the signal
        if bid_price * self.trade_coin_amount < minimum_cost:
            self._log_procedure('Close Signal', '{}({}) amount is too little to sell'.format(
                self.coin_pair.trade_coin, self.trade_coin_amount))
            return None
        order_info = self._place_selling_orders(self.trade_coin_amount)
        if order_info and order_info.get('side'):
            order_info['side'] = 'close'
        return order_info

    # Public Functions

    def fetch_balance_until_success(self):
        """Fetch balance"""
        failed_times = 0
        while True:
            try:
                balance = self._ccxt_object_for_order.fetch_balance()['free']
                self.base_coin_amount = float(balance[self.coin_pair.base_coin])
                self.trade_coin_amount = float(balance[self.coin_pair.trade_coin])
                # log price
                exchange_name = re.sub('Trader$', '', self.__class__.__name__)
                balance_log = """Balance ({}.{}):
 - {}: {}
 - {}: {}""".format(exchange_name, self.coin_pair.pair(), self.coin_pair.trade_coin, self.trade_coin_amount, self.coin_pair.base_coin, self.base_coin_amount, )
                self.logger.log_procedure(balance_log)
                return self.base_coin_amount, self.trade_coin_amount
            except Exception:
                failed_times += 1
                if failed_times % 5 == 0:  # log every 5 times
                    self._log_exception('Fetch Balance')
                time.sleep(5)

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
