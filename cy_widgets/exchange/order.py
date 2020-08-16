import math
from enum import Enum
from functools import reduce
from cy_components.utils.coin_pair import CoinPair


class OrderSide(Enum):
    BUY = 'buy'      # 多
    SELL = 'sell'    # 空
    CLOSE = 'close'  # 平


class OrderType(Enum):
    MARKET = 'market'  # 市价
    LIMIT = 'limit'    # 限价


class Order:
    """订单类，带订单信息，已经最终下单信息
    输出订单格式：
    {
      'info': order,
      'id': str(order['id']),
      'timestamp': timestamp,
      'datetime': self.iso8601(timestamp),
      'lastTradeTimestamp': None,
      'symbol': symbol,
      'type': orderType,
      'side': side,
      'price': self.safe_float(order, 'price'),
      'average': self.safe_float(order, 'avg_execution_price'),
      'amount': self.safe_float(order, 'original_amount'),
      'remaining': self.safe_float(order, 'remaining_amount'),
      'filled': self.safe_float(order, 'executed_amount'),
      'status': status,
      'fee': None,
    }
    """
    # 输出订单
    __result_orders = list()

    def __init__(self, coin_pair: CoinPair,
                 base_coin_amount,
                 trade_coin_amount,
                 leverage=1,
                 side=OrderSide.BUY,
                 type=OrderType.LIMIT,
                 bid_order_price_coefficient=1.01,
                 ask_order_price_coefficient=0.99):
        super().__init__()
        self.coin_pair = coin_pair
        self.base_coin_amount = base_coin_amount
        self.trade_coin_amount = trade_coin_amount
        self.leverage = leverage
        self.side = side
        self.type = type
        self.bid_order_price_coefficient = bid_order_price_coefficient
        self.ask_order_price_coefficient = ask_order_price_coefficient

    @staticmethod
    def fetch_cost_amount(order_info):
        """获取花费的币数量"""
        if order_info:
            # 没有 cost 字段的，用 filled * price 计算出实际花费
            if order_info.get('cost'):
                return order_info['cost']
            if order_info.get('filled') and order_info.get('price'):
                return order_info['filled'] * order_info['price']
        return 0

    @staticmethod
    def all_filled(order_info):
        """检查是否全成交"""
        return order_info['remaining'] is not None and math.isclose(order_info['remaining'], 0.0)

    @staticmethod
    def set_order_side(order_info, side: OrderSide):
        """更新Side"""
        if order_info and order_info.get('side'):
            order_info['side'] = side.value.lower()
        return order_info

    def integrate_orders(self, order_infos, fetch_ticker_func):
        """整合一组订单

        Parameters
        ----------
        order_infos : [dict]
            订单列表，每项一个字典
        fetch_ticker_func : func
            获取 ticker 方法

        Returns
        -------
        [dict]
            一组订单
        """

        if not order_infos:
            return None
        elif len(order_infos) == 1:
            return order_infos[0]

        def reduce_order_field(key, order_list):
            """同字段累加"""
            if len(order_list) == 1:
                return order_list[0][key]
            return reduce(lambda x, y: x[key] + y[key], order_list)
        filled = reduce_order_field('filled', order_infos)
        cost = reduce_order_field('cost', order_infos)
        amount = reduce_order_field('filled', order_infos[:-1]) + order_infos[-1]['amount']
        # 剩余和价格取最后一个订单
        remaining = order_infos[-1]['remaining']
        price = order_infos[-1]['price']
        # 计算成交均价
        if cost and filled:
            average = cost / filled
        else:
            # 有的平台没有足够信息，暂定用盘口价格来计算 average/cost
            ask_price, bid_price = fetch_ticker_func()
            estimated_price = ask_price if self.__order.side == OrderSide.BUY else bid_price
            average = estimated_price
            cost = average * filled
        result_order = order_infos[-1]
        result_order['filled'] = filled
        result_order['cost'] = cost
        result_order['amount'] = amount
        result_order['remaining'] = remaining
        result_order['price'] = price
        result_order['average'] = average
        # self._log_procedure('Integrate Orders', result_order)
        return result_order

    def append_order(self, order_info):
        self.__result_orders.append(order_info)
