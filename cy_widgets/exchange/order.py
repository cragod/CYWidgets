from enum import Enum
from cy_components.utils.coin_pair import CoinPair


class OrderSide(Enum):
    BUY = 'buy'      # 多
    SELL = 'sell'    # 空
    CLOSE = 'close'  # 平


class OrderType(Enum):
    MARKET = 'market'  # 市价
    LIMIT = 'limit'    # 限价

# ---------------------------------


class Order:
    """订单类，带订单信息，已经最终下单信息"""
    coin_pair = CoinPair()
    leverage = 1  # 杠杆
    side = OrderSide.BUY
    type = OrderType.LIMIT
    base_coin_amount = 0
    trade_coin_amount = 0
    exchange_name = ""  # 交易所
