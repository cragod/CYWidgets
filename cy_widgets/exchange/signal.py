from enum import Enum


class ExchangeSignal(Enum):
    """现货交易信号"""
    LONG = 'long'   # 开多
    SHORT = 'short'  # 开空
    CLOSE = 'close'  # 平仓
    CLOSE_THEN_LONG = 'close_then_long'   # 平仓 + 开多
    CLOSE_THEN_SHORT = 'close_then_short'  # 平仓 + 开空
