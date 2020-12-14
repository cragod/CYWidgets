import pandas as pd
from cy_components.defines.column_names import *


class GenericPosition:
    """[candle_begin_time, open, high, low, close, volume, signal]
    -> [candle_begin_time, open ,high, low, close, volume, signal, position]"""
    @staticmethod
    def calculate_position(df: pd.DataFrame):
        # ===由signal计算出实际的每天持有仓位
        # signal的计算运用了收盘价，是每根K线收盘之后产生的信号，到第二根开盘的时候才买入，仓位才会改变。
        df[COL_POS] = df[COL_SIGNAL].shift()
        df[COL_POS].fillna(method='ffill', inplace=True)
        df[COL_POS].fillna(value=0, inplace=True)  # 将初始行数的position补全为0
        return df
