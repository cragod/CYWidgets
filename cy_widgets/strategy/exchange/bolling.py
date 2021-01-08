import talib as ta
import numpy as np
import pandas as pd

from cy_components.defines.column_names import *
from .base import BaseExchangeStrategy

COL_STD = 'std'
COL_MEDIAN = 'median'
COL_UPPER = 'upper'
COL_LOWER = 'lower'
COL_SIGNAL_LONG = 'signal_long'
COL_SIGNAL_SHORT = 'signal_short'


class BollingExchangeStrategy(BaseExchangeStrategy):
    """布林线交易策略"""
    m = 0
    n = 0

    def __init__(self, *args, **kwargs):
        super(BollingExchangeStrategy, self).__init__(args, kwargs)
        self.m = round(self.m, 4)

    def __str__(self):
        return 'bolling_strategy'

    @classmethod
    def strategy_with(cls, parameters):
        bolling = BollingExchangeStrategy()
        bolling.m = parameters[0]
        bolling.n = parameters[1]
        return bolling

    @property
    def identifier(self):
        res_str = '|m: %s; n: %s; l: %s' % (self.m, self.n, self.leverage)
        return res_str

    @property
    def name(self):
        return "Bolling"

    @property
    def candle_count_for_calculating(self):
        """多取10个以防万一"""
        return self.n + 10

    def available_to_calculate(self, df: pd.DataFrame):
        return self.m > 0 and self.n > 0 and df.shape[0] > self.m

    def calculate_signals(self, df: pd.DataFrame, drop_extra_columns=True):
        m = self.m
        n = self.n
        # 计算均线
        df[COL_MEDIAN] = ta.MA(df[COL_CLOSE], timeperiod=n)

        # 计算上轨、下轨道
        df[COL_STD] = ta.STDDEV(df[COL_CLOSE], timeperiod=n, nbdev=1)  # ddof代表标准差自由度
        df[COL_UPPER] = df[COL_MEDIAN] + m * df[COL_STD]
        df[COL_LOWER] = df[COL_MEDIAN] - m * df[COL_STD]

        # ===找出做多平仓信号
        condition1 = df[COL_CLOSE] < df[COL_MEDIAN]  # 当前K线的收盘价 < 中轨
        condition2 = df[COL_CLOSE].shift(1) >= df[COL_MEDIAN].shift(1)  # 之前K线的收盘价 >= 中轨
        df.loc[condition1 & condition2, COL_SIGNAL_LONG] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

        # ===找出做多信号
        condition1 = df[COL_CLOSE] > df[COL_UPPER]  # 当前K线的收盘价 > 上轨
        condition2 = df[COL_CLOSE].shift(1) <= df[COL_UPPER].shift(1)  # 之前K线的收盘价 <= 上轨
        df.loc[condition1 & condition2, COL_SIGNAL_LONG] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

        # ===找出做空平仓信号
        condition1 = df[COL_CLOSE] > df[COL_MEDIAN]  # 当前K线的收盘价 > 中轨
        condition2 = df[COL_CLOSE].shift(1) <= df[COL_MEDIAN].shift(1)  # 之前K线的收盘价 <= 中轨
        df.loc[condition1 & condition2, COL_SIGNAL_SHORT] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

        # ===找出做空信号
        condition1 = df[COL_CLOSE] < df[COL_LOWER]  # 当前K线的收盘价 < 下轨
        condition2 = df[COL_CLOSE].shift(1) >= df[COL_LOWER].shift(1)  # 之前K线的收盘价 >= 下轨
        df.loc[condition1 & condition2, COL_SIGNAL_SHORT] = -1  # 将产生做空信号的那根K线的signal设置为-1，-1代表做空

        # ===合并做多做空信号，去除重复信号
        df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1, skipna=True)  # 若你的pandas版本是最新的，请使用本行代码代替上面一行
        temp = df[df['signal'].notnull()][['signal']]
        # # === 去除重复信号
        temp = temp[temp['signal'] != temp['signal'].shift(1)]
        df['signal'] = temp['signal']

        if drop_extra_columns:
            df.drop([COL_MEDIAN, COL_STD, COL_UPPER, COL_LOWER, COL_SIGNAL_LONG,
                     COL_SIGNAL_SHORT], axis=1, inplace=True)

        return df

    def calculate_realtime_signals(self, df, debug=False, position_info=None, position_info_save_func=None):
        return self.calculate_signals(df).iloc[-1].signal
