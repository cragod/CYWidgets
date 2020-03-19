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
COL_PRE_SIGNAL = 'pre_signal'
COL_RSI = 'rsi'


class BollingExchangeStrategy(BaseExchangeStrategy):
    """布林线交易策略"""
    m = 0
    n = 0
    rsi_period = 0
    rsi_threshold = 0

    def __init__(self, *args, **kwargs):
        super(BollingExchangeStrategy, self).__init__(args, kwargs)

    @classmethod
    def parameter_schema(cls):
        """整合自身参数和父类参数"""
        base_schema = super(cls, cls).parameter_schema()
        bolling_schema = [
            {'name': 'm', 'type': 0, 'min': 0, 'max': 10, 'default': '2'},
            {'name': 'n', 'type': 0, 'min': 0, 'max': 1000, 'default': '100'},
            {'name': 'rsi_period', 'type': 0, 'min': 0, 'max': 1000, 'default': '0'},
            {'name': 'rsi_threshold', 'type': 0, 'min': 0, 'max': 100, 'default': '0'},
        ]
        bolling_schema.extend(base_schema)
        return bolling_schema

    @property
    def identifier(self):
        res_str = '%s | %s | %s' % (self.m, self.n, self.leverage)
        if self.rsi_period > 0:
            res_str = res_str + '| {} | {}'.format(self.rsi_period, self.rsi_threshold)
        return res_str

    @property
    def candle_count_for_calculating(self):
        """多取10个以防万一"""
        return self.n + 10

    def available_to_calculate(self, df: pd.DataFrame):
        return self.m > 0 and self.n > 0 and df.shape[0] > self.m

    def calculate_signals(self, df: pd.DataFrame, drop_extra_columns=True):
        #         print("""
        # Bolling Parameters：
        #     m: %s
        #     n: %s
        #     l: %s
        #   rsi: %s
        # rsi_t: %s
        #         """ % (str(self.m), str(self.n), str(self.leverage), str(self.rsi_period), str(self.rsi_threshold)))
        m = self.m
        n = self.n
        rsi_period = self.rsi_period
        rsi_threshold = self.rsi_threshold
        # 计算均线
        df[COL_MEDIAN] = ta.MA(df[COL_CLOSE], timeperiod=n)

        # 计算上轨、下轨道
        df[COL_STD] = ta.STDDEV(df[COL_CLOSE], timeperiod=n, nbdev=1)  # ddof代表标准差自由度
        df[COL_UPPER] = df[COL_MEDIAN] + m * df[COL_STD]
        df[COL_LOWER] = df[COL_MEDIAN] - m * df[COL_STD]

        # 趋势强度
        if rsi_period > 0:
            df[COL_RSI] = ta.RSI(df[COL_CLOSE], timeperiod=rsi_period)
        else:
            df[COL_RSI] = 100

        # ===找出做多平仓信号
        condition1 = df[COL_CLOSE] < df[COL_MEDIAN]  # 当前K线的收盘价 < 中轨
        condition2 = df[COL_CLOSE].shift(1) >= df[COL_MEDIAN].shift(1)  # 之前K线的收盘价 >= 中轨
        df.loc[condition1 & condition2, COL_SIGNAL_LONG] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

        # ===找出做多信号
        condition1 = df[COL_CLOSE] > df[COL_UPPER]  # 当前K线的收盘价 > 上轨
        condition2 = df[COL_CLOSE].shift(1) <= df[COL_UPPER].shift(1)  # 之前K线的收盘价 <= 上轨
        condition3 = df[COL_RSI] > rsi_threshold   # 趋势强度超过阙值
        df.loc[condition1 & condition2 & condition3, COL_SIGNAL_LONG] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

        if self.shortable:
            # ===找出做空平仓信号
            condition1 = df[COL_CLOSE] > df[COL_MEDIAN]  # 当前K线的收盘价 > 中轨
            condition2 = df[COL_CLOSE].shift(1) <= df[COL_MEDIAN].shift(1)  # 之前K线的收盘价 <= 中轨
            df.loc[condition1 & condition2, COL_SIGNAL_SHORT] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

            # ===找出做空信号
            condition1 = df[COL_CLOSE] < df[COL_LOWER]  # 当前K线的收盘价 < 下轨
            condition2 = df[COL_CLOSE].shift(1) >= df[COL_LOWER].shift(1)  # 之前K线的收盘价 >= 下轨
            df.loc[condition1 & condition2 & condition3, COL_SIGNAL_SHORT] = -1  # 将产生做空信号的那根K线的signal设置为-1，-1代表做空
            # df.drop_duplicates(subset=[COL_SIGNAL_LONG, COL_SIGNAL_SHORT], inplace=True)
        else:
            df[COL_SIGNAL_SHORT] = np.nan

        # ===合并做多做空信号，去除重复信号
        df[COL_SIGNAL] = df[[COL_SIGNAL_LONG, COL_SIGNAL_SHORT]].sum(axis=1, min_count=1, skipna=True)

        temp = df[df[COL_SIGNAL].notnull()][[COL_SIGNAL]]
        temp = temp[temp[COL_SIGNAL] != temp[COL_SIGNAL].shift(1)]
        df[COL_SIGNAL] = temp[COL_SIGNAL]
        if drop_extra_columns:
            df.drop([COL_MEDIAN, COL_STD, COL_UPPER, COL_LOWER, COL_SIGNAL_LONG, COL_SIGNAL_SHORT], axis=1, inplace=True)

        # ===由signal计算出实际的每天持有仓位
        # signal的计算运用了收盘价，是每根K线收盘之后产生的信号，到第二根开盘的时候才买入，仓位才会改变。
        df[COL_POS] = df[COL_SIGNAL].shift()
        df[COL_POS].fillna(method='ffill', inplace=True)
        df[COL_POS].fillna(value=0, inplace=True)  # 将初始行数的position补全为0

        return df
