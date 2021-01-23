from datetime import datetime
import numpy as np
import pandas as pd
from .base import *


class IceMAShrinkageStrategy(BaseExchangeStrategy):
    """ICE MA SHRINKAGE"""

    short_ma = 0
    long_ma = 0
    gap_ma = 0

    def __init__(self, *args, **kwargs):
        super(IceMAShrinkageStrategy, self).__init__(args, kwargs)

    def __str__(self):
        return 'ice_ma_shrinkage'

    @classmethod
    def strategy_with(cls, parameters):
        bolling = IceMAShrinkageStrategy()
        bolling.short_ma = int(parameters[0])
        bolling.long_ma = int(parameters[1])
        bolling.gap_ma = int(parameters[2])
        return bolling

    @property
    def identifier(self):
        """当前策略的标识串"""
        return '{},{},{}'.format(self.short_ma, self.long_ma, self.gap_ma)

    @property
    def name(self):
        """策略名"""
        return "IceMAShrinkage"

    @property
    def candle_count_for_calculating(self):
        return max(self.long_ma, self.gap_ma) * 2 + 10

    def available_to_calculate(self, df):
        return (df.shape[0] >= self.candle_count_for_calculating - 10) and self.long_ma > self.short_ma

    def calculate_signals(self, df, drop_extra_columns=True):
        '''
        :param df:
        :param para: s, l, a
        :return:
        '''

        # ===策略参数
        # 短期均线周期，长期均线周期，OS的均线周期
        s, l, a = self.short_ma, self.long_ma, self.gap_ma

        # ===计算指标
        # 计算短、长周期均线
        df['s_ma'] = df['close'].rolling(s, min_periods=1).mean()

        # df['l_wma'] = df['close'].rolling(l, min_periods=1).mean()
        # 取消注释长期均线即用wma计算
        df['l_wma'] = df['close'].rolling(l, min_periods=1).apply(lambda x: (x * np.arange(1, len(x) + 1)).sum() / (l * (l - 1) * 0.5), raw=True)

        # 计算均线差值及差值的均线
        df['os'] = df['s_ma'] - df['l_wma']
        df['os_ma'] = df['os'].rolling(a, min_periods=1).mean()

        # ===计算多空信号
        df.loc[(df['os_ma'] < 0) & (df['os_ma'].shift(1) >= 0), 'signal_short'] = -1
        df.loc[(df['os_ma'] > 0) & (df['os_ma'].shift(1) <= 0), 'signal_short'] = 0
        df.loc[(df['os_ma'] > 0) & (df['os_ma'].shift(1) <= 0), 'signal_long'] = 1
        df.loc[(df['os_ma'] < 0) & (df['os_ma'].shift(1) >= 0), 'signal_long'] = 0

        # 合并做多做空信号，去除重复信号
        df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1, skipna=True)  # 若你的pandas版本是最新的，请使用本行代码代替上面一行
        temp = df[df['signal'].notnull()][['signal']]
        temp = temp[temp['signal'] != temp['signal'].shift(1)]
        df['signal'] = temp['signal']

        # ===删除无关变量
        df.drop(['s_ma', 'l_wma', 'os',
                 'signal_long', 'signal_short'], axis=1, inplace=True)

        return df

    def calculate_realtime_signals(self, df, debug=False, position_info=None, position_info_save_func=None):
        return self.calculate_signals(df).iloc[-1].signal
