import talib as ta
import numpy as np
import pandas as pd

from cy_components.defines.column_names import *
from .base import BaseExchangeStrategy


class VixBollingStrategy(BaseExchangeStrategy):
    """布林线交易策略"""
    n = 0

    def __init__(self, *args, **kwargs):
        super(VixBollingStrategy, self).__init__(args, kwargs)

    def __str__(self):
        return 'vix_bolling_strategy'

    @classmethod
    def strategy_with(cls, parameters):
        bolling = VixBollingStrategy()
        bolling.n = int(parameters[0])
        return bolling

    @property
    def identifier(self):
        res_str = 'n: %s; l: %s' % (self.n, self.stop_loss_pct)
        return res_str

    @property
    def name(self):
        return "VixBolling"

    @property
    def candle_count_for_calculating(self):
        """多取10个以防万一"""
        return self.n * 2 + 10

    def available_to_calculate(self, df: pd.DataFrame):
        return df.shape[0] >= self.candle_count_for_calculating - 10

    def calculate_signals(self, df: pd.DataFrame, drop_extra_columns=True):
        n = self.n

        df['vix'] = np.log(df['close']) / np.log(df['close'].shift(n)) - 1
        df['mean'] = df['vix'].rolling(n).mean()
        df['up'] = df['vix'].rolling(n).max().shift(1)
        df['down'] = df['vix'].rolling(n).min().shift(1)

        # === 多 - 平
        condition1 = df['vix'] < df['mean']
        condition2 = df['vix'].shift(1) >= df['mean'].shift(1)
        df.loc[condition1 & condition2, 'signal_long'] = 0

        # === 多
        condition1 = df['vix'] > df['up']
        condition2 = df['vix'].shift(1) <= df['up'].shift(1)
        df.loc[condition1 & condition2, 'signal_long'] = 1

        # === 空 - 平
        condition1 = df['vix'] > df['mean']
        condition2 = df['vix'].shift(1) <= df['mean'].shift(1)
        df.loc[condition1 & condition2, 'signal_short'] = 0

        # === 空
        condition1 = df['vix'] < df['down']
        condition2 = df['vix'].shift(1) >= df['down'].shift(1)
        df.loc[condition1 & condition2, 'signal_short'] = -1

        # ===合并做多做空信号，去除重复信号
        df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1, skipna=True)  # 若你的pandas版本是最新的，请使用本行代码代替上面一行
        temp = df[df['signal'].notnull()][['signal']]
        # # === 去除重复信号
        temp = temp[temp['signal'] != temp['signal'].shift(1)]
        df['signal'] = temp['signal']

        return df

    def calculate_realtime_signals(self, df, debug=False, position_info=None, position_info_save_func=None):
        return self.calculate_signals(df).iloc[-1].signal
