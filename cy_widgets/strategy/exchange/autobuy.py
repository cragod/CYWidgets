import talib as ta
import pandas as pd
from cy_components.defines.column_names import *
from .base import BaseExchangeStrategy


class AutoBuyCoinStrategy(BaseExchangeStrategy):

    interval_day = 1  # buying interval
    start_index = 0  # start index
    ma_periods = 0  # MA periods

    def __init__(self, *args, **kwargs):
        super(AutoBuyCoinStrategy, self).__init__(args, kwargs)

    @classmethod
    def parameter_schema(cls):
        """ parameters' schema for selection """
        base_schema = super(cls, cls).parameter_schema()
        abc_schema = [
            {'name': 'interval_day', 'type': 2, 'min': 1, 'max': 30, 'default': '1'},  # Int
            {'name': 'start_index', 'type': 2, 'min': 1, 'max': 30, 'default': '1'},  # Int
            {'name': 'ma_periods', 'type': 0, 'min': 0, 'max': 100, 'default': '0'},  # Int
        ]
        abc_schema.extend(base_schema)
        return abc_schema

    @property
    def identifier(self):
        res_str = "{} | {} | {}".format(self.interval_day, self.start_index, self.ma_periods)
        return res_str

    @property
    def candle_count_for_calculating(self):
        return self.ma_periods + 10

    def available_to_calculate(self, df: pd.DataFrame):
        return self.interval_day >= 1 and self.start_index >= 0

    def calculate_signals(self, df: pd.DataFrame, drop_extra_columns=True):
        # Signal
        df.loc[self.start_index::self.interval_day, COL_SIGNAL] = 1
        if self.ma_periods > 0:
            col_ma = 'ma'
            col_max = 'max_ratio'
            col_min = 'min_ratio'
            col_close_to_ma_change = 'high_change'
            # MA
            df[col_ma] = ta.MA(df[COL_CLOSE], timeperiod=self.ma_periods)

            df[col_min] = 0.5
            df[col_max] = 2.5
            df[col_close_to_ma_change] = df[COL_CLOSE] / df[col_ma]
            df[col_close_to_ma_change].fillna(value=0, inplace=True)
            signal_cond = df[COL_SIGNAL] > 0
            normalized_cond = (df[col_close_to_ma_change] <= 1.05) & (df[col_close_to_ma_change] >= 0.95)
            less_buy_cond = df[col_close_to_ma_change] > 1.05
            over_buy_cond = df[col_close_to_ma_change] < 0.95
            df.loc[signal_cond & normalized_cond, COL_POS] = 1
            df.loc[signal_cond & over_buy_cond, COL_POS] = 1 + (0.95 - df[col_close_to_ma_change]) * 10
            df.loc[signal_cond & over_buy_cond, COL_POS] = df[[COL_POS, col_max]].min(axis=1)
            df.loc[signal_cond & less_buy_cond, COL_POS] = 1  # - (df[col_close_to_ma_change] - 1.05) * 10
            df.loc[signal_cond & less_buy_cond, COL_POS] = df[[COL_POS, col_min]].max(axis=1)
        else:
            df[COL_POS] = df[COL_SIGNAL]
        return df
