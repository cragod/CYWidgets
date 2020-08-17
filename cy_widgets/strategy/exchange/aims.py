import talib as ta
import pandas as pd
from cy_components.defines.column_names import *
from .base import BaseExchangeStrategy


class AutoInvestVarietalStrategy(BaseExchangeStrategy):
    """ 定投式主动交易，策略只负责买入信号，卖出外部自定标准 """

    ma_periods = 0  # MA periods
    signal_scale = 10

    def __init__(self, *args, **kwargs):
        super(AutoInvestVarietalStrategy, self).__init__(args, kwargs)

    @classmethod
    def parameter_schema(cls):
        """ parameters' schema for selection """
        base_schema = super(cls, cls).parameter_schema()
        abc_schema = [
            {'name': 'ma_periods', 'type': 0, 'min': 0, 'max': 100, 'default': '0'},  # Int
        ]
        abc_schema.extend(base_schema)
        return abc_schema

    @property
    def identifier(self):
        res_str = "{}".format(self.ma_periods)
        return res_str

    @property
    def name(self):
        return 'auto_invest_varietal'

    @property
    def candle_count_for_calculating(self):
        return self.ma_periods + 10

    def available_to_calculate(self, df: pd.DataFrame):
        return True

    def calculate_signals(self, df: pd.DataFrame, drop_extra_columns=True):
        if self.ma_periods > 0:
            col_ma = 'ma'
            # MA
            df[col_ma] = ta.MA(df[COL_CLOSE], timeperiod=self.ma_periods).shift(1)
            # open / last_ma5 = signal
            df.loc[df[COL_OPEN] / df[col_ma] < 1, COL_SIGNAL] = (1 - df[COL_OPEN] / df[col_ma]) * self.signal_scale + 1
            # fillna
            df[COL_SIGNAL].fillna(value=0, inplace=True)
        return df
