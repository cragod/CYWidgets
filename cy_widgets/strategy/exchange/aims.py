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
    def strategy_with(cls, parameters):
        strategy = AutoInvestVarietalStrategy()
        strategy.ma_periods = parameters[0]
        strategy.signal_scale = parameters[1]
        return strategy

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

    def calculate_realtime_signals(self, df, debug=False, position_info=None, position_info_save_func=None):
        return self.calculate_signals(df).iloc[-1].signal


COL_STD = 'std'
COL_MEDIAN = 'median'
COL_UPPER = 'upper'
COL_LOWER = 'lower'


class AutoInvestBollingStrategy(BaseExchangeStrategy):
    """ 布林线下轨买入，策略只负责买入信号，卖出外部自定标准 """

    m = 0
    n = 0
    signal_scale = 10
    buy_threshold = 0.03

    def __init__(self, *args, **kwargs):
        super(AutoInvestBollingStrategy, self).__init__(args, kwargs)

    @classmethod
    def strategy_with(cls, parameters):
        aib = AutoInvestBollingStrategy()
        aib.m = parameters[0]
        aib.n = int(parameters[1])
        aib.signal_scale = parameters[2]
        aib.buy_threshold = parameters[3]

    @property
    def identifier(self):
        res_str = "{}.{}.{}".format(self.signal_scale, self.m, self.n)
        return res_str

    @property
    def name(self):
        return 'auto_invest_bolling'

    @property
    def candle_count_for_calculating(self):
        return self.n + 10

    def available_to_calculate(self, df: pd.DataFrame):
        return True

    def calculate_signals(self, df: pd.DataFrame, drop_extra_columns=True):
        m = self.m
        n = self.n
        # 计算均线
        df[COL_MEDIAN] = ta.MA(df[COL_CLOSE], timeperiod=n)

        # 计算上轨、下轨道
        df[COL_STD] = ta.STDDEV(df[COL_CLOSE], timeperiod=n, nbdev=1)  # ddof代表标准差自由度
        df[COL_UPPER] = df[COL_MEDIAN] + m * df[COL_STD]
        df[COL_LOWER] = df[COL_MEDIAN] - m * df[COL_STD]

        condition = df[COL_OPEN] < df[COL_LOWER] * (1 + self.buy_threshold)  # 开盘低于 (下轨 + 比例阈值的) 时买入
        df.loc[condition, COL_SIGNAL] = (1 - df[COL_OPEN] / (df[COL_LOWER] *
                                                             (1 + self.buy_threshold))) * self.signal_scale + 1
        # fillna
        df[COL_SIGNAL].fillna(value=0, inplace=True)
        return df

    def calculate_realtime_signals(self, df, debug=False, position_info=None, position_info_save_func=None):
        return self.calculate_signals(df).iloc[-1].signal
