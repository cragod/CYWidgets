import talib as ta
import pandas as pd
from cy_components.defines.column_names import *
from .base import BaseExchangeStrategy


class RSIExchangeStrategy(BaseExchangeStrategy):
    """RSI交易策略"""
    bull_rsi = 15
    bull_rsi_upper = 75
    bull_rsi_lower = 40

    bear_rsi = 15
    bear_rsi_upper = 75
    bear_rsi_lower = 450

    ma_long = 1000
    ma_short = 50

    # Can use default value

    bull_mod_high = 5
    bull_mod_low = -5

    bear_mod_high = 15
    bear_mod_low = -5

    adx = 3
    adx_high = 70
    adx_low = 50

    def __init__(self, *args, **kwargs):
        """
        bull_rsi = 15
        bull_rsi_upper = 75
        bull_rsi_lower = 40

        bear_rsi = 15
        bear_rsi_upper = 75
        bear_rsi_lower = 45

        ma_long = 1000
        ma_short = 50
        """
        super(RSIExchangeStrategy, self).__init__(args, kwargs)

    @classmethod
    def strategy_with(cls, parameters):
        return RSIExchangeStrategy()

    @property
    def identifier(self):
        """strategy instance's identifier, combined by parameters commonly."""
        return '{} | {} | {} | {} || {}/{} || {}/{}'.format(self.ma_long, self.ma_short, self.bull_rsi, self.bear_rsi, self.bull_rsi_upper, self.bull_rsi_lower, self.bear_rsi_upper, self.bear_rsi_lower)

    @property
    def name(self):
        return "RSI"

    @property
    def candle_count_for_calculating(self):
        return self.ma_long + 10

    def available_to_calculate(self, df: pd.DataFrame):
        return df.shape[0] > self.ma_long

    def calculate_signals(self, df: pd.DataFrame, drop_extra_columns=True):
        """Cal all signals
        MA_LONG MA_SHORT BULL_RSI BEAR_RSI
        MA_SHORT < MA_LONG:  [BEAR]
            bear_rsi > bear_rsi_high -> short
            bear_rsi < bear_rsi_low -> long
        MA_SHORT >= MA_LONG:  [BULL]
            bull_rsi > bull_rsi_high -> short
            bull_rsi < bull_rsi_low -> long
        """
        col_ma_short = 'ma_short'
        col_ma_long = 'ma_long'

        col_rsi_bull = 'rsi_bull'
        col_rsi_bear = 'rsi_bear'

        col_adx = 'adx'
        col_bear_rsi_high = 'bear_rsi_high'
        col_bear_rsi_low = 'bear_rsi_low'
        col_bull_rsi_high = 'bull_rsi_high'
        col_bull_rsi_low = 'bull_rsi_low'

        col_signal_long = 'signal_long'
        col_signal_short = 'signal_short'
        col_signal = 'signal'

        df[col_ma_short] = ta.SMA(df[COL_CLOSE], timeperiod=self.ma_short)
        df[col_ma_long] = ta.SMA(df[COL_CLOSE], timeperiod=self.ma_long)
        df[col_rsi_bull] = ta.RSI(df[COL_CLOSE], timeperiod=self.bull_rsi)
        df[col_rsi_bear] = ta.RSI(df[COL_CLOSE], timeperiod=self.bear_rsi)

        df[col_bear_rsi_high] = self.bear_rsi_upper
        df[col_bear_rsi_low] = self.bear_rsi_lower
        df[col_bull_rsi_high] = self.bull_rsi_upper
        df[col_bull_rsi_low] = self.bull_rsi_lower

        df[col_adx] = ta.ADX(df[COL_HIGH], df[COL_LOW], df[COL_CLOSE], timeperiod=self.adx)

        bear_cond = df[col_ma_short] < df[col_ma_long]

        adx_strong_cond = df[col_adx] > self.adx_high
        adx_weak_cond = df[col_adx] < self.adx_low

        # strengthen
        df.loc[bear_cond & adx_strong_cond, col_bear_rsi_high] += self.bear_mod_high
        df.loc[bear_cond & adx_weak_cond, col_bear_rsi_low] += self.bear_mod_low

        df.loc[~bear_cond & adx_strong_cond, col_bull_rsi_high] += self.bull_mod_high
        df.loc[~bear_cond & adx_weak_cond, col_bull_rsi_low] += self.bull_mod_low

        bear_rsi_short_cond = df[col_rsi_bear] > df[col_bear_rsi_high]
        bear_rsi_long_cond = df[col_rsi_bear] < df[col_bear_rsi_low]

        bull_rsi_short_cond = df[col_rsi_bull] > df[col_bull_rsi_high]
        bull_rsi_long_cond = df[col_rsi_bull] < df[col_bull_rsi_low]

        df.loc[bear_cond & bear_rsi_long_cond, col_signal_long] = 1
        df.loc[bear_cond & bear_rsi_short_cond, col_signal_short] = 0
        df.loc[~bear_cond & bull_rsi_long_cond, col_signal_long] = 1
        df.loc[~bear_cond & bull_rsi_short_cond, col_signal_short] = 0

        # ===合并做多做空信号，去除重复信号
        df[col_signal] = df[[col_signal_long, col_signal_short]].sum(axis=1, min_count=1, skipna=True)

        temp = df[df[col_signal].notnull()][[col_signal]]
        temp = temp[temp[col_signal] != temp[col_signal].shift(1)]
        df[col_signal] = temp[col_signal]
        if drop_extra_columns:
            df.drop([col_signal_long, col_signal_short, col_rsi_bull,
                     col_rsi_bear, col_ma_long, col_ma_short, col_bear_rsi_high,
                     col_bear_rsi_low, col_bull_rsi_high, col_bull_rsi_low, col_adx], axis=1, inplace=True)

        return df

    def calculate_realtime_signals(self, df, debug=False, position_info=None, position_info_save_func=None):
        return self.calculate_signals(df).iloc[-1].signal
