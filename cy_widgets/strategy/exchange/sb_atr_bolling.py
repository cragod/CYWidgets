import pandas as pd
import talib as ta
from .base import BaseExchangeStrategy


class SBATRBollingStrategy(BaseExchangeStrategy):
    """布林线交易策略"""
    n = 0
    stop_loss_pct = 100

    def __init__(self, *args, **kwargs):
        super(SBATRBollingStrategy, self).__init__(args, kwargs)

    def __str__(self):
        return 'sb_ma_gap_bolling_strategy'

    @classmethod
    def strategy_with(cls, parameters):
        bolling = SBATRBollingStrategy()
        bolling.n = int(parameters[0])
        bolling.stop_loss_pct = float(parameters[1])
        return bolling

    @property
    def identifier(self):
        res_str = '| n: %s | stop: %s' % (self.n, self.stop_loss_pct)
        return res_str

    @property
    def name(self):
        return "SBATRBolling"

    @property
    def candle_count_for_calculating(self):
        return self.n + 10

    def available_to_calculate(self, df: pd.DataFrame):
        return df.shape[0] > self.candle_count_for_calculating - 10

    def calculate_signals(self, df: pd.DataFrame, drop_extra_columns=True):
        n = self.n
        stop_loss_pct = self.stop_loss_pct

        # ----计算atr和std
        df['atr'] = ta.ATR(df['high'], df['low'], df['close'], n)
        df['std'] = df['close'].rolling(window=n).std(ddof=0)

        # -----计算中轨以及atr和std的倍数

        # ---中轨
        df['median'] = df['close'].rolling(window=n).mean()

        # ---atr，std倍数
        df['atr_J神'] = abs(df['close'] - df['median']) / df['atr']
        df['m_atr'] = df['atr_J神'].rolling(window=n).max().shift(1)
        df['boll_J神'] = abs(df['close'] - df['median']) / df['std']
        df['m_boll'] = df['boll_J神'].rolling(window=n).max().shift(1)

        # ---分别计算atr，布林通道上下轨
        df['upper_atr'] = df['median'] + df['m_atr'] * df['atr']
        df['lower_atr'] = df['median'] - df['m_atr'] * df['atr']

        df['upper_boll'] = df['median'] + df['m_boll'] * df['std']
        df['lower_boll'] = df['median'] - df['m_boll'] * df['std']

        # ----将两个上下轨揉在一起。取MIN开仓太频繁，取MAX开仓太少，最终取mean
        df['upper'] = df[['upper_atr', 'upper_boll']].mean(axis=1)
        df['lower'] = df[['lower_atr', 'lower_boll']].mean(axis=1)

        # -----计算开仓

        condition1 = df['close'] < df['median']  # 当前K线的收盘价 < 中轨
        condition2 = df['close'].shift(1) >= df['median'].shift(1)  # 之前K线的收盘价 >= 中轨
        df.loc[condition1 & condition2, 'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

        # ===找出做多信号
        condition1 = df['close'] > df['upper']  # 当前K线的收盘价 > 上轨
        condition2 = df['close'].shift(1) <= df['upper'].shift(1)  # 之前K线的收盘价 <= 上轨
        df.loc[condition1 & condition2, 'signal_long'] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

        # ===找出做空平仓信号
        condition1 = df['close'] > df['median']  # 当前K线的收盘价 > 中轨
        condition2 = df['close'].shift(1) <= df['median'].shift(1)  # 之前K线的收盘价 <= 中轨
        df.loc[condition1 & condition2, 'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

        # ===找出做空信号
        condition1 = df['close'] < df['lower']  # 当前K线的收盘价 < 下轨
        condition2 = df['close'].shift(1) >= df['lower'].shift(1)  # 之前K线的收盘价 >= 下轨
        df.loc[condition1 & condition2, 'signal_short'] = -1  # 将产生做空信号的那根K线的signal设置为-1，-1代表做空

        # ===考察是否需要止盈止损
        self.process_stop_lose(df, stop_loss_pct)

        return df

    def calculate_realtime_signals(self, df, debug=False, position_info=None, position_info_save_func=None):
        signal_df = self.calculate_signals(df)
        if debug:
            print(signal_df[-50:])
            print(signal_df.iloc[-1])
        return signal_df.iloc[-1].signal
