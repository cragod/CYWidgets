import pandas as pd
import talib as ta
from .base import BaseExchangeStrategy


class SBMAGapBollingStrategy(BaseExchangeStrategy):
    """布林线交易策略"""
    n = 0
    stop_loss_pct = 100

    def __init__(self, *args, **kwargs):
        super(SBMAGapBollingStrategy, self).__init__(args, kwargs)

    def __str__(self):
        return 'sb_ma_gap_bolling_strategy'

    @classmethod
    def strategy_with(cls, parameters):
        bolling = SBMAGapBollingStrategy()
        bolling.n = int(parameters[0])
        bolling.stop_loss_pct = float(parameters[1])
        return bolling

    @property
    def identifier(self):
        res_str = '| n: %s | stop: %s' % (self.n, self.stop_loss_pct)
        return res_str

    @property
    def name(self):
        return "SBMAGapBolling"

    @property
    def candle_count_for_calculating(self):
        return self.n + 10

    def available_to_calculate(self, df: pd.DataFrame):
        return df.shape[0] > self.candle_count_for_calculating - 10

    def calculate_signals(self, df: pd.DataFrame, drop_extra_columns=True):
        ma_long = self.n
        stop_loss_pct = self.stop_loss_pct

        # ----分别取最高价，最低价的快慢速均线共四根，为了节省算力EMA和MA共用周期

        df['ma_short_h'] = ta.EMA(df.high, ma_long)
        df['ma_short_l'] = ta.EMA(df.low, ma_long)
        df['ma_long_h'] = df['high'].rolling(window=ma_long).mean()
        df['ma_long_l'] = df['low'].rolling(window=ma_long).mean()

        df['ma'] = df['close'].rolling(window=ma_long).mean()  # 一根均线
        df['atr'] = ta.NATR(df.high, df.low, df.close, ma_long)  # atr

        # ----取四根均线的最大间距
        condition1 = abs(df['ma_short_h'] - df['ma_long_l']) > abs(df['ma_short_l'] - df['ma_long_h'])
        condition2 = abs(df['ma_short_h'] - df['ma_long_l']) <= abs(df['ma_short_l'] - df['ma_long_h'])
        df.loc[condition1, 'ga'] = df['ma_short_h'] - df['ma_long_l']
        df.loc[condition2, 'ga'] = df['ma_short_l'] - df['ma_long_h']

        df['gap'] = df['ga'] * df['atr']  # atr加持

        df['x'] = df['atr'] / df['atr'].shift(1)  # 也可以是 短周期的atr/长周期的atr，这个自由把握。可以用来调整轨道宽度。也可以调整周期，也可以调整杠杆。
        df['y'] = df['close'] / df['ma']

        # 对中轨微微调
        df['median'] = df['gap'].rolling(window=ma_long).mean() / df['y']

        df['std'] = df['gap'].rolling(window=ma_long).std(ddof=0)
        df['gap_J神'] = abs(df['gap'] - df['median']) / df['std']
        df['gap_m'] = df['gap_J神'].rolling(window=ma_long).mean().shift(1)  # 均线收缩貌似均线将要交叉的时候才有效，所以取了min，取mean也可以。

        # df['upper'] = (df['median'] + df['gap_m'] * df['std']) /df['x']
        # df['lower'] = (df['median'] - df['gap_m'] * df['std']) /df['x']

        df['upper'] = df['median'] + df['gap_m'] * df['std']
        df['lower'] = df['median'] - df['gap_m'] * df['std']

        condition1 = df['gap'] < df['median']  # 当前K线的收盘价 < 中轨
        condition2 = df['gap'].shift(1) >= df['median'].shift(1)  # 之前K线的收盘价 >= 中轨
        df.loc[condition1 & condition2, 'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

        # ===找出做多信号
        condition1 = df['gap'] > df['upper']  # 当前K线的收盘价 > 上轨
        condition2 = df['gap'].shift(1) <= df['upper'].shift(1)  # 之前K线的收盘价 <= 上轨
        df.loc[condition1 & condition2, 'signal_long'] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

        # ===找出做空平仓信号
        condition1 = df['gap'] > df['median']  # 当前K线的收盘价 > 中轨
        condition2 = df['gap'].shift(1) <= df['median'].shift(1)  # 之前K线的收盘价 <= 中轨
        df.loc[condition1 & condition2, 'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

        # ===找出做空信号
        condition1 = df['gap'] < df['lower']  # 当前K线的收盘价 < 下轨
        condition2 = df['gap'].shift(1) >= df['lower'].shift(1)  # 之前K线的收盘价 >= 下轨
        df.loc[condition1 & condition2, 'signal_short'] = -1  # 将产生做空信号的那根K线的signal设置为-1，-1代表做空

        # ===考察是否需要止盈止损
        self.process_stop_lose(df, stop_loss_pct)

        # # 计算真实信号
        # df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1, skipna=True)  # 若你的pandas版本是最新的，请使用本行代码代替上面一行
        # temp = df[df['signal'].notnull()][['signal']]
        # temp = temp[temp['signal'] != temp['signal'].shift(1)]
        # df['signal'] = temp['signal']

        return df

    def calculate_realtime_signals(self, df, debug=False, position_info=None, position_info_save_func=None):
        signal_df = self.calculate_signals(df)
        if debug:
            print(signal_df[-50:])
            print(signal_df.iloc[-1])
        return signal_df.iloc[-1].signal
