import pandas as pd
from .base import BaseExchangeStrategy


class MTMAdaptBollingStrategy(BaseExchangeStrategy):
    """布林线交易策略"""
    n = 0
    scale = 35

    def __init__(self, *args, **kwargs):
        super(MTMAdaptBollingStrategy, self).__init__(args, kwargs)

    def __str__(self):
        return 'mtm_adapt_bolling_strategy'

    @classmethod
    def strategy_with(cls, parameters):
        bolling = MTMAdaptBollingStrategy()
        bolling.n = int(parameters[0])
        bolling.scale = int(parameters[1])
        return bolling

    @property
    def name(self):
        return "MTMAdaptBolling"

    @property
    def candle_count_for_calculating(self):
        return self.n * self.scale * 2 + 10

    def available_to_calculate(self, df: pd.DataFrame):
        return df.shape[0] > self.n * self.scale * 2

    def calculate_signals(self, df: pd.DataFrame, drop_extra_columns=True):
        n1 = self.n
        n2 = self.scale * n1

        df['median'] = df['close'].rolling(window=n2).mean()
        df['std'] = df['close'].rolling(n2, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
        df['z_score'] = abs(df['close'] - df['median']) / df['std']
        df['m'] = df['z_score'].rolling(window=n2).mean()
        df['upper'] = df['median'] + df['std'] * df['m']
        df['lower'] = df['median'] - df['std'] * df['m']

        condition_long = df['close'] > df['upper']
        condition_short = df['close'] < df['lower']

        df['mtm'] = df['close'] / df['close'].shift(n1) - 1
        df['mtm_mean'] = df['mtm'].rolling(window=n1, min_periods=1).mean()

        # 基于价格atr，计算波动率因子wd_atr
        df['c1'] = df['high'] - df['low']
        df['c2'] = abs(df['high'] - df['close'].shift(1))
        df['c3'] = abs(df['low'] - df['close'].shift(1))
        df['tr'] = df[['c1', 'c2', 'c3']].max(axis=1)
        df['atr'] = df['tr'].rolling(window=n1, min_periods=1).mean()
        df['avg_price'] = df['close'].rolling(window=n1, min_periods=1).mean()
        df['wd_atr'] = df['atr'] / df['avg_price']

        # 参考ATR，对MTM指标，计算波动率因子
        df['mtm_l'] = df['low'] / df['low'].shift(n1) - 1
        df['mtm_h'] = df['high'] / df['high'].shift(n1) - 1
        df['mtm_c'] = df['close'] / df['close'].shift(n1) - 1
        df['mtm_c1'] = df['mtm_h'] - df['mtm_l']
        df['mtm_c2'] = abs(df['mtm_h'] - df['mtm_c'].shift(1))
        df['mtm_c3'] = abs(df['mtm_l'] - df['mtm_c'].shift(1))
        df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
        df['mtm_atr'] = df['mtm_tr'].rolling(window=n1, min_periods=1).mean()

        # 参考ATR，对MTM mean指标，计算波动率因子
        df['mtm_l_mean'] = df['mtm_l'].rolling(window=n1, min_periods=1).mean()
        df['mtm_h_mean'] = df['mtm_h'].rolling(window=n1, min_periods=1).mean()
        df['mtm_c_mean'] = df['mtm_c'].rolling(window=n1, min_periods=1).mean()
        df['mtm_c1'] = df['mtm_h_mean'] - df['mtm_l_mean']
        df['mtm_c2'] = abs(df['mtm_h_mean'] - df['mtm_c_mean'].shift(1))
        df['mtm_c3'] = abs(df['mtm_l_mean'] - df['mtm_c_mean'].shift(1))
        df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
        df['mtm_atr_mean'] = df['mtm_tr'].rolling(window=n1, min_periods=1).mean()

        indicator = 'mtm_mean'

        # mtm_mean指标分别乘以三个波动率因子
        df[indicator] = 1e5 * df['mtm_atr'] * df['mtm_atr_mean'] * df['wd_atr'] * df[indicator]

        # 对新策略因子计算自适应布林
        df['median'] = df[indicator].rolling(window=n1).mean()
        df['std'] = df[indicator].rolling(n1, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
        df['z_score'] = abs(df[indicator] - df['median']) / df['std']
        # df['m'] = df['z_score'].rolling(window=n1).max().shift(1)
        # df['m'] = df['z_score'].rolling(window=n1).mean()
        df['m'] = df['z_score'].rolling(window=n1).min().shift(1)
        df['up'] = df['median'] + df['std'] * df['m']
        df['dn'] = df['median'] - df['std'] * df['m']

        # 突破上轨做多
        condition1 = df[indicator] > df['up']
        condition2 = df[indicator].shift(1) <= df['up'].shift(1)
        condition = condition1 & condition2
        df.loc[condition, 'signal_long'] = 1

        # 突破下轨做空
        condition1 = df[indicator] < df['dn']
        condition2 = df[indicator].shift(1) >= df['dn'].shift(1)
        condition = condition1 & condition2
        df.loc[condition, 'signal_short'] = -1

        # 均线平仓(多头持仓)
        condition1 = df[indicator] < df['median']
        condition2 = df[indicator].shift(1) >= df['median'].shift(1)
        condition = condition1 & condition2
        df.loc[condition, 'signal_long'] = 0

        # 均线平仓(空头持仓)
        condition1 = df[indicator] > df['median']
        condition2 = df[indicator].shift(1) <= df['median'].shift(1)
        condition = condition1 & condition2
        df.loc[condition, 'signal_short'] = 0

        df.loc[condition_long, 'signal_short'] = 0
        df.loc[condition_short, 'signal_long'] = 0

        # ===由signal计算出实际的每天持有仓位
        # signal的计算运用了收盘价，是每根K线收盘之后产生的信号，到第二根开盘的时候才买入，仓位才会改变。
        df['signal_short'].fillna(method='ffill', inplace=True)
        df['signal_long'].fillna(method='ffill', inplace=True)
        df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1, skipna=True)  # 若你的pandas版本是最新的，请使用本行代码代替上面一行
        temp = df[df['signal'].notnull()][['signal']]
        temp = temp[temp['signal'] != temp['signal'].shift(1)]
        df['signal'] = temp['signal']

        # df.drop(['signal_long', 'signal_short'], axis=1, inplace=True)
        df.drop(['mtm', 'mtm_l', 'mtm_h', 'mtm_c', 'atr', 'z_score', 'c1', 'c2', 'c3', 'tr', 'avg_price', 'wd_atr',
                 'mtm_c3', 'mtm_tr', 'mtm_atr', 'mtm_l_mean', 'mtm_h_mean', 'mtm_c_mean', 'mtm_atr_mean', 'mtm_c2', 'mtm_c1'], axis=1, inplace=True)
        return df

    def calculate_realtime_signals(self, df, debug=False, position_info=None, position_info_save_func=None):
        signal_df = self.calculate_signals(df)
        if debug:
            print(signal_df[-50:])
            print(signal_df.iloc[-1])
        return signal_df.iloc[-1].signal
