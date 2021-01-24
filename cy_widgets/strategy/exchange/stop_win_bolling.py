import pandas as pd
from .base import BaseExchangeStrategy


class StopWinBollingStrategy(BaseExchangeStrategy):
    """布林线震荡策略"""
    n = 0
    m = 0
    stop_win_pct = 0

    def __init__(self, *args, **kwargs):
        super(StopWinBollingStrategy, self).__init__(args, kwargs)

    def __str__(self):
        return 'shock_bolling_strategy'

    @classmethod
    def strategy_with(cls, parameters):
        bolling = StopWinBollingStrategy()
        bolling.n = int(parameters[0])
        bolling.m = float(parameters[1])
        bolling.stop_win_pct = float(parameters[2])
        return bolling

    @property
    def name(self):
        return "ShockBolling"

    @property
    def candle_count_for_calculating(self):
        return self.n + 10

    def available_to_calculate(self, df: pd.DataFrame):
        return df.shape[0] > self.candle_count_for_calculating - 10

    def calculate_signals(self, df: pd.DataFrame, drop_extra_columns=True):
        """
        :param df:
        :param para: n, m,zy
        :return:

        # 布林线策略
        # 布林线中轨：n天收盘价的移动平均线
        # 布林线上轨：n天收盘价的移动平均线 + m * n天收盘价的标准差
        # 布林线上轨：n天收盘价的移动平均线 - m * n天收盘价的标准差
        # 当收盘价由下向上穿过上轨的时候，做多；然后由上向下穿过中轨的时候，平仓。
        # 当收盘价由上向下穿过下轨的时候，做空；然后由下向上穿过中轨的时候，平仓。
        """

        # ===策略参数
        # n代表取平均线和标准差的参数
        # m代表标准差的倍数
        # zy代表收盘价减去上轨或下轨的差的绝对值除以上轨或下轨
        n = self.n
        m = self.m
        zy = self.stop_win_pct
        # ===计算指标
        # 计算均线
        df['median'] = df['close'].rolling(n, min_periods=1).mean()
        # 计算上轨、下轨道
        df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
        df['upper'] = df['median'] + m * df['std']
        df['lower'] = df['median'] - m * df['std']

        # 计算做多上轨止盈值=（收盘价-上轨）/上轨
        df['upper_zy'] = abs((df['close'] - df['upper']) / df['upper'])

        # 计算做空下轨止盈偏值=|（收盘价-下轨）|/上轨
        df['lower_zy'] = abs((df['close'] - df['lower']) / df['lower'])

        # ===计算信号
        # 找出做多信号
        condition1 = df['close'] > df['upper']  # 当前K线的收盘价 > 上轨
        condition2 = df['close'].shift(1) <= df['upper'].shift(1)  # 之前K线的收盘价 <= 上轨
        df.loc[condition1 & condition2, 'signal_long'] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

        # 找出做多平仓信号
        condition1 = df['close'] < df['median']  # 当前K线的收盘价 < 中轨
        condition2 = df['close'].shift(1) >= df['median'].shift(1)  # 之前K线的收盘价 >= 中轨
        condition3 = df['close'] > df['upper']  # 收盘价格 > 上轨
        condition4 = df['upper_zy'] > zy  # （收盘价-上轨）/上轨>止盈比例
        df.loc[condition1 & condition2, 'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓
        df.loc[condition3 & condition4, 'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

        # 找出做空信号
        condition1 = df['close'] < df['lower']  # 当前K线的收盘价 < 下轨
        condition2 = df['close'].shift(1) >= df['lower'].shift(1)  # 之前K线的收盘价 >= 下轨
        df.loc[condition1 & condition2, 'signal_short'] = -1  # 将产生做空信号的那根K线的signal设置为-1，-1代表做空

        # 找出做空平仓信号
        condition1 = df['close'] > df['median']  # 当前K线的收盘价 > 中轨
        condition2 = df['close'].shift(1) <= df['median'].shift(1)  # 之前K线的收盘价 <= 中轨
        condition3 = df['close'] < df['lower']  # 收盘价格 < 下轨
        condition4 = df['lower_zy'] > zy  # |（收盘价-下轨）|/上轨>止盈比例
        df.loc[condition1 & condition2, 'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓
        df.loc[condition3 & condition4, 'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

        # 合并做多做空信号，去除重复信号
        df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1, skipna=True)  # 若你的pandas版本是最新的，请使用本行代码代替上面一行
        temp = df[df['signal'].notnull()][['signal']]
        temp = temp[temp['signal'] != temp['signal'].shift(1)]
        df['signal'] = temp['signal']

        # ===删除无关变量
        # df.drop(['median', 'std', 'upper', 'lower', 'signal_long', 'signal_short'], axis=1, inplace=True)
        df.drop(['std', 'signal_long', 'signal_short'], axis=1, inplace=True)

        return df

    def calculate_realtime_signals(self, df, debug=False, position_info=None, position_info_save_func=None):
        signal_df = self.calculate_signals(df)
        if debug:
            print(signal_df[-50:])
            print(signal_df.iloc[-1])
        return signal_df.iloc[-1].signal
