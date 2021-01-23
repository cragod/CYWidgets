from datetime import datetime
import numpy as np
import pandas as pd
from .base import *


class AberrationFlashBollingStrategy(BaseExchangeStrategy):
    """Aberration + J.Flash"""

    n = 0
    m = 0.0
    stop_loss_pct = 5
    leverage_rate = 1
    holding_times_min = 10

    def __init__(self, *args, **kwargs):
        super(AberrationFlashBollingStrategy, self).__init__(args, kwargs)

    def __str__(self):
        return 'aberration_flash_bolling_strategy'

    @classmethod
    def strategy_with(cls, parameters):
        bolling = AberrationFlashBollingStrategy()
        bolling.n = int(parameters[0])
        bolling.m = round(float(parameters[1]), 2)
        bolling.stop_loss_pct = round(float(parameters[2]), 2)
        bolling.leverage = float(parameters[3])
        return bolling

    @property
    def identifier(self):
        """当前策略的标识串"""
        return '{},{},{},{},'.format(self.n, self.m, self.stop_loss_pct, self.leverage)

    @property
    def name(self):
        """策略名"""
        return "AberrationFlashBolling"

    @property
    def candle_count_for_calculating(self):
        return self.n + 10

    def available_to_calculate(self, df):
        return df.shape[0] >= self.candle_count_for_calculating - 10

    def calculate_signals(self, df, drop_extra_columns=True):
        """计算信号, 统一返回格式[candle_begin_time, open, high, low, close, volume, signal]"""
        # ===计算指标
        n = self.n
        m = self.m
        stop_loss_pct = self.stop_loss_pct
        leverage_rate = self.leverage

        # 存临时ma
        ma_dict = dict()
        # 初始化signal
        df['signal'] = np.nan

        # 计算flash加速均线的最小持仓周期
        holding_times_min = self.holding_times_min

        # 计算中轨均线
        df['median'] = df['close'].rolling(n, min_periods=1).mean()

        # flash加速均线，加速前先用中轨初始化
        df['flash_stop_win'] = df['median'].copy()  # 止盈

        # 布林上下轨:
        df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
        # df['zscore'] = abs(df['close'] - df['median']) / df['std']
        # m = df['zscore'].rolling(n).max().shift()
        df['upper'] = df['median'] + m * df['std']
        df['lower'] = df['median'] - m * df['std']

        # mtm指标，参考Aberration动量过滤
        df['mtm'] = df['close'] / df['close'].shift(n) - 1

        # ===找出做多信号
        condition1 = (df['close'] > df['upper']) & (df['mtm'] > 0)  # 当前K线的收盘价 > 上轨, mtm > 0
        condition2 = df['close'].shift(1) <= df['upper'].shift(1)  # 之前K线的收盘价 <= 上轨
        df.loc[condition1 & condition2, 'signal_long'] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

        # ===找出做多平仓信号
        condition1 = df['close'] < df['median']  # 当前K线的收盘价 < 中轨
        condition2 = df['close'].shift(1) >= df['median'].shift(1)  # 之前K线的收盘价 >= 中轨
        df.loc[condition1 & condition2, 'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

        # ===找出做空信号
        condition1 = (df['close'] < df['lower']) & (df['mtm'] < 0)  # 当前K线的收盘价 < 下轨, mtm < 0
        condition2 = df['close'].shift(1) >= df['lower'].shift(1)  # 之前K线的收盘价 >= 下轨
        df.loc[condition1 & condition2, 'signal_short'] = -1  # 将产生做空信号的那根K线的signal设置为-1，-1代表做空

        # ===找出做空平仓信号
        condition1 = df['close'] > df['median']  # 当前K线的收盘价 > 中轨
        condition2 = df['close'].shift(1) <= df['median'].shift(1)  # 之前K线的收盘价 <= 中轨
        df.loc[condition1 & condition2, 'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

        # ===考察是否需要止盈止损
        info_dict = {
            'pre_signal': 0,
            'stop_lose_price': None,
            'holding_times': 0,
            'stop_win_times': 0,
            'stop_win_price': 0
        }  # 用于记录之前交易信号，止损价格，持仓次数
        # pre_signal：上一次signal
        # stop_lose_price: 开仓后，记录的止损价格
        # holding_times: 开仓后，持仓计数器，计算均线加速用
        # stop_win_times: 开仓后，止盈点产生的次数
        # stop_win_price：开仓后，每次产生的止盈点价格

        # 逐行遍历df，考察每一行的交易信号
        for i in range(df.shape[0]):
            # 如果之前是空仓
            if info_dict['pre_signal'] == 0:

                # 当本周期有做多信号
                if df.at[i, 'signal_long'] == 1:
                    df.at[i, 'signal'] = 1  # 将真实信号设置为1
                    # 记录当前状态
                    pre_signal = 1  # 信号
                    stop_lose_price = df.at[i, 'close'] * (1 - stop_loss_pct / 100 / leverage_rate)  # 以本周期的收盘价乘以一定比例作为止损价格。
                    info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price, 'holding_times': 0,
                                 'stop_win_times': 0, 'stop_win_price': 0}  # 开多仓后 初始化info_dict

                # 当本周期有做空信号
                elif df.at[i, 'signal_short'] == -1:
                    df.at[i, 'signal'] = -1  # 将真实信号设置为-1
                    # 记录相关信息
                    pre_signal = -1  # 信号
                    stop_lose_price = df.at[i, 'close'] * (1 + stop_loss_pct / 100 / leverage_rate)  # 以本周期的收盘价乘以一定比例作为止损价格
                    info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price, 'holding_times': 0,
                                 'stop_win_times': 0, 'stop_win_price': 0}  # 开空仓后 初始化info_dict
                # 无信号
                else:
                    # 无信号，初始化info_dict
                    info_dict = {'pre_signal': 0, 'stop_lose_price': None, 'holding_times': 0, 'stop_win_times': 0,
                                 'stop_win_price': 0}

            # 如果之前是多头仓位
            elif info_dict['pre_signal'] == 1:
                # 由持仓次数，决定flash加速均线用哪个ma
                holding_times = info_dict['holding_times']
                ma_temp = max(n - holding_times, holding_times_min)
                info_dict['holding_times'] = holding_times + 1

                # 如果该ma已存在于ma_dict，则直接获取，不重复计算
                if ma_temp in ma_dict:
                    df_ma_temp = ma_dict[ma_temp]
                else:
                    df_ma_temp = df['close'].rolling(ma_temp, min_periods=1).mean()
                    ma_dict[ma_temp] = df_ma_temp

                # flash加速均线止盈点记录，用于分析及可视化
                df.at[i, 'flash_stop_win'] = df_ma_temp.iloc[i]

                # 如果价格达到止盈点
                if df.at[i, 'close'] < df.at[i, 'flash_stop_win']:

                    # 如果价格超过了上一次止盈点价格，说明上方可能还有空间，则重新开始flash加速，
                    if df.at[i, 'close'] > info_dict['stop_win_price'] or info_dict['stop_win_times'] == 0:
                        # 记录最新的止盈点价格
                        info_dict['stop_win_price'] = df.at[i, 'close']
                        # 产生的止盈点次数+1
                        info_dict['stop_win_times'] = info_dict['stop_win_times'] + 1
                        # flash加速均线的持仓次数清零
                        info_dict['holding_times'] = 0

                    # 直到利润的尽头才平仓
                    else:
                        df.at[i, 'signal_long'] = 0  # 将真实信号设置为0

                # 当本周期有平多仓信号，或者需要止损
                if (df.at[i, 'signal_long'] == 0) or (df.at[i, 'close'] < info_dict['stop_lose_price']):
                    df.at[i, 'signal'] = 0  # 将真实信号设置为0
                    # 记录相关信息
                    info_dict = {'pre_signal': 0, 'stop_lose_price': None, 'holding_times': 0, 'stop_win_times': 0,
                                 'stop_win_price': 0}

                # 当本周期有平多仓并且还要开空仓
                if df.at[i, 'signal_short'] == -1:
                    df.at[i, 'signal'] = -1  # 将真实信号设置为-1
                    # 记录相关信息
                    pre_signal = -1  # 信号
                    stop_lose_price = df.at[i, 'close'] * (1 + stop_loss_pct / 100 / leverage_rate)  # 以本周期的收盘价乘以一定比例作为止损价格
                    info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price, 'holding_times': 0,
                                 'stop_win_times': 0, 'stop_win_price': 0}

            # 如果之前是空头仓位
            elif info_dict['pre_signal'] == -1:
                # 由持仓次数，决定flash加速均线用哪个ma
                holding_times = info_dict['holding_times']
                ma_temp = max(n - holding_times, holding_times_min)
                info_dict['holding_times'] = holding_times + 1

                # 如果该ma已存在于ma_dict，则直接获取，不重复计算
                if ma_temp in ma_dict:
                    df_ma_temp = ma_dict[ma_temp]
                else:
                    df_ma_temp = df['close'].rolling(ma_temp, min_periods=1).mean()
                    ma_dict[ma_temp] = df_ma_temp

                # flash加速均线止盈点记录，用于分析及可视化
                df.at[i, 'flash_stop_win'] = df_ma_temp.iloc[i]

                # 如果价格达到止盈点
                if df.at[i, 'close'] > df.at[i, 'flash_stop_win']:
                    # 如果价格跌破了上一次止盈点价格，说明下方可能还有空间，则重新开始flash加速，
                    if df.at[i, 'close'] < info_dict['stop_win_price'] or info_dict['stop_win_times'] == 0:
                        # 记录最新的止盈点价格
                        info_dict['stop_win_price'] = df.at[i, 'close']
                        info_dict['stop_win_times'] = info_dict['stop_win_times'] + 1
                        info_dict['holding_times'] = 0
                    else:
                        df.at[i, 'signal_short'] = 0  # 将真实信号设置为0

                # 当本周期有平空仓信号，或者需要止损
                if (df.at[i, 'signal_short'] == 0) or (df.at[i, 'close'] > info_dict['stop_lose_price']):
                    df.at[i, 'signal'] = 0  # 将真实信号设置为0
                    # 记录相关信息
                    info_dict = {'pre_signal': 0, 'stop_lose_price': None, 'holding_times': 0, 'stop_win_times': 0,
                                 'stop_win_price': 0}

                # 当本周期有平空仓并且还要开多仓
                if df.at[i, 'signal_long'] == 1:
                    df.at[i, 'signal'] = 1  # 将真实信号设置为1
                    # 记录相关信息
                    pre_signal = 1  # 信号
                    stop_lose_price = df.at[i, 'close'] * (
                        1 - stop_loss_pct / 100 / leverage_rate)  # 以本周期的收盘价乘以一定比例作为止损价格，也可以用下周期的开盘价df.at[i+1, 'open']
                    info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price, 'holding_times': 0,
                                 'stop_win_times': 0, 'stop_win_price': 0}

            # 其他情况
            else:
                raise ValueError('不可能出现其他的情况，如果出现，说明代码逻辑有误，报错')

        return df

    def calculate_realtime_signals(self, df, debug=False, position_info=None, position_info_save_func=None):
        """实盘信号，这里只给信号，其他的外面处理"""
        n = self.n
        m = self.m

        df['median'] = df['close'].rolling(n).mean()
        # 计算上轨、下轨道
        df['std'] = df['close'].rolling(n).std(ddof=0)  # ddof代表标准差自由度
        df['upper'] = df['median'] + m * df['std']
        df['lower'] = df['median'] - m * df['std']

        df['mtm'] = df['close'] / df['close'].shift(n) - 1

        # 做空条件
        condition1 = df.iloc[-1]['close'] < df.iloc[-1]['lower']
        condition2 = df.iloc[-2]['close'] > df.iloc[-2]['lower']
        condition3 = df.iloc[-1]['mtm'] < 0
        short_condition = condition1 & condition2 & condition3

        # 做多条件
        condition1 = df.iloc[-1]['close'] > df.iloc[-1]['upper']
        condition2 = df.iloc[-2]['close'] < df.iloc[-2]['upper']
        condition3 = df.iloc[-1]['mtm'] > 0
        long_condition = condition1 & condition2 & condition3

        # 平空条件
        condition1 = df.iloc[-1]['close'] > df.iloc[-1]['median']
        condition2 = df.iloc[-2]['close'] < df.iloc[-2]['median']
        close_short_condition = condition1 & condition2

        # 平多条件
        condition1 = df.iloc[-1]['close'] < df.iloc[-1]['median']
        condition2 = df.iloc[-2]['close'] > df.iloc[-2]['median']
        close_long_condition = condition1 & condition2

        signal = None

        # ===考察是否需要止盈止损，有记录就读取
        info_dict = position_info if position_info is not None else {
            'pre_signal': 0,
            'stop_lose_price': None,
            'holding_times': 0,
            'stop_win_times': 0,
            'stop_win_price': 0
        }
        # 用于记录之前交易信号，止损价格，持仓次数
        # pre_signal：上一次signal
        # stop_lose_price: 开仓后，记录的止损价格
        # holding_times: 开仓后，持仓计数器，计算均线加速用
        # stop_win_times: 开仓后，止盈点产生的次数
        # stop_win_price：开仓后，每次产生的止盈点价格

        def log_if_debug(content):
            if debug:
                print('---------------------')
                print(datetime.now())
                print(content)

        log_if_debug("strategy info before: {}".format(info_dict))

        # 如果之前是空仓
        if info_dict['pre_signal'] == 0:

            # 当本周期有做多信号
            if long_condition:
                signal = 1
                # 记录当前状态
                pre_signal = 1  # 信号
                stop_lose_price = df.iloc[-1]['close'] * (1 - self.stop_loss_pct / 100 / self.leverage_rate)  # 以本周期的收盘价乘以一定比例作为止损价格。
                info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price, 'holding_times': 0,
                             'stop_win_times': 0, 'stop_win_price': 0}  # 开多仓后 初始化info_dict

                log_if_debug("0. 开多")

            # 当本周期有做空信号
            elif short_condition:
                signal = -1  # 将真实信号设置为-1
                # 记录相关信息
                pre_signal = -1  # 信号
                stop_lose_price = df.iloc[-1]['close'] * (1 + self.stop_loss_pct / 100 / self.leverage_rate)  # 以本周期的收盘价乘以一定比例作为止损价格
                info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price, 'holding_times': 0,
                             'stop_win_times': 0, 'stop_win_price': 0}  # 开空仓后 初始化info_dict

                log_if_debug("1. 开空")
            # 无信号
            else:
                # 无信号，初始化info_dict
                info_dict = {'pre_signal': 0, 'stop_lose_price': None, 'holding_times': 0, 'stop_win_times': 0,
                             'stop_win_price': 0}

                log_if_debug("2. 无信号")

        # 如果之前是多头仓位
        elif info_dict['pre_signal'] == 1:
            # 由持仓次数，决定flash加速均线用哪个ma
            holding_times = info_dict['holding_times']
            ma_temp = max(n - holding_times, self.holding_times_min)
            info_dict['holding_times'] = holding_times + 1

            # 计算加速均线
            df_ma_temp = df['close'].rolling(ma_temp, min_periods=1).mean()

            # 如果价格达到止盈点
            if df.iloc[-1]['close'] < df_ma_temp.iloc[-1]:

                # 如果价格超过了上一次止盈点价格，说明上方可能还有空间，则重新开始flash加速，
                if df.iloc[-1]['close'] > info_dict['stop_win_price'] or info_dict['stop_win_times'] == 0:
                    # 记录最新的止盈点价格
                    info_dict['stop_win_price'] = df.iloc[-1]['close']
                    # 产生的止盈点次数+1
                    info_dict['stop_win_times'] = info_dict['stop_win_times'] + 1
                    # flash加速均线的持仓次数清零
                    info_dict['holding_times'] = 0

                    log_if_debug("3. 多，重新加速")

                # 直到利润的尽头才平仓
                else:
                    signal = 0  # 将真实信号设置为0

                    log_if_debug("4. 平多（下穿加速线）")

            # 当本周期有平多仓信号，或者需要止损
            if close_long_condition or (df.iloc[-1]['close'] < info_dict['stop_lose_price']):
                signal = 0
                # 记录相关信息
                info_dict = {'pre_signal': 0, 'stop_lose_price': None, 'holding_times': 0, 'stop_win_times': 0,
                             'stop_win_price': 0}

                log_if_debug("5. 平多 or 止损")

            # 当本周期有平多仓并且还要开空仓
            if short_condition:
                signal = -1  # 将真实信号设置为-1
                # 记录相关信息
                pre_signal = -1  # 信号
                stop_lose_price = df.iloc[-1]['close'] * (1 + self.stop_loss_pct / 100 / self.leverage_rate)  # 以本周期的收盘价乘以一定比例作为止损价格
                info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price, 'holding_times': 0,
                             'stop_win_times': 0, 'stop_win_price': 0}

                log_if_debug("6. 平多开空")

        # 如果之前是空头仓位
        elif info_dict['pre_signal'] == -1:
            # 由持仓次数，决定flash加速均线用哪个ma
            holding_times = info_dict['holding_times']
            ma_temp = max(n - holding_times, self.holding_times_min)
            info_dict['holding_times'] = holding_times + 1

            df_ma_temp = df['close'].rolling(ma_temp, min_periods=1).mean()

            # 如果价格达到止盈点
            if df.iloc[-1]['close'] > df_ma_temp.iloc[-1]:
                # 如果价格跌破了上一次止盈点价格，说明下方可能还有空间，则重新开始flash加速，
                if df.iloc[-1]['close'] < info_dict['stop_win_price'] or info_dict['stop_win_times'] == 0:
                    # 记录最新的止盈点价格
                    info_dict['stop_win_price'] = df.iloc[-1]['close']
                    info_dict['stop_win_times'] = info_dict['stop_win_times'] + 1
                    info_dict['holding_times'] = 0

                    log_if_debug("7. 空，重新加速")
                else:
                    signal = 0  # 将真实信号设置为0

                    log_if_debug("8. 平空（上穿加速线）")

            # 当本周期有平空仓信号，或者需要止损
            if close_short_condition or (df.iloc[-1]['close'] > info_dict['stop_lose_price']):
                signal = 0
                # 记录相关信息
                info_dict = {'pre_signal': 0, 'stop_lose_price': None, 'holding_times': 0, 'stop_win_times': 0,
                             'stop_win_price': 0}

                log_if_debug("9. 平空 or 止损")

            # 当本周期有平空仓并且还要开多仓
            if long_condition == 1:
                signal = 1  # 将真实信号设置为1
                # 记录相关信息
                pre_signal = 1  # 信号
                stop_lose_price = df.iloc[-1]['close'] * (
                    1 - self.stop_loss_pct / 100 / self.leverage_rate)  # 以本周期的收盘价乘以一定比例作为止损价格，也可以用下周期的开盘价df.at[i+1, 'open']
                info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price, 'holding_times': 0,
                             'stop_win_times': 0, 'stop_win_price': 0}

                log_if_debug("10. 平空开多")

        # 其他情况
        else:
            raise ValueError('不可能出现其他的情况，如果出现，说明代码逻辑有误，报错')

        # 保存信号
        if position_info_save_func is not None:
            position_info_save_func(info_dict)
            log_if_debug("strategy info after: {}".format(info_dict))

        return signal
