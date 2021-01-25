# -*- coding: utf-8 -*-
import numpy as np
import talib as ta
from abc import ABC, abstractproperty, abstractclassmethod, abstractmethod


class BaseExchangeStrategy(ABC):
    """交易策略基类"""
    shortable = True  # 能否做空
    leverage = 1  # 策略杠杆

    def __init__(self, *initial_data, **kwargs):
        """支持按字典方式传入参数信息"""
        for dictionary in initial_data:
            for key in dictionary:
                setattr(self, key, dictionary[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])

    @abstractclassmethod
    def strategy_with(cls, parameters):
        raise NotImplementedError('初始化参数')

    @abstractproperty
    def name(self):
        """策略名"""
        raise NotImplementedError('Need a name')

    @abstractproperty
    def candle_count_for_calculating(self):
        """计算策略需要的 K 线根数，用于实盘获取 K 线时参考"""
        raise NotImplementedError

    @abstractmethod
    def available_to_calculate(self, df):
        """检查 K 线数据是否能用于策略计算"""
        return True

    @abstractmethod
    def calculate_signals(self, df, drop_extra_columns=True):
        """计算信号, 统一返回格式[candle_begin_time, open, high, low, close, volume, signal]"""
        raise NotImplementedError('?')

    @abstractmethod
    def calculate_realtime_signals(self, df, debug=False, position_info=None, position_info_save_func=None):
        """计算实时信号

        Parameters
        ----------
        position_info : dict, optional
            策略仓位数据
        position_info_save_func : [type], optional
            保存方法

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError("?")

    def process_stop_lose(self, df, stop_loss_pct):
        # ===考察是否需要止盈止损
        df_new = df[['signal_long', 'signal_short']]

        array_long_short = df_new.to_numpy(dtype=np.float32)  # should float32, for int32 does not have np.nan

        df_c = df[['close']]

        array_data = df_c.to_numpy(dtype=np.float32)

        result_array = self.numpy_process_stop_lose(array_long_short, array_data, stop_loss_pct)

        df['signal'] = result_array[:, 0]

    def numpy_process_stop_lose(self, array1, array_close, stop_loss_pct):
        n = array1.shape[0]
        result_signal = np.zeros((n, 1), dtype=np.float32)
        result_signal.fill(np.nan)
        pre_signal = 0
        stop_lose_price = np.nan

        for i in range(n):
            # 如果之前是空仓
            if pre_signal == 0:
                # 当本周期有做多信号
                if array1[i, 0] == 1:
                    result_signal[i, 0] = 1  # 将真实信号设置为1
                    # 记录当前状态
                    pre_signal = 1  # 信号
                    stop_lose_price = array_close[i, 0] * (1 - stop_loss_pct / 100.0)  # 以本周期的收盘价乘以一定比例作为止损价格。也可以用下周期的开盘价df.at[i+1, 'open']，但是此时需要注意i等于最后一个i时，取i+1会报错

                # 当本周期有做空信号
                elif array1[i, 1] == -1:
                    result_signal[i, 0] = -1  # 将真实信号设置为-1
                    # 记录相关信息
                    pre_signal = -1  # 信号
                    stop_lose_price = array_close[i, 0] * (1 + stop_loss_pct / 100.0)  # 以本周期的收盘价乘以一定比例作为止损价格，也可以用下周期的开盘价df.at[i+1, 'open']

                # 无信号
                else:
                    # 记录相关信息
                    pre_signal = 0
                    stop_lose_price = np.nan

            # 如果之前是多头仓位
            elif pre_signal == 1:
                # 当本周期有平多仓信号，或者需要止损,止盈
                if (array1[i, 0] == 0) or (array_close[i, 0] < stop_lose_price):
                    result_signal[i, 0] = 0  # 将真实信号设置为0
                    # 记录相关信息
                    pre_signal = 0
                    stop_lose_price = np.nan

                # 当本周期有平多仓并且还要开空仓
                if array1[i, 1] == -1:
                    result_signal[i, 0] = -1  # 将真实信号设置为-1
                    # 记录相关信息
                    pre_signal = -1  # 信号
                    stop_lose_price = array_close[i, 0] * (1 + stop_loss_pct / 100.0)  # 以本周期的收盘价乘以一定比例作为止损价格，也可以用下周期的开盘价df.at[i+1, 'open']

                # zwx add, if pre_signal still is 1,  use max value as zhisun
                if pre_signal == 1:
                    tmp_stop_lose_price = array_close[i, 0] * (1 - stop_loss_pct / 100.0)
                    if tmp_stop_lose_price > stop_lose_price:
                        stop_lose_price = tmp_stop_lose_price

            # 如果之前是空头仓位
            elif pre_signal == -1:
                # 当本周期有平空仓信号，或者需要止损, 止盈
                if (array1[i, 1] == 0) or (array_close[i, 0] > stop_lose_price):
                    result_signal[i, 0] = 0  # 将真实信号设置为0
                    # 记录相关信息
                    pre_signal = 0
                    stop_lose_price = np.nan

                # 当本周期有平空仓并且还要开多仓
                if array1[i, 0] == 1:
                    result_signal[i, 0] = 1  # 将真实信号设置为1
                    # 记录相关信息
                    pre_signal = 1  # 信号
                    stop_lose_price = array_close[i, 0] * (1 - stop_loss_pct / 100.0)  # 以本周期的收盘价乘以一定比例作为止损价格，也可以用下周期的开盘价df.at[i+1, 'open']

                # if pre_signal still is -1,  use min value as zhiying
                if pre_signal == -1:
                    tmp_stop_lose_price = array_close[i, 0] * (1 + stop_loss_pct / 100.0)
                    if tmp_stop_lose_price < stop_lose_price:
                        stop_lose_price = tmp_stop_lose_price

            # 其他情况
            else:
                raise ValueError('不可能出现其他的情况，如果出现，说明代码逻辑有误，报错')

        return result_signal
