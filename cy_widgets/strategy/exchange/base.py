# -*- coding: utf-8 -*-

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
    def identifier(self):
        """当前策略的标识串"""
        raise NotImplementedError('Need a identifier')

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
    def calculate_realtime_signals(self, df, avg_price, debug=False):
        """实盘信号"""
        raise NotImplementedError("?")
