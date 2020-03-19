# -*- coding: utf-8 -*-

from abc import ABC, abstractproperty


class BaseExchangeStrategy(ABC):
    """交易策略基类"""
    shortable = False  # 能否做空
    leverage = 1  # 策略杠杆

    def __init__(self, *initial_data, **kwargs):
        """支持按字典方式传入参数信息"""
        for dictionary in initial_data:
            for key in dictionary:
                setattr(self, key, dictionary[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])

    @classmethod
    def parameter_schema(cls):
        """ 参数的配置选项
        type {
            0: Double
            1: Bool
        }
        """
        return [
            {'name': 'leverage', 'type': 0, 'min': 0, 'max': 5, 'default': '1'},
            {'name': 'shortable', 'type': 1, 'default': '1'}
        ]

    @abstractproperty
    def identifier(self):
        """当前策略的标识串"""
        raise NotImplementedError('Need a identifier')

    @abstractproperty
    def candle_count_for_calculating(self):
        """计算策略需要的 K 线根数，用于实盘获取 K 线时参考"""
        raise NotImplementedError

    def available_to_calculate(self, df):
        """检查 K 线数据是否能用于策略计算"""
        return True

    def calculate_signals(self, df, drop_extra_columns=True):
        """计算信号"""
        raise NotImplementedError('?')
