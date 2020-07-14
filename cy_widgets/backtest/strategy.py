import pandas as pd
import types
from ..strategy.exchange.base import BaseExchangeStrategy


class StrategyBacktest:
    """单次回测逻辑"""

    def __init__(self, df: pd.DataFrame, strategy: BaseExchangeStrategy, evaluate_func, result_handler):
        """回测配置初始化

        Parameters
        ----------
        df : pd.DataFrame
            [candle_begin_time, open, high, low, close, volume]
        strategy : BaseExchangeStrategy
            策略
        evaluate_func : (df) -> df_ev
            评估方法
        result_handler : (df_ev, error_des) -> void
            结果处理
        """
        def assert_param(tar, tar_type):
            assert tar is not None and isinstance(tar, tar_type)

        assert_param(df, pd.DataFrame)
        assert_param(strategy, BaseExchangeStrategy)
        assert_param(evaluate_func, types.FunctionType)
        assert_param(result_handler, types.FunctionType)

        self.__df = df
        self.__strategy = strategy
        self.__evaluate_func = evaluate_func
        self.__result_handler = result_handler

    def perform_test(self):
        """执行回测"""
        if self.__strategy.available_to_calculate(self.__df):
            signal_df = self.__strategy.calculate_signals(self.__df)
            self.__result_handler(self.__evaluate_func(signal_df), None)
        else:
            self.__result_handler(None, 'K 线数量不够计算信号')