import pandas as pd
import types
from ..strategy.exchange.base import BaseExchangeStrategy


class StrategyBacktest:
    """单次回测逻辑"""

    def __init__(self, df: pd.DataFrame, strategy: BaseExchangeStrategy, pos_func, evaluate_func, result_handler, context=None):
        """回测配置初始化

        Parameters
        ----------
        df : pd.DataFrame
            [candle_begin_time, open, high, low, close, volume]
        strategy : BaseExchangeStrategy
            策略
        pos_func: (df) -> pos_df
            仓位方法
        evaluate_func : (pod_df) -> df_ev
            评估方法
        result_handler : (context, df_pos, df_ev, strategy, error_des) -> result_df
            结果处理
        """
        # def assert_param(tar, tar_type):
        #     assert tar is not None and isinstance(tar, tar_type)

        # assert_param(df, pd.DataFrame)
        # assert_param(strategy, BaseExchangeStrategy)
        # assert_param(evaluate_func, types.FunctionType)
        # assert_param(result_handler, types.FunctionType)

        self.__df = df
        self.__strategy = strategy
        self.__pos_func = pos_func
        self.__evaluate_func = evaluate_func
        self.__result_handler = result_handler
        self.__context = context

    def perform_test(self):
        """执行回测"""
        if self.__strategy.available_to_calculate(self.__df):
            signal_df = self.__strategy.calculate_signals(self.__df)
            pos_df = self.__pos_func(signal_df)
            return self.__result_handler(self.__context, pos_df, self.__evaluate_func(pos_df.copy()), self.__strategy, None)
        else:
            return self.__result_handler(self.__context, None, None, None, 'K 线数量不够计算信号')
