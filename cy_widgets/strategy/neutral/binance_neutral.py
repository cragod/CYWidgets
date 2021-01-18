from .base import *
import numpy as np
from fracdiff import fdiff


class BinanceNeutralStrategy_1(NeutralStrategyBase):

    @classmethod
    def strategy_with_parameters(cls, parameters):
        """初始化"""
        return BinanceNeutralStrategy_1(int(parameters[0]), f'{int(parameters[1])}h', float(parameters[2]))

    @property
    def display_name(self):
        return "BinanceNeutralStrategy_1"

    @property
    def candle_count_4_cal_factor(self):
        return 35 * 3 + 10

    def __add_diff(self, _df, _diff_d, _name, _add=True):
        """ 为 数据列 添加 差分数据列
        :param _add:
        :param _df: 原数据 DataFrame
        :param _d_list: 差分阶数 [0.3, 0.5, 0.7]
        :param _name: 需要添加 差分值 的数据列 名称
        :param _agg_dict:
        :param _agg_type:
        :param _add:
        :return: """
        if _add:
            if len(_df) >= 12:  # 数据行数大于等于12才进行差分操作
                _diff_ar = fdiff(_df[_name], n=_diff_d, window=10, mode="valid")  # 列差分，不使用未来数据
                _paddings = len(_df) - len(_diff_ar)  # 差分后数据长度变短，需要在前面填充多少数据
                _diff = np.nan_to_num(np.concatenate((np.full(_paddings, 0), _diff_ar)), nan=0)  # 将所有nan替换为0
                _df[_name + f'_diff_{_diff_d}'] = _diff  # 将差分数据记录到 DataFrame
            else:
                _df[_name + f'_diff_{_diff_d}'] = np.nan  # 数据行数不足12的填充为空数据

    def cal_factor(self, df):
        # alpha_factors = ['bias_bh_9_diff_0.7', '涨跌幅_bh_6_diff_0.5']
        # _dna = ['涨跌幅_bh_48_diff_0.3', '振幅_bh_12_diff_0.5', '振幅2_bh_9', 'bias_bh_9_diff_0.7']
        alpha_factors = ['bias_bh_12_diff_0.5', 'bias_bh_9_diff_0.3']
        _dna = ['振幅_bh_24_diff_0.5', 'bias_bh_60_diff_0.5', '资金流入比例_bh_6_diff_0.7', '振幅_bh_24_diff_0.5']
        # --- bias ---  涨跌幅更好的表达方式 bias 币价偏离均线的比例。
        for (n, diff_d) in [(12, 0.5), (9, 0.3), (60, 0.5)]:
            ma = df['close'].rolling(n, min_periods=1).mean()
            df[f'bias_bh_{n}'] = (df['close'] / ma - 1)

            # 差分
            self.__add_diff(_df=df, _diff_d=diff_d, _name=f'bias_bh_{n}')

        # --- 涨跌幅 ---
        for (hour, diff_d) in [(48, 0.3), (6, 0.5)]:
            df[f'涨跌幅_bh_{hour}'] = df['close'].pct_change(hour)

            # 差分
            self.__add_diff(_df=df, _diff_d=diff_d, _name=f'涨跌幅_bh_{hour}')

        # --- 振幅 ---  最高价最低价
        for(n, diff_d) in [(24, 0.5)]:
            high = df['high'].rolling(n, min_periods=1).max()
            low = df['low'].rolling(n, min_periods=1).min()
            df[f'振幅_bh_{n}'] = (high / low - 1)

            # 差分
            self.__add_diff(_df=df, _diff_d=diff_d, _name=f'振幅_bh_{n}')

        # --- 资金流入比例 --- 币安独有的数据
        for(n, diff_d) in [(6, 0.7)]:
            volume = df['quote_volume'].rolling(n, min_periods=1).sum()
            buy_volume = df['taker_buy_quote_asset_volume'].rolling(n, min_periods=1).sum()
            df[f'资金流入比例_bh_{n}'] = (buy_volume / volume)

            # 差分
            self.__add_diff(_df=df, _diff_d=diff_d, _name=f'资金流入比例_bh_{n}')

        # --- 振幅2 ---  收盘价、开盘价
        # high = df[['close', 'open']].max(axis=1)
        # low = df[['close', 'open']].min(axis=1)
        # for (n, diff_d) in [(9, 0)]:
        #     high = high.rolling(n, min_periods=1).max()
        #     low = low.rolling(n, min_periods=1).min()
        #     df[f'振幅2_bh_{n}'] = (high / low - 1)

        #     # 差分
        #     self.__add_diff(_df=df, _diff_d=diff_d, _name=f'振幅2_bh_{n}')

        df['factor'] = \
            df[alpha_factors[0]] * (df[_dna[0]] + df[_dna[1]]) +\
            df[alpha_factors[1]] * (df[_dna[2]] + df[_dna[3]])
        return df
