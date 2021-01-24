from .base import *


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
            self._add_diff(_df=df, _diff_d=diff_d, _name=f'bias_bh_{n}')

        # --- 涨跌幅 ---
        for (hour, diff_d) in [(48, 0.3), (6, 0.5)]:
            df[f'涨跌幅_bh_{hour}'] = df['close'].pct_change(hour)

            # 差分
            self._add_diff(_df=df, _diff_d=diff_d, _name=f'涨跌幅_bh_{hour}')

        # --- 振幅 ---  最高价最低价
        for(n, diff_d) in [(24, 0.5)]:
            high = df['high'].rolling(n, min_periods=1).max()
            low = df['low'].rolling(n, min_periods=1).min()
            df[f'振幅_bh_{n}'] = (high / low - 1)

            # 差分
            self._add_diff(_df=df, _diff_d=diff_d, _name=f'振幅_bh_{n}')

        # --- 资金流入比例 --- 币安独有的数据
        for(n, diff_d) in [(6, 0.7)]:
            volume = df['quote_volume'].rolling(n, min_periods=1).sum()
            buy_volume = df['taker_buy_quote_asset_volume'].rolling(n, min_periods=1).sum()
            df[f'资金流入比例_bh_{n}'] = (buy_volume / volume)

            # 差分
            self._add_diff(_df=df, _diff_d=diff_d, _name=f'资金流入比例_bh_{n}')

        # --- 振幅2 ---  收盘价、开盘价
        # high = df[['close', 'open']].max(axis=1)
        # low = df[['close', 'open']].min(axis=1)
        # for (n, diff_d) in [(9, 0)]:
        #     high = high.rolling(n, min_periods=1).max()
        #     low = low.rolling(n, min_periods=1).min()
        #     df[f'振幅2_bh_{n}'] = (high / low - 1)

        #     # 差分
        #     self._add_diff(_df=df, _diff_d=diff_d, _name=f'振幅2_bh_{n}')

        df['factor'] = \
            df[alpha_factors[0]] * (df[_dna[0]] + df[_dna[1]]) +\
            df[alpha_factors[1]] * (df[_dna[2]] + df[_dna[3]])
        return df


class BinanceNeutralStrategy_2(NeutralStrategyBase):

    @classmethod
    def strategy_with_parameters(cls, parameters):
        """初始化"""
        return BinanceNeutralStrategy_2(int(parameters[0]), f'{int(parameters[1])}h', float(parameters[2]))

    @property
    def display_name(self):
        return "BinanceNeutralStrategy_2"

    @property
    def candle_count_4_cal_factor(self):
        return 35 * 3 + 10

    def cal_factor(self, df):
        alpha_factors = ['bias_bh_12_diff_0.5', '涨跌幅_bh_3_diff_0.3']
        _dna = ['振幅_bh_48', '涨跌幅skew_bh_9', 'K_bh_12', 'J_bh_12_diff_0.7']

        # --- KDJ ---
        for (n, diff_d) in [(12, 0.7)]:
            # 正常K线数据 计算 KDJ
            low_list = df['low'].rolling(n, min_periods=1).min()  # 过去n(含当前行)行数据 最低价的最小值
            high_list = df['high'].rolling(n, min_periods=1).max()  # 过去n(含当前行)行数据 最高价的最大值
            rsv = (df['close'] - low_list) / (high_list - low_list) * 100  # 未成熟随机指标值
            df[f'K_bh_{n}'] = rsv.ewm(com=2).mean()  # K
            df[f'D_bh_{n}'] = df[f'K_bh_{n}'].ewm(com=2).mean()  # D
            df[f'J_bh_{n}'] = 3 * df[f'K_bh_{n}'] - 2 * df[f'D_bh_{n}']  # J

            #  差分
            # 使用差分后的K线数据 计算 KDJ  --- 标准差变大，数据更不稳定，放弃
            # 用计算后的KDJ指标，再差分  --- 标准差变小，数据更稳定，采纳
            for _ind in ['K', 'D', 'J']:
                self._add_diff(_df=df, _diff_d=diff_d, _name=f'{_ind}_bh_{n}')

        # --- bias ---  涨跌幅更好的表达方式 bias 币价偏离均线的比例。
        for (n, diff_d) in [(12, 0.5)]:
            ma = df['close'].rolling(n, min_periods=1).mean()
            df[f'bias_bh_{n}'] = (df['close'] / ma - 1)

            # 差分
            self._add_diff(_df=df, _diff_d=diff_d, _name=f'bias_bh_{n}')

        # --- 涨跌幅 ---
        for (hour, diff_d) in [(3, 0.3)]:
            df[f'涨跌幅_bh_{hour}'] = df['close'].pct_change(hour)

            # 差分
            self._add_diff(_df=df, _diff_d=diff_d, _name=f'涨跌幅_bh_{hour}')

        # --- 振幅 ---  最高价最低价
        for(n, diff_d) in [(48, 0.5)]:
            high = df['high'].rolling(n, min_periods=1).max()
            low = df['low'].rolling(n, min_periods=1).min()
            df[f'振幅_bh_{n}'] = (high / low - 1)

            # 差分
            self._add_diff(_df=df, _diff_d=diff_d, _name=f'振幅_bh_{n}')

        # --- 涨跌幅skew ---  在商品期货市场有效
        # skew偏度rolling最小周期为3才有数据
        for(n, diff_d) in [(9, 0.5)]:
            change = df['close'].pct_change()
            df[f'涨跌幅skew_bh_{n}'] = change.rolling(n).skew()

            # 差分
            self._add_diff(_df=df, _diff_d=diff_d, _name=f'涨跌幅skew_bh_{n}')

        df['factor'] = \
            df[alpha_factors[0]] * (df[_dna[0]] + df[_dna[1]]) +\
            df[alpha_factors[1]] * (df[_dna[2]] + df[_dna[3]])
        return df
