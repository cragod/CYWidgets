import pandas as pd
import numpy as np


def indicator_field_name(indicator, back_hour):
    return f'{indicator}_bh_{back_hour}'


# ===== 技术指标 =====


def kdj_indicator(df, back_hour_list, extra_agg_dict={}):
    # 正常K线数据 计算 KDJ
    for n in back_hour_list:
        low_list = df['low'].rolling(n, min_periods=1).min()  # 过去n(含当前行)行数据 最低价的最小值
        high_list = df['high'].rolling(n, min_periods=1).max()  # 过去n(含当前行)行数据 最高价的最大值
        rsv = (df['close'] - low_list) / (high_list - low_list) * 100  # 未成熟随机指标值

        df[f'K_bh_{n}'] = rsv.ewm(com=2).mean().shift(1)  # K
        extra_agg_dict[f'K_bh_{n}'] = 'first'

        df[f'D_bh_{n}'] = df[f'K_bh_{n}'].ewm(com=2).mean()  # D
        extra_agg_dict[f'D_bh_{n}'] = 'first'

        df[f'J_bh_{n}'] = 3 * df[f'K_bh_{n}'] - 2 * df[f'D_bh_{n}']  # J
        extra_agg_dict[f'J_bh_{n}'] = 'first'


def rsi_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- RSI ---  在期货市场很有效
    close_dif = df['close'].diff()
    df['up'] = np.where(close_dif > 0, close_dif, 0)
    df['down'] = np.where(close_dif < 0, abs(close_dif), 0)
    for n in back_hour_list:
        a = df['up'].rolling(n).sum()
        b = df['down'].rolling(n).sum()
        df[f'RSI_bh_{n}'] = (a / (a + b)).shift(1)  # RSI
        extra_agg_dict[f'RSI_bh_{n}'] = 'first'


def avg_price_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- 均价 ---  对应低价股策略(预计没什么用)
    # 策略改进思路：以下所有用到收盘价的因子，都可尝试使用均价代替
    for n in back_hour_list:
        df[f'均价_bh_{n}'] = (df['quote_volume'].rolling(n).sum() / df['volume'].rolling(n).sum()).shift(1)
        extra_agg_dict[f'均价_bh_{n}'] = 'first'


def 涨跌幅_indictor(df, back_hour_list, extra_agg_dict={}):
    for n in back_hour_list:
        df[f'涨跌幅_bh_{n}'] = df['close'].pct_change(n).shift(1)
        extra_agg_dict[f'涨跌幅_bh_{n}'] = 'first'


def bias_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- bias ---  涨跌幅更好的表达方式 bias 币价偏离均线的比例。
    for n in back_hour_list:
        ma = df['close'].rolling(n, min_periods=1).mean()
        df[f'bias_bh_{n}'] = (df['close'] / ma - 1).shift(1)
        extra_agg_dict[f'bias_bh_{n}'] = 'first'


def 振幅_indicator(df, back_hour_list, extra_agg_dict={}):
    for n in back_hour_list:
        high = df['high'].rolling(n, min_periods=1).max()
        low = df['low'].rolling(n, min_periods=1).min()
        df[f'振幅_bh_{n}'] = (high / low - 1).shift(1)
        extra_agg_dict[f'振幅_bh_{n}'] = 'first'


def 振幅2_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- 振幅2 ---  收盘价、开盘价
    high = df[['close', 'open']].max(axis=1)
    low = df[['close', 'open']].min(axis=1)
    for n in back_hour_list:
        high = high.rolling(n, min_periods=1).max()
        low = low.rolling(n, min_periods=1).min()
        df[f'振幅2_bh_{n}'] = (high / low - 1).shift(1)
        extra_agg_dict[f'振幅2_bh_{n}'] = 'first'


def 涨跌幅std_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- 涨跌幅std ---  振幅的另外一种形式
    change = df['close'].pct_change()
    for n in back_hour_list:
        df[f'涨跌幅std_bh_{n}'] = change.rolling(n).std().shift(1)
        extra_agg_dict[f'涨跌幅std_bh_{n}'] = 'first'


def 涨跌幅skew_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- 涨跌幅skew ---  在商品期货市场有效
    # skew偏度rolling最小周期为3才有数据
    change = df['close'].pct_change()
    for n in back_hour_list:
        df[f'涨跌幅skew_bh_{n}'] = change.rolling(n).skew().shift(1)
        extra_agg_dict[f'涨跌幅skew_bh_{n}'] = 'first'


def 成交额_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- 成交额 ---  对应小市值概念
    for n in back_hour_list:
        df[f'成交额_bh_{n}'] = df['quote_volume'].rolling(n, min_periods=1).sum().shift(1)
        extra_agg_dict[f'成交额_bh_{n}'] = 'first'


def 成交额std_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- 成交额std ---  191选股因子中最有效的因子
    for n in back_hour_list:
        df[f'成交额std_bh_{n}'] = df['quote_volume'].rolling(n, min_periods=2).std().shift(1)
        extra_agg_dict[f'成交额std_bh_{n}'] = 'first'


def 资金流入比例_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- 资金流入比例 --- 币安独有的数据
    for n in back_hour_list:
        volume = df['quote_volume'].rolling(n, min_periods=1).sum()
        buy_volume = df['taker_buy_quote_asset_volume'].rolling(n, min_periods=1).sum()
        df[f'资金流入比例_bh_{n}'] = (buy_volume / volume).shift(1)
        extra_agg_dict[f'资金流入比例_bh_{n}'] = 'first'


def 量比_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- 量比 ---
    for n in back_hour_list:
        df[f'量比_bh_{n}'] = (df['quote_volume'] / df['quote_volume'].rolling(n, min_periods=1).mean()).shift(1)
        extra_agg_dict[f'量比_bh_{n}'] = 'first'


def 成交笔数_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- 成交笔数 ---
    for n in back_hour_list:
        df[f'成交笔数_bh_{n}'] = df['trade_num'].rolling(n, min_periods=1).sum().shift(1)
        extra_agg_dict[f'成交笔数_bh_{n}'] = 'first'


def 量价相关系数_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- 量价相关系数 ---  量价相关选股策略
    for n in back_hour_list:
        df[f'量价相关系数_bh_{n}'] = df['close'].rolling(n).corr(df['quote_volume']).shift(1)
        extra_agg_dict[f'量价相关系数_bh_{n}'] = 'first'


def cci_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- cci ---  量价相关选股策略
    for n in back_hour_list:
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['ma'] = df['tp'].rolling(window=n, min_periods=1).mean()
        df['md'] = abs(df['close'] - df['ma']).rolling(window=n, min_periods=1).mean()
        df[f'cci_bh_{n}'] = (df['tp'] - df['ma']) / df['md'] / 0.015
        df[f'cci_bh_{n}'] = df[f'cci_bh_{n}'].shift(1)
        extra_agg_dict[f'cci_bh_{n}'] = 'first'


def psy_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- psy ---  量价相关选股策略
    for n in back_hour_list:
        df['rtn'] = df['close'].diff()
        df['up'] = np.where(df['rtn'] > 0, 1, 0)
        df[f'psy_bh_{n}'] = df['up'].rolling(window=n).sum() / n
        df[f'psy_bh_{n}'] = df[f'psy_bh_{n}'].shift(1)
        extra_agg_dict[f'psy_bh_{n}'] = 'first'


def cmo_indicator(df, back_hour_list, extra_agg_dict={}):
    # --- cmo ---  量价相关选股策略
    for n in back_hour_list:
        df['momentum'] = df['close'] - df['close'].shift(1)
        df['up'] = np.where(df['momentum'] > 0, df['momentum'], 0)
        df['dn'] = np.where(df['momentum'] < 0, abs(df['momentum']), 0)
        df['up_sum'] = df['up'].rolling(window=n, min_periods=1).sum()
        df['dn_sum'] = df['dn'].rolling(window=n, min_periods=1).sum()
        df[f'cmo_bh_{n}'] = (df['up_sum'] - df['dn_sum']) / (df['up_sum'] + df['dn_sum'])
        df[f'cmo_bh_{n}'] = df[f'cmo_bh_{n}'].shift(1)
        extra_agg_dict[f'cmo_bh_{n}'] = 'first'
