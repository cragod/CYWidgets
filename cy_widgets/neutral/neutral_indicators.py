import pandas as pd
import numpy as np
import talib as ta
from sklearn.linear_model import LinearRegression  # 版本0.0
from fracdiff import fdiff


def indicator_field_name(indicator, back_hour):
    return f'{indicator}_bh_{back_hour}'


def add_diff_columns(df, name, agg_dict, agg_type, diff_d=[0.3, 0.5, 0.7]):
    """ 为 数据列 添加 差分数据列
    :param _add:
    :param _df: 原数据 DataFrame
    :param _d_list: 差分阶数 [0.3, 0.5, 0.7]
    :param _name: 需要添加 差分值 的数据列 名称
    :param _agg_dict:
    :param _agg_type:
    :param _add:
    :return: """
    for d_num in diff_d:
        if len(df) >= 12:  # 数据行数大于等于12才进行差分操作
            _diff_ar = fdiff(df[name], n=d_num, window=10, mode="valid")  # 列差分，不使用未来数据
            _paddings = len(df) - len(_diff_ar)  # 差分后数据长度变短，需要在前面填充多少数据
            _diff = np.nan_to_num(np.concatenate((np.full(_paddings, 0), _diff_ar)), nan=0)  # 将所有nan替换为0
            df[name + f'_diff_{d_num}'] = _diff  # 将差分数据记录到 DataFrame
        else:
            df[name + f'_diff_{d_num}'] = np.nan  # 数据行数不足12的填充为空数据

        agg_dict[name + f'_diff_{d_num}'] = agg_type


def process_general_procedure(df, f_name, extra_agg_dict, add_diff):
    """处理通用流程"""
    extra_agg_dict[f_name] = 'first'
    if type(add_diff) is list:
        add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
    elif add_diff:
        add_diff_columns(df, f_name, extra_agg_dict, 'first')


# ===== 技术指标 =====


# def kdj_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
#     # 正常K线数据 计算 KDJ
#     for n in back_hour_list:
#         low_list = df['low'].rolling(n, min_periods=1).min()  # 过去n(含当前行)行数据 最低价的最小值
#         high_list = df['high'].rolling(n, min_periods=1).max()  # 过去n(含当前行)行数据 最高价的最大值
#         rsv = (df['close'] - low_list) / (high_list - low_list) * 100  # 未成熟随机指标值

#         df[f'K_bh_{n}'] = rsv.ewm(com=2).mean().shift(1 if need_shift else 0)  # K
#         extra_agg_dict[f'K_bh_{n}'] = 'first'

#         df[f'D_bh_{n}'] = df[f'K_bh_{n}'].ewm(com=2).mean()  # D
#         extra_agg_dict[f'D_bh_{n}'] = 'first'

#         df[f'J_bh_{n}'] = 3 * df[f'K_bh_{n}'] - 2 * df[f'D_bh_{n}']  # J
#         extra_agg_dict[f'J_bh_{n}'] = 'first'


# def avg_price_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
#     # --- 均价 ---  对应低价股策略(预计没什么用)
#     # 策略改进思路：以下所有用到收盘价的因子，都可尝试使用均价代替
#     for n in back_hour_list:
#         df[f'均价_bh_{n}'] = (df['quote_volume'].rolling(n).sum() / df['volume'].rolling(n).sum()).shift(1 if need_shift else 0)
#         extra_agg_dict[f'均价_bh_{n}'] = 'first'


# def 涨跌幅_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
#     for n in back_hour_list:
#         df[f'涨跌幅_bh_{n}'] = df['close'].pct_change(n).shift(1 if need_shift else 0)
#         extra_agg_dict[f'涨跌幅_bh_{n}'] = 'first'


# def 振幅_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
#     for n in back_hour_list:
#         high = df['high'].rolling(n, min_periods=1).max()
#         low = df['low'].rolling(n, min_periods=1).min()
#         df[f'振幅_bh_{n}'] = (high / low - 1).shift(1 if need_shift else 0)
#         extra_agg_dict[f'振幅_bh_{n}'] = 'first'


# def 振幅2_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
#     # --- 振幅2 ---  收盘价、开盘价
#     high = df[['close', 'open']].max(axis=1)
#     low = df[['close', 'open']].min(axis=1)
#     for n in back_hour_list:
#         high = high.rolling(n, min_periods=1).max()
#         low = low.rolling(n, min_periods=1).min()
#         df[f'振幅2_bh_{n}'] = (high / low - 1).shift(1 if need_shift else 0)
#         extra_agg_dict[f'振幅2_bh_{n}'] = 'first'


# def 涨跌幅std_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
#     # --- 涨跌幅std ---  振幅的另外一种形式
#     change = df['close'].pct_change()
#     for n in back_hour_list:
#         df[f'涨跌幅std_bh_{n}'] = change.rolling(n).std().shift(1 if need_shift else 0)
#         extra_agg_dict[f'涨跌幅std_bh_{n}'] = 'first'


# def 涨跌幅skew_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
#     # --- 涨跌幅skew ---  在商品期货市场有效
#     # skew偏度rolling最小周期为3才有数据
#     change = df['close'].pct_change()
#     for n in back_hour_list:
#         df[f'涨跌幅skew_bh_{n}'] = change.rolling(n).skew().shift(1 if need_shift else 0)
#         extra_agg_dict[f'涨跌幅skew_bh_{n}'] = 'first'


# def 成交额_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
#     # --- 成交额 ---  对应小市值概念
#     for n in back_hour_list:
#         df[f'成交额_bh_{n}'] = df['quote_volume'].rolling(n, min_periods=1).sum().shift(1 if need_shift else 0)
#         extra_agg_dict[f'成交额_bh_{n}'] = 'first'


# def 成交额std_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
#     # --- 成交额std ---  191选股因子中最有效的因子
#     for n in back_hour_list:
#         df[f'成交额std_bh_{n}'] = df['quote_volume'].rolling(n, min_periods=2).std().shift(1 if need_shift else 0)
#         extra_agg_dict[f'成交额std_bh_{n}'] = 'first'


# def 量比_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
#     # --- 量比 ---
#     for n in back_hour_list:
#         df[f'量比_bh_{n}'] = (df['quote_volume'] / df['quote_volume'].rolling(n, min_periods=1).mean()).shift(1 if need_shift else 0)
#         extra_agg_dict[f'量比_bh_{n}'] = 'first'


# def 成交笔数_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
#     # --- 成交笔数 ---
#     for n in back_hour_list:
#         df[f'成交笔数_bh_{n}'] = df['trade_num'].rolling(n, min_periods=1).sum().shift(1 if need_shift else 0)
#         extra_agg_dict[f'成交笔数_bh_{n}'] = 'first'


# def 量价相关系数_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
#     # --- 量价相关系数 ---  量价相关选股策略
#     for n in back_hour_list:
#         df[f'量价相关系数_bh_{n}'] = df['close'].rolling(n).corr(df['quote_volume']).shift(1 if need_shift else 0)
#         extra_agg_dict[f'量价相关系数_bh_{n}'] = 'first'

def 资金流入比例_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # --- 资金流入比例 --- 币安独有的数据, n
    for n in back_hour_list:
        volume = df['quote_volume'].rolling(n, min_periods=1).sum()
        buy_volume = df['taker_buy_quote_asset_volume'].rolling(n, min_periods=1).sum()
        f_name = f'资金流入比例_bh_{n}'
        df[f_name] = (buy_volume / volume).shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def rsi_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # --- RSI ---  在期货市场很有效
    close_dif = df['close'].diff()
    df['up'] = np.where(close_dif > 0, close_dif, 0)
    df['down'] = np.where(close_dif < 0, abs(close_dif), 0)
    for n in back_hour_list:
        a = df['up'].rolling(n).sum()
        b = df['down'].rolling(n).sum()

        f_name = f'rsi_bh_{n}'
        df[f_name] = (a / (a + b)).shift(1 if need_shift else 0)  # RSI
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def bias_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # --- bias ---  涨跌幅更好的表达方式 bias 币价偏离均线的比例。n
    for n in back_hour_list:
        f_name = f'bias_bh_{n}'
        ma = df['close'].rolling(n, min_periods=1).mean()
        df[f_name] = (df['close'] / ma - 1).shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def cci_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # --- cci ---  量价相关选股策略 2*n
    for n in back_hour_list:
        f_name = f'cci_bh_{n}'
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['ma'] = df['tp'].rolling(window=n, min_periods=1).mean()
        df['md'] = abs(df['close'] - df['ma']).rolling(window=n, min_periods=1).mean()
        df[f_name] = (df['tp'] - df['ma']) / df['md'] / 0.015
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def cci_ema_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # magic_cci
    for n in back_hour_list:
        """
        N=14
        TP=(HIGH+LOW+CLOSE)/3
        MA=MA(TP,N)
        MD=MA(ABS(TP-MA),N)
        CCI=(TP-MA)/(0.015MD)
        CCI 指标用来衡量典型价格（最高价、最低价和收盘价的均值）与其
        一段时间的移动平均的偏离程度。CCI 可以用来反映市场的超买超卖
        状态。一般认为，CCI 超过 100 则市场处于超买状态；CCI 低于-100
        则市场处于超卖状态。当 CCI 下穿 100/上穿-100 时，说明股价可能
        要开始发生反转，可以考虑卖出/买入。
        """
        df['oma'] = df['open'].ewm(span=n, adjust=False).mean()  # 取 open 的ema
        df['hma'] = df['high'].ewm(span=n, adjust=False).mean()  # 取 high 的ema
        df['lma'] = df['low'].ewm(span=n, adjust=False).mean()  # 取 low的ema
        df['cma'] = df['close'].ewm(span=n, adjust=False).mean()  # 取 close的ema
        df['tp'] = (df['oma'] + df['hma'] + df['lma'] + df[
            'cma']) / 4  # 魔改CCI基础指标 将TP=(HIGH+LOW+CLOSE)/3  替换成以open/high/low/close的ema 的均值
        df['ma'] = df['tp'].ewm(span=n, adjust=False).mean()  # MA(TP,N)  将移动平均改成 ema
        df['abs_diff_close'] = abs(df['tp'] - df['ma'])  # ABS(TP-MA)
        df['md'] = df['abs_diff_close'].ewm(span=n, adjust=False).mean()  # MD=MA(ABS(TP-MA),N)  将移动平均替换成ema

        f_name = f'cci_ema_bh_{n}'
        df[f_name] = (df['tp'] - df['ma']) / df['md']  # CCI=(TP-MA)/(0.015MD)  CCI在一定范围内
        df[f_name] = df[f_name].shift(1 if need_shift else 0)  # 取前一周期防止未来函数  实盘中不需要
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # # 删除中间数据
        del df['oma']
        del df['hma']
        del df['lma']
        del df['cma']
        del df['tp']
        del df['ma']
        del df['abs_diff_close']
        del df['md']


def psy_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # --- psy ---  量价相关选股策略
    for n in back_hour_list:
        f_name = f'psy_bh_{n}'
        df['rtn'] = df['close'].diff()
        df['up'] = np.where(df['rtn'] > 0, 1, 0)
        df[f_name] = df['up'].rolling(window=n).sum() / n
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def cmo_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # --- cmo ---  量价相关选股策略
    for n in back_hour_list:
        f_name = f'cmo_bh_{n}'
        df['momentum'] = df['close'] - df['close'].shift(1)
        df['up'] = np.where(df['momentum'] > 0, df['momentum'], 0)
        df['dn'] = np.where(df['momentum'] < 0, abs(df['momentum']), 0)
        df['up_sum'] = df['up'].rolling(window=n, min_periods=1).sum()
        df['dn_sum'] = df['dn'].rolling(window=n, min_periods=1).sum()
        df[f_name] = (df['up_sum'] - df['dn_sum']) / (df['up_sum'] + df['dn_sum'])
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def vma_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # VMA 指标, n
    for n in back_hour_list:
        """
        N=20
        PRICE=(HIGH+LOW+OPEN+CLOSE)/4
        VMA=MA(PRICE,N)
        VMA 就是简单移动平均把收盘价替换为最高价、最低价、开盘价和
        收盘价的平均值。当 PRICE 上穿/下穿 VMA 时产生买入/卖出信号。
        """
        f_name = f'vma_bh_{n}'
        price = (df['high'] + df['low'] + df['open'] + df['close']) / 4  # PRICE=(HIGH+LOW+OPEN+CLOSE)/4
        vma = price.rolling(n, min_periods=1).mean()  # VMA=MA(PRICE,N)
        df[f_name] = price / vma - 1  # 去量纲
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def pmo_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # PMO 指标, 8*n
    for n in back_hour_list:
        """
        N1=10
        N2=40
        N3=20
        ROC=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*100
        ROC_MA=DMA(ROC,2/N1)
        ROC_MA10=ROC_MA*10
        PMO=DMA(ROC_MA10,2/N2)
        PMO_SIGNAL=DMA(PMO,2/(N3+1))
        PMO 指标是 ROC 指标的双重平滑（移动平均）版本。与 SROC 不 同(SROC 是先对价格作平滑再求 ROC)，而 PMO 是先求 ROC 再对
        ROC 作平滑处理。PMO 越大（大于 0），则说明市场上涨趋势越强；
        PMO 越小（小于 0），则说明市场下跌趋势越强。如果 PMO 上穿/
        下穿其信号线，则产生买入/卖出指标。
        """
        f_name = f'pmo_bh_{n}'
        df['ROC'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1) * \
            100  # ROC=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*100
        df['ROC_MA'] = df['ROC'].rolling(n, min_periods=1).mean()  # ROC_MA=DMA(ROC,2/N1)
        df['ROC_MA10'] = df['ROC_MA'] * 10  # ROC_MA10=ROC_MA*10
        df['PMO'] = df['ROC_MA10'].rolling(4 * n, min_periods=1).mean()  # PMO=DMA(ROC_MA10,2/N2)
        df['PMO_SIGNAL'] = df['PMO'].rolling(2 * n, min_periods=1).mean()  # PMO_SIGNAL=DMA(PMO,2/(N3+1))

        df[f_name] = df['PMO_SIGNAL'].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间过渡数据
        del df['ROC']
        del df['ROC_MA']
        del df['ROC_MA10']
        del df['PMO']
        del df['PMO_SIGNAL']


def reg_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # REG 指标, n
    for n in back_hour_list:
        """
        N=40
        X=[1,2,...,N]
        Y=[REF(CLOSE,N-1),...,REF(CLOSE,1),CLOSE]
        做回归得 REG_CLOSE=aX+b
        REG=(CLOSE-REG_CLOSE)/REG_CLOSE
        在过去的 N 天内收盘价对序列[1,2,...,N]作回归得到回归直线，当收盘
        价超过回归直线的一定范围时买入，低过回归直线的一定范围时卖
        出。如果 REG 上穿 0.05/下穿-0.05 则产生买入/卖出信号。
        """
        f_name = f'reg_bh_{n}'
        # sklearn 线性回归

        def reg_ols(_y, n):
            _x = np.arange(n) + 1
            model = LinearRegression().fit(_x.reshape(-1, 1), _y)  # 线性回归训练
            y_pred = model.coef_ * _x + model.intercept_  # y = ax + b
            return y_pred[-1]

        df['reg_close'] = df['close'].rolling(n).apply(lambda y: reg_ols(y, n))  # 求数据拟合的线性回归
        df['reg'] = df['close'] / df['reg_close'] - 1

        df[f_name] = df['reg'].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间过程数据
        del df['reg']
        del df['reg_close']


def reg_ta_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # REG 指标, n
    for n in back_hour_list:
        """
        N=40
        X=[1,2,...,N]
        Y=[REF(CLOSE,N-1),...,REF(CLOSE,1),CLOSE]
        做回归得 REG_CLOSE=aX+b
        REG=(CLOSE-REG_CLOSE)/REG_CLOSE
        在过去的 N 天内收盘价对序列[1,2,...,N]作回归得到回归直线，当收盘
        价超过回归直线的一定范围时买入，低过回归直线的一定范围时卖
        出。如果 REG 上穿 0.05/下穿-0.05 则产生买入/卖出信号。
        """
        f_name = f'reg_ta_bh_{n}'
        df['reg_close'] = ta.LINEARREG(df['close'], timeperiod=n)  # 该部分为talib内置求线性回归
        df['reg'] = df['close'] / df['reg_close'] - 1

        # # sklearn 线性回归
        # def reg_ols(_y, n):
        #     _x = np.arange(n) + 1
        #     model = LinearRegression().fit(_x.reshape(-1, 1), _y)  # 线性回归训练
        #     y_pred = model.coef_ * _x + model.intercept_  # y = ax + b
        #     return y_pred[-1]

        # df['reg_close'] = df['close'].rolling(n).apply(lambda y: reg_ols(y, n))  # 求数据拟合的线性回归
        # df['reg'] = df['close'] / df['reg_close'] - 1

        df[f_name] = df['reg'].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间过程数据
        del df['reg']
        del df['reg_close']


def dema_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # DEMA 指标
    for n in back_hour_list:
        """
        N=60
        EMA=EMA(CLOSE,N)
        DEMA=2*EMA-EMA(EMA,N)
        DEMA 结合了单重 EMA 和双重 EMA，在保证平滑性的同时减少滞后
        性。
        """
        f_name = f'dema_bh_{n}'
        ema = df['close'].ewm(n, adjust=False).mean()  # EMA=EMA(CLOSE,N)
        ema_ema = ema.ewm(n, adjust=False).mean()  # EMA(EMA,N)
        dema = 2 * ema - ema_ema  # DEMA=2*EMA-EMA(EMA,N)
        # dema 去量纲
        df[f_name] = dema / ema - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def cr_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # CR 指标
    for n in back_hour_list:
        """
        N=20
        TYP=(HIGH+LOW+CLOSE)/3
        H=MAX(HIGH-REF(TYP,1),0)
        L=MAX(REF(TYP,1)-LOW,0)
        CR=SUM(H,N)/SUM(L,N)*100
        CR 与 AR、BR 类似。CR 通过比较最高价、最低价和典型价格来衡
        量市场人气，其衡量昨日典型价格在今日最高价、最低价之间的位置。
        CR 超过 200 时，表示股价上升强势；CR 低于 50 时，表示股价下跌
        强势。如果 CR 上穿 200/下穿 50 则产生买入/卖出信号。
        """
        f_name = f'cr_bh_{n}'
        df['TYP'] = (df['high'] + df['low'] + df['close']) / 3  # TYP=(HIGH+LOW+CLOSE)/3
        df['H_TYP'] = df['high'] - df['TYP'].shift(1)  # HIGH-REF(TYP,1)
        df['H'] = np.where(df['high'] > df['TYP'].shift(1), df['H_TYP'], 0)  # H=MAX(HIGH-REF(TYP,1),0)
        df['L_TYP'] = df['TYP'].shift(1) - df['low']  # REF(TYP,1)-LOW
        df['L'] = np.where(df['TYP'].shift(1) > df['low'], df['L_TYP'], 0)  # L=MAX(REF(TYP,1)-LOW,0)
        df['CR'] = df['H'].rolling(n).sum() / df['L'].rolling(n).sum() * 100  # CR=SUM(H,N)/SUM(L,N)*100
        df[f_name] = df['CR'].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间数据
        del df['TYP']
        del df['H_TYP']
        del df['H']
        del df['L_TYP']
        del df['L']
        del df['CR']


def bop_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # BOP 指标
    for n in back_hour_list:
        """
        N=20
        BOP=MA((CLOSE-OPEN)/(HIGH-LOW),N)
        BOP 的变化范围为-1 到 1，用来衡量收盘价与开盘价的距离（正、负
        距离）占最高价与最低价的距离的比例，反映了市场的多空力量对比。
        如果 BOP>0，则多头更占优势；BOP<0 则说明空头更占优势。BOP
        越大，则说明价格被往最高价的方向推动得越多；BOP 越小，则说
        明价格被往最低价的方向推动得越多。我们可以用 BOP 上穿/下穿 0
        线来产生买入/卖出信号。
        """
        f_name = f'bop_bh_{n}'
        df['co'] = df['close'] - df['open']  # CLOSE-OPEN
        df['hl'] = df['high'] - df['low']  # HIGH-LOW
        df['BOP'] = (df['co'] / df['hl']).rolling(n, min_periods=1).mean()  # BOP=MA((CLOSE-OPEN)/(HIGH-LOW),N)

        df[f_name] = df['BOP'].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间过程数据
        del df['co']
        del df['hl']
        del df['BOP']


def hullma_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # HULLMA 指标
    for n in back_hour_list:
        """
        N=20,80
        X=2*EMA(CLOSE,[N/2])-EMA(CLOSE,N)
        HULLMA=EMA(X,[√𝑁])
        HULLMA 也是均线的一种，相比于普通均线有着更低的延迟性。我们
        用短期均线上/下穿长期均线来产生买入/卖出信号。
        """
        f_name = f'hullma_bh_{n}'
        ema1 = df['close'].ewm(n, adjust=False).mean()  # EMA(CLOSE,[N/2])
        ema2 = df['close'].ewm(n * 2, adjust=False).mean()  # EMA(CLOSE,N)
        df['X'] = 2 * ema1 - ema2  # X=2*EMA(CLOSE,[N/2])-EMA(CLOSE,N)
        df['HULLMA'] = df['X'].ewm(int(np.sqrt(2 * n)), adjust=False).mean()  # HULLMA=EMA(X,[√𝑁])
        # 去量纲
        df[f_name] = df['HULLMA'].shift(1) - 1
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除过程数据
        del df['X']
        del df['HULLMA']


def angle_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # --- Angle ---
    for n in back_hour_list:
        f_name = f'angle_bh_{n}'
        ma = df['close'].rolling(window=n, min_periods=1).mean()
        df[f_name] = ta.LINEARREG_ANGLE(ma, n)
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def gap_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # ---- Gap, n*3 ----
    for n in back_hour_list:
        ma = df['close'].rolling(window=n, min_periods=1).mean()
        wma = ta.WMA(df['close'], n)
        gap = wma - ma
        f_name = f'gap_bh_{n}'
        df[f_name] = gap / abs(gap).rolling(window=n).sum()
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def 癞子_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # ---- 癞子 ----
    for n in back_hour_list:
        diff = df['close'] / df['close'].shift(1) - 1
        f_name = f'癞子_bh_{n}'
        df[f_name] = diff / abs(diff).rolling(window=n).sum()
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def pac_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # PAC 指标
    for n in back_hour_list:
        """
        N1=20
        N2=20
        UPPER=SMA(HIGH,N1,1)
        LOWER=SMA(LOW,N2,1)
        用最高价和最低价的移动平均来构造价格变化的通道，如果价格突破
        上轨则做多，突破下轨则做空。
        """
        f_name = f'pac_bh_{n}'
        # upper = df['high'].rolling(n, min_periods=1).mean()
        df['upper'] = df['high'].ewm(span=n).mean()  # UPPER=SMA(HIGH,N1,1)
        # lower = df['low'].rolling(n, min_periods=1).mean()
        df['lower'] = df['low'].ewm(span=n).mean()  # LOWER=SMA(LOW,N2,1)
        df['width'] = df['upper'] - df['lower']  # 添加指标求宽度进行去量纲
        df['width_ma'] = df['width'].rolling(n, min_periods=1).mean()

        df[f_name] = df['width'] / df['width_ma'] - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间数据
        del df['upper']
        del df['lower']
        del df['width']
        del df['width_ma']


def ddi_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # DDI
    for n in back_hour_list:
        """
        n = 40
        HL=HIGH+LOW
        HIGH_ABS=ABS(HIGH-REF(HIGH,1))
        LOW_ABS=ABS(LOW-REF(LOW,1))
        DMZ=IF(HL>REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
        DMF=IF(HL<REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
        DIZ=SUM(DMZ,N)/(SUM(DMZ,N)+SUM(DMF,N))
        DIF=SUM(DMF,N)/(SUM(DMZ,N)+SUM(DMF,N))
        DDI=DIZ-DIF
        DDI 指标用来比较向上波动和向下波动的比例。如果 DDI 上穿/下穿 0
        则产生买入/卖出信号。
        """
        f_name = f'ddi_bh_{n}'
        df['hl'] = df['high'] + df['low']  # HL=HIGH+LOW
        df['abs_high'] = abs(df['high'] - df['high'].shift(1))  # HIGH_ABS=ABS(HIGH-REF(HIGH,1))
        df['abs_low'] = abs(df['low'] - df['low'].shift(1))  # LOW_ABS=ABS(LOW-REF(LOW,1))
        max_value1 = df[['abs_high', 'abs_low']].max(axis=1)  # MAX(HIGH_ABS,LOW_ABS)
        # df.loc[df['hl'] > df['hl'].shift(1), 'DMZ'] = max_value1
        # df['DMZ'].fillna(value=0, inplace=True)
        # DMZ=IF(HL>REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
        df['DMZ'] = np.where((df['hl'] > df['hl'].shift(1)), max_value1, 0)
        # df.loc[df['hl'] < df['hl'].shift(1), 'DMF'] = max_value1
        # df['DMF'].fillna(value=0, inplace=True)
        # DMF=IF(HL<REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
        df['DMF'] = np.where((df['hl'] < df['hl'].shift(1)), max_value1, 0)

        DMZ_SUM = df['DMZ'].rolling(n).sum()  # SUM(DMZ,N)
        DMF_SUM = df['DMF'].rolling(n).sum()  # SUM(DMF,N)
        DIZ = DMZ_SUM / (DMZ_SUM + DMF_SUM)  # DIZ=SUM(DMZ,N)/(SUM(DMZ,N)+SUM(DMF,N))
        DIF = DMF_SUM / (DMZ_SUM + DMF_SUM)  # DIF=SUM(DMF,N)/(SUM(DMZ,N)+SUM(DMF,N))
        df[f_name] = DIZ - DIF
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间数据
        del df['hl']
        del df['abs_high']
        del df['abs_low']
        del df['DMZ']
        del df['DMF']


def dc_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # DC 指标
    for n in back_hour_list:
        """
        N=20
        UPPER=MAX(HIGH,N)
        LOWER=MIN(LOW,N)
        MIDDLE=(UPPER+LOWER)/2
        DC 指标用 N 天最高价和 N 天最低价来构造价格变化的上轨和下轨，
        再取其均值作为中轨。当收盘价上穿/下穿中轨时产生买入/卖出信号。
        """
        f_name = f'dc_bh_{n}'
        upper = df['high'].rolling(n, min_periods=1).max()  # UPPER=MAX(HIGH,N)
        lower = df['low'].rolling(n, min_periods=1).min()  # LOWER=MIN(LOW,N)
        middle = (upper + lower) / 2  # MIDDLE=(UPPER+LOWER)/2
        ma_middle = middle.rolling(n, min_periods=1).mean()  # 求中轨的均线
        # 进行无量纲处理
        df[f_name] = middle / ma_middle - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def v3_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # v3
    for n1 in back_hour_list:
        df['mtm'] = df['close'] / df['close'].shift(n1) - 1
        df['mtm_mean'] = df['mtm'].rolling(window=n1, min_periods=1).mean()

        # 基于价格atr，计算波动率因子wd_atr
        df['c1'] = df['high'] - df['low']
        df['c2'] = abs(df['high'] - df['close'].shift(1))
        df['c3'] = abs(df['low'] - df['close'].shift(1))
        df['tr'] = df[['c1', 'c2', 'c3']].max(axis=1)
        df['atr'] = df['tr'].rolling(window=n1, min_periods=1).mean()
        df['atr_avg_price'] = df['close'].rolling(window=n1, min_periods=1).mean()
        df['wd_atr'] = df['atr'] / df['atr_avg_price']

        # 参考ATR，对MTM指标，计算波动率因子
        df['mtm_l'] = df['low'] / df['low'].shift(n1) - 1
        df['mtm_h'] = df['high'] / df['high'].shift(n1) - 1
        df['mtm_c'] = df['close'] / df['close'].shift(n1) - 1
        df['mtm_c1'] = df['mtm_h'] - df['mtm_l']
        df['mtm_c2'] = abs(df['mtm_h'] - df['mtm_c'].shift(1))
        df['mtm_c3'] = abs(df['mtm_l'] - df['mtm_c'].shift(1))
        df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
        df['mtm_atr'] = df['mtm_tr'].rolling(window=n1, min_periods=1).mean()

        # 参考ATR，对MTM mean指标，计算波动率因子
        df['mtm_l_mean'] = df['mtm_l'].rolling(window=n1, min_periods=1).mean()
        df['mtm_h_mean'] = df['mtm_h'].rolling(window=n1, min_periods=1).mean()
        df['mtm_c_mean'] = df['mtm_c'].rolling(window=n1, min_periods=1).mean()
        df['mtm_c1'] = df['mtm_h_mean'] - df['mtm_l_mean']
        df['mtm_c2'] = abs(df['mtm_h_mean'] - df['mtm_c_mean'].shift(1))
        df['mtm_c3'] = abs(df['mtm_l_mean'] - df['mtm_c_mean'].shift(1))
        df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
        df['mtm_atr_mean'] = df['mtm_tr'].rolling(window=n1, min_periods=1).mean()

        indicator = 'mtm_mean'

        # mtm_mean指标分别乘以三个波动率因子
        df[indicator] = 1e5 * df['mtm_atr'] * df['mtm_atr_mean'] * df['wd_atr'] * df[indicator]

        f_name = f'v3_bh_{n1}'
        df[f_name] = df[indicator].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间数据
        del df['mtm']
        del df['mtm_mean']
        del df['c1']
        del df['c2']
        del df['c3']
        del df['tr']
        del df['atr']
        del df['atr_avg_price']
        del df['wd_atr']
        del df['mtm_l']
        del df['mtm_h']
        del df['mtm_c']
        del df['mtm_c1']
        del df['mtm_c2']
        del df['mtm_c3']
        del df['mtm_tr']
        del df['mtm_atr']
        del df['mtm_l_mean']
        del df['mtm_h_mean']
        del df['mtm_c_mean']
        del df['mtm_atr_mean']


def v1_up_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # v1 上轨
    for n in back_hour_list:
        n1 = n

        # 计算动量因子
        mtm = df['close'] / df['close'].shift(n1) - 1
        mtm_mean = mtm.rolling(window=n1, min_periods=1).mean()

        # 基于价格atr，计算波动率因子wd_atr
        c1 = df['high'] - df['low']
        c2 = abs(df['high'] - df['close'].shift(1))
        c3 = abs(df['low'] - df['close'].shift(1))
        tr = np.max(np.array([c1, c2, c3]), axis=0)  # 三个数列取其大值
        atr = pd.Series(tr).rolling(window=n1, min_periods=1).mean()
        avg_price = df['close'].rolling(window=n1, min_periods=1).mean()
        wd_atr = atr / avg_price  # === 波动率因子

        # 参考ATR，对MTM指标，计算波动率因子
        mtm_l = df['low'] / df['low'].shift(n1) - 1
        mtm_h = df['high'] / df['high'].shift(n1) - 1
        mtm_c = df['close'] / df['close'].shift(n1) - 1
        mtm_c1 = mtm_h - mtm_l
        mtm_c2 = abs(mtm_h - mtm_c.shift(1))
        mtm_c3 = abs(mtm_l - mtm_c.shift(1))
        mtm_tr = np.max(np.array([mtm_c1, mtm_c2, mtm_c3]), axis=0)  # 三个数列取其大值
        mtm_atr = pd.Series(mtm_tr).rolling(window=n1, min_periods=1).mean()  # === mtm 波动率因子

        # 参考ATR，对MTM mean指标，计算波动率因子
        mtm_l_mean = mtm_l.rolling(window=n1, min_periods=1).mean()
        mtm_h_mean = mtm_h.rolling(window=n1, min_periods=1).mean()
        mtm_c_mean = mtm_c.rolling(window=n1, min_periods=1).mean()
        mtm_c1 = mtm_h_mean - mtm_l_mean
        mtm_c2 = abs(mtm_h_mean - mtm_c_mean.shift(1))
        mtm_c3 = abs(mtm_l_mean - mtm_c_mean.shift(1))
        mtm_tr = np.max(np.array([mtm_c1, mtm_c2, mtm_c3]), axis=0)  # 三个数列取其大值
        mtm_atr_mean = pd.Series(mtm_tr).rolling(window=n1, min_periods=1).mean()  # === mtm_mean 波动率因子

        indicator = mtm_mean
        # mtm_mean指标分别乘以三个波动率因子
        indicator *= wd_atr * mtm_atr * mtm_atr_mean
        indicator = pd.Series(indicator)

        # 对新策略因子计算自适应布林
        median = indicator.rolling(window=n1, min_periods=1).mean()
        std = indicator.rolling(n1, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
        z_score = abs(indicator - median) / std
        m1 = pd.Series(z_score).rolling(window=n1, min_periods=1).max()
        up1 = median + std * m1
        factor1 = up1 - indicator
        factor1 = factor1 * 1e8

        f_name = f'v1_up_bh_{n}'
        df[f_name] = factor1.shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def rccd_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # RCCD 指标, 8*n
    for n in back_hour_list:
        """
        M=40
        N1=20
        N2=40
        RC=CLOSE/REF(CLOSE,M)
        ARC1=SMA(REF(RC,1),M,1)
        DIF=MA(REF(ARC1,1),N1)-MA(REF(ARC1,1),N2)
        RCCD=SMA(DIF,M,1)
        RC 指标为当前价格与昨日价格的比值。当 RC 指标>1 时，说明价格在上升；当 RC 指标增大时，说明价格上升速度在增快。当 RC 指标
        <1 时，说明价格在下降；当 RC 指标减小时，说明价格下降速度在增
        快。RCCD 指标先对 RC 指标进行平滑处理，再取不同时间长度的移
        动平均的差值，再取移动平均。如 RCCD 上穿/下穿 0 则产生买入/
        卖出信号。
        """
        df['RC'] = df['close'] / df['close'].shift(2 * n)  # RC=CLOSE/REF(CLOSE,M)
        # df['ARC1'] = df['RC'].rolling(2 * n, min_periods=1).mean()
        df['ARC1'] = df['RC'].ewm(span=2 * n).mean()  # ARC1=SMA(REF(RC,1),M,1)
        df['MA1'] = df['ARC1'].shift(1).rolling(n, min_periods=1).mean()  # MA(REF(ARC1,1),N1)
        df['MA2'] = df['ARC1'].shift(1).rolling(2 * n, min_periods=1).mean()  # MA(REF(ARC1,1),N2)
        df['DIF'] = df['MA1'] - df['MA2']  # DIF=MA(REF(ARC1,1),N1)-MA(REF(ARC1,1),N2)
        # df['RCCD'] = df['DIF'].rolling(2 * n, min_periods=1).mean()
        df['RCCD'] = df['DIF'].ewm(span=2 * n).mean()  # RCCD=SMA(DIF,M,1)

        f_name = f'rccd_bh_{n}'
        df[f_name] = df['RCCD'].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间数据
        del df['RC']
        del df['ARC1']
        del df['MA1']
        del df['MA2']
        del df['DIF']
        del df['RCCD']


def vidya_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # VIDYA, 2*n
    for n in back_hour_list:
        """
        N=10
        VI=ABS(CLOSE-REF(CLOSE,N))/SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        VIDYA=VI*CLOSE+(1-VI)*REF(CLOSE,1)
        VIDYA 也属于均线的一种，不同的是，VIDYA 的权值加入了 ER
        （EfficiencyRatio）指标。在当前趋势较强时，ER 值较大，VIDYA
        会赋予当前价格更大的权重，使得 VIDYA 紧随价格变动，减小其滞
        后性；在当前趋势较弱（比如振荡市中）,ER 值较小，VIDYA 会赋予
        当前价格较小的权重，增大 VIDYA 的滞后性，使其更加平滑，避免
        产生过多的交易信号。
        当收盘价上穿/下穿 VIDYA 时产生买入/卖出信号。
        """
        df['abs_diff_close'] = abs(df['close'] - df['close'].shift(n))  # ABS(CLOSE-REF(CLOSE,N))
        df['abs_diff_close_sum'] = df['abs_diff_close'].rolling(n).sum()  # SUM(ABS(CLOSE-REF(CLOSE,1))
        # VI=ABS(CLOSE-REF(CLOSE,N))/SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        VI = df['abs_diff_close'] / df['abs_diff_close_sum']
        VIDYA = VI * df['close'] + (1 - VI) * df['close'].shift(1)  # VIDYA=VI*CLOSE+(1-VI)*REF(CLOSE,1)
        # 进行无量纲处理
        f_name = f'vidya_bh_{n}'
        df[f_name] = VIDYA / df['close'] - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间数据
        del df['abs_diff_close']
        del df['abs_diff_close_sum']


def apz_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # APZ 指标, 4*n
    for n in back_hour_list:
        """
        N=10
        M=20
        PARAM=2
        VOL=EMA(EMA(HIGH-LOW,N),N)
        UPPER=EMA(EMA(CLOSE,M),M)+PARAM*VOL
        LOWER= EMA(EMA(CLOSE,M),M)-PARAM*VOL
        APZ（Adaptive Price Zone 自适应性价格区间）与布林线 Bollinger
        Band 和肯通纳通道 Keltner Channel 很相似，都是根据价格波动性围
        绕均线而制成的价格通道。只是在这三个指标中计算价格波动性的方
        法不同。在布林线中用了收盘价的标准差，在肯通纳通道中用了真波
        幅 ATR，而在 APZ 中运用了最高价与最低价差值的 N 日双重指数平
        均来反映价格的波动幅度。
        """
        df['hl'] = df['high'] - df['low']  # HIGH-LOW,
        df['ema_hl'] = df['hl'].ewm(n, adjust=False).mean()  # EMA(HIGH-LOW,N)
        df['vol'] = df['ema_hl'].ewm(n, adjust=False).mean()  # VOL=EMA(EMA(HIGH-LOW,N),N)

        # 计算通道 可以作为CTA策略 作为因子的时候进行改造
        df['ema_close'] = df['close'].ewm(2 * n, adjust=False).mean()  # EMA(CLOSE,M)
        df['ema_ema_close'] = df['ema_close'].ewm(2 * n, adjust=False).mean()  # EMA(EMA(CLOSE,M),M)
        # EMA去量纲
        f_name = f'apz_bh_{n}'
        df[f_name] = df['vol'] / df['ema_ema_close']
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间数据
        del df['hl']
        del df['ema_hl']
        del df['vol']
        del df['ema_close']
        del df['ema_ema_close']


def rwih_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # RWI 指标, n
    for n in back_hour_list:
        """
        N=14
        TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(
        CLOSE,1)-LOW))
        ATR=MA(TR,N)
        RWIH=(HIGH-REF(LOW,1))/(ATR*√N)
        RWIL=(REF(HIGH,1)-LOW)/(ATR*√N)
        RWI（随机漫步指标）对一段时间股票的随机漫步区间与真实运动区
        间进行比较以判断股票价格的走势。
        如果 RWIH>1，说明股价长期是上涨趋势，则产生买入信号；
        如果 RWIL>1，说明股价长期是下跌趋势，则产生卖出信号。
        """
        df['c1'] = abs(df['high'] - df['low'])  # ABS(HIGH-LOW)
        df['c2'] = abs(df['close'] - df['close'].shift(1))  # ABS(HIGH-REF(CLOSE,1))
        df['c3'] = abs(df['high'] - df['close'].shift(1))  # ABS(REF(CLOSE,1)-LOW)
        # TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(CLOSE,1)-LOW))
        df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)
        df['ATR'] = df['TR'].rolling(n, min_periods=1).mean()  # ATR=MA(TR,N)
        df['RWIH'] = (df['high'] - df['low'].shift(1)) / (df['ATR'] * np.sqrt(n))  # RWIH=(HIGH-REF(LOW,1))/(ATR*√N)

        f_name = f'rwih_bh_{n}'
        df[f_name] = df['RWIH'].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间过程数据
        del df['c1']
        del df['c2']
        del df['c3']
        del df['TR']
        del df['ATR']
        del df['RWIH']


def rwil_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # RWI 指标, n
    for n in back_hour_list:
        """
        N=14
        TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(
        CLOSE,1)-LOW))
        ATR=MA(TR,N)
        RWIH=(HIGH-REF(LOW,1))/(ATR*√N)
        RWIL=(REF(HIGH,1)-LOW)/(ATR*√N)
        RWI（随机漫步指标）对一段时间股票的随机漫步区间与真实运动区
        间进行比较以判断股票价格的走势。
        如果 RWIH>1，说明股价长期是上涨趋势，则产生买入信号；
        如果 RWIL>1，说明股价长期是下跌趋势，则产生卖出信号。
        """
        df['c1'] = abs(df['high'] - df['low'])  # ABS(HIGH-LOW)
        df['c2'] = abs(df['close'] - df['close'].shift(1))  # ABS(HIGH-REF(CLOSE,1))
        df['c3'] = abs(df['high'] - df['close'].shift(1))  # ABS(REF(CLOSE,1)-LOW)
        # TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(CLOSE,1)-LOW))
        df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)
        df['ATR'] = df['TR'].rolling(n, min_periods=1).mean()  # ATR=MA(TR,N)
        df['RWIL'] = (df['high'].shift(1) - df['low']) / (df['ATR'] * np.sqrt(n))  # RWIL=(REF(HIGH,1)-LOW)/(ATR*√N)

        f_name = f'rwil_bh_{n}'
        df[f_name] = df['RWIL'].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间过程数据
        del df['c1']
        del df['c2']
        del df['c3']
        del df['TR']
        del df['ATR']
        del df['RWIL']


def ma_displaced_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # ma_displaced 2*n
    for n in back_hour_list:
        """
        N=20
        M=10
        MA_CLOSE=MA(CLOSE,N)
        MADisplaced=REF(MA_CLOSE,M)
        MADisplaced 指标把简单移动平均线向前移动了 M 个交易日，用法
        与一般的移动平均线一样。如果收盘价上穿/下穿 MADisplaced 则产
        生买入/卖出信号。
        有点变种bias
        """
        ma = df['close'].rolling(2 * n, min_periods=1).mean()  # MA(CLOSE,N) 固定俩个参数之间的关系  减少参数
        ref = ma.shift(n)  # MADisplaced=REF(MA_CLOSE,M)

        f_name = f'ma_displaced_bh_{n}'
        df[f_name] = df['close'] / ref - 1  # 去量纲
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def dbcd_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # DBCD 6*n
    for n in back_hour_list:
        df['ma'] = df['close'].rolling(n, min_periods=1).mean()  # MA(CLOSE,N)
        df['BIAS'] = (df['close'] - df['ma']) / df['ma'] * 100  # BIAS=(CLOSE-MA(CLOSE,N)/MA(CLOSE,N))*100
        df['BIAS_DIF'] = df['BIAS'] - df['BIAS'].shift(3 * n)  # BIAS_DIF=BIAS-REF(BIAS,M)
        df['DBCD'] = df['BIAS_DIF'].rolling(3 * n + 2, min_periods=1).mean()
        # df['dbcd'] = df['BIAS_DIF'].ewm(span=3 * n3).mean()  # DBCD=SMA(BIAS_DIFF,T,1)
        f_name = f'dbcd_bh_{n}'
        df[f_name] = df['DBCD'].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        del df['ma']
        del df['BIAS']
        del df['BIAS_DIF']
        del df['DBCD']


def uos_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # UOS 指标
    for n in back_hour_list:
        M = n
        N = 2 * n
        O = 4 * n
        df['ref_close'] = df['close'].shift(1)
        df['TH'] = df[['high', 'ref_close']].max(axis=1)
        df['TL'] = df[['low', 'ref_close']].min(axis=1)
        df['TR'] = df['TH'] - df['TL']
        df['XR'] = df['close'] - df['TL']
        df['XRM'] = df['XR'].rolling(M).sum() / df['TR'].rolling(M).sum()
        df['XRN'] = df['XR'].rolling(N).sum() / df['TR'].rolling(N).sum()
        df['XRO'] = df['XR'].rolling(O).sum() / df['TR'].rolling(O).sum()
        df['UOS'] = 100 * (df['XRM'] * N * O + df['XRN'] * M * O + df['XRO'] * M * N) / (M * N + M * O + N * O)

        f_name = f'uos_bh_{n}'
        df[f_name] = df['UOS'].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        del df['ref_close']
        del df['TH']
        del df['TL']
        del df['TR']
        del df['XR']
        del df['XRM']
        del df['XRN']
        del df['XRO']
        del df['UOS']


def trix_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # TRIX 3n
    for n in back_hour_list:
        df['ema'] = df['close'].ewm(n, adjust=False).mean()
        df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean()
        df['ema_ema_ema'] = df['ema_ema'].ewm(n, adjust=False).mean()

        df['TRIX'] = (df['ema_ema_ema'] - df['ema_ema_ema'].shift(1)) / df['ema_ema_ema'].shift(1)

        f_name = f'trix_bh_{n}'
        df[f_name] = df['TRIX'].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        del df['ema']
        del df['ema_ema']
        del df['ema_ema_ema']
        del df['TRIX']


def vwap_bias_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # bias因子以均价表示, n
    for n in back_hour_list:
        df['vwap'] = df['volume'] / df['quote_volume']
        ma = df['vwap'].rolling(n, min_periods=1).mean()
        f_name = f'vwap_bias_bh_{n}'
        df[f_name] = df['vwap'] / ma - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        del df['vwap']


def ko_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # KO
    for n in back_hour_list:
        df['price'] = (df['high'] + df['low'] + df['close']) / 3
        df['V'] = np.where(df['price'] > df['price'].shift(1), df['volume'], -df['volume'])
        df['V_ema1'] = df['V'].ewm(n, adjust=False).mean()
        df['V_ema2'] = df['V'].ewm(int(n * 1.618), adjust=False).mean()
        df['KO'] = df['V_ema1'] - df['V_ema2']
        # 标准化
        f_name = f'ko_bh_{n}'
        df[f_name] = (df['KO'] - df['KO'].rolling(n).min()) / (
            df['KO'].rolling(n).max() - df['KO'].rolling(n).min())
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        del df['price']
        del df['V']
        del df['V_ema1']
        del df['V_ema2']
        del df['KO']

    # df['comp_zack'] = df['reg_diff_0.5'] * (df['uos_diff_0.3'] + df['k_diff_0.3']) \
    #     + df['pmo'] * (df['trix_diff_0.5'] + df['vwap_bias_diff_0.3']) \
    #     + df['dbcd_diff_0.5'] * (df['dc'] + df['ko'])
    # return df


def mtm_mean_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # mtm_mean
    for n in back_hour_list:
        f_name = f'mtm_mean_bh_{n}'
        df[f_name] = (df['close'] / df['close'].shift(n) - 1).rolling(window=n,
                                                                      min_periods=1).mean().shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def force_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # force
    for n in back_hour_list:
        df['force'] = df['quote_volume'] * (df['close'] - df['close'].shift(1))

        f_name = f'force_bh_{n}'
        df[f_name] = df['force'].rolling(n, min_periods=1).mean()
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间过程数据
        del df['force']


def bolling_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # Bolling
    for n in back_hour_list:
        # 计算布林上下轨
        df['std'] = df['close'].rolling(n, min_periods=1).std()
        df['ma'] = df['close'].rolling(n, min_periods=1).mean()
        df['upper'] = df['ma'] + 1.0 * df['std']
        df['lower'] = df['ma'] - 1.0 * df['std']
        # 将上下轨中间的部分设为0
        condition_0 = (df['close'] <= df['upper']) & (df['close'] >= df['lower'])
        condition_1 = df['close'] > df['upper']
        condition_2 = df['close'] < df['lower']
        df.loc[condition_0, 'distance'] = 0
        df.loc[condition_1, 'distance'] = df['close'] - df['upper']
        df.loc[condition_2, 'distance'] = df['close'] - df['lower']

        f_name = f'bolling_bh_{n}'
        df[f_name] = df['distance'] / df['std']
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def vix_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # vix, 2*n
    for n in back_hour_list:
        df['vix'] = df['close'] / df['close'].shift(n) - 1
        df['up'] = df['vix'].rolling(window=n).max().shift(1)

        f_name = f'vix_bh_{n}'
        df[f_name] = df['vix'] - df['up']
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def vix_bw_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    for n in back_hour_list:
        df['vix'] = df['close'] / df['close'].shift(n) - 1
        df['vix_median'] = df['vix'].rolling(
            window=n, min_periods=1).mean()
        df['vix_std'] = df['vix'].rolling(n, min_periods=1).std()
        df['vix_score'] = abs(
            df['vix'] - df['vix_median']) / df['vix_std']
        df['max'] = df['vix_score'].rolling(
            window=n, min_periods=1).max().shift(1)
        df['min'] = df['vix_score'].rolling(
            window=n, min_periods=1).min().shift(1)
        df['vix_upper'] = df['vix_median'] + df['max'] * df['vix_std']
        df['vix_lower'] = df['vix_median'] - df['max'] * df['vix_std']

        f_name = f'vix_bw_bh_{n}'
        df[f_name] = (df['vix_upper'] - df['vix_lower'])*np.sign(df['vix_median'].diff(n))
        condition1 = np.sign(df['vix_median'].diff(n)) != np.sign(df['vix_median'].diff(1))
        condition2 = np.sign(df['vix_median'].diff(n)) != np.sign(df['vix_median'].diff(1).shift(1))
        df.loc[condition1, f_name] = 0
        df.loc[condition2, f_name] = 0
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        df.drop(['vix', 'vix_median', 'vix_std', 'max', 'min', 'vix_score',
                'vix_upper', 'vix_lower'], axis=1, inplace=True)


def atr_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # ATR
    for n in back_hour_list:
        # 基于价格atr，计算atr涨幅因子
        df['c1'] = df['high'] - df['low']
        df['c2'] = abs(df['high'] - df['close'].shift(1))
        df['c3'] = abs(df['low'] - df['close'].shift(1))
        df['tr'] = df[['c1', 'c2', 'c3']].max(axis=1)
        df['atr'] = df['tr'].rolling(window=n, min_periods=1).mean()
        df['avg_atr'] = df['atr'].rolling(window=n, min_periods=1).mean()
        df['atr_speed_up'] = df['atr'] / df['avg_atr']

        f_name = f'atr_bh_{n}'
        df[f_name] = df['atr_speed_up'].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def market_pnl_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # 市场盈亏 n
    for n in back_hour_list:
        quote_volume_ema = df['quote_volume'].ewm(span=n, adjust=False).mean()
        volume_ema = df['volume'].ewm(span=n, adjust=False).mean()
        cost = (df['open'] + df['low'] + df['close']) / 3
        cost_ema = cost.ewm(span=n, adjust=False).mean()
        condition = df['quote_volume'] > 0
        df.loc[condition, 'avg_p'] = df['quote_volume'] / df['volume']
        condition = df['quote_volume'] == 0

        df.loc[condition, 'avg_p'] = df['close'].shift(1)
        condition1 = df['avg_p'] <= df['high']
        condition2 = df['avg_p'] >= df['low']
        df.loc[condition1 & condition2, f'前{n}h平均持仓成本'] = quote_volume_ema / volume_ema
        condition1 = df['avg_p'] > df['high']
        condition2 = df['avg_p'] < df['low']
        df.loc[condition1 & condition2, f'前{n}h平均持仓成本'] = cost_ema

        f_name = f'market_pnl_bh_{n}'
        df[f_name] = df['close'] / df[f'前{n}h平均持仓成本'] - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        del df[f'avg_p']
        del df[f'前{n}h平均持仓成本']


def 收高差值_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # 当前收盘价减去过去几天最高价的均值
    for n in back_hour_list:
        df['high_mean'] = df['high'].rolling(n, min_periods=1).mean()
        f_name = f'收高差值_bh_{n}'
        # 去量纲
        df[f_name] = (df['close'] - df['high_mean']) / df['close']
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def pvt_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # PVT 指标 有改动, 2*n
    for n in back_hour_list:
        df['PVT'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1) * df['volume']
        df['PVT_MA'] = df['PVT'].rolling(n, min_periods=1).mean()

        # 去量纲
        f_name = f'pvt_bh_{n}'
        df[f_name] = (df['PVT'] / df['PVT_MA'] - 1)
        df[f_name] = df[f_name].rolling(n, min_periods=1).sum().shift(1)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def macd_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    """macd, 3n"""
    for n in back_hour_list:
        """
        N1=20
        N2=40
        N3=5
        MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        MACD_SIGNAL=EMA(MACD,N3)
        MACD_HISTOGRAM=MACD-MACD_SIGNAL

        MACD 指标衡量快速均线与慢速均线的差值。由于慢速均线反映的是
        之前较长时间的价格的走向，而快速均线反映的是较短时间的价格的
        走向，所以在上涨趋势中快速均线会比慢速均线涨的快，而在下跌趋
        势中快速均线会比慢速均线跌得快。所以 MACD 上穿/下穿 0 可以作
        为一种构造交易信号的方式。另外一种构造交易信号的方式是求
        MACD 与其移动平均（信号线）的差值得到 MACD 柱，利用 MACD
        柱上穿/下穿 0（即 MACD 上穿/下穿其信号线）来构造交易信号。这
        种方式在其他指标的使用中也可以借鉴。
        """
        short_windows = n
        long_windows = 3 * n
        macd_windows = int(1.618 * n)

        df['ema_short'] = df['close'].ewm(span=short_windows, adjust=False).mean()  # EMA(CLOSE,N1)
        df['ema_long'] = df['close'].ewm(span=long_windows, adjust=False).mean()  # EMA(CLOSE,N2)
        df['dif'] = df['ema_short'] - df['ema_long']  # MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        df['dea'] = df['dif'].ewm(span=macd_windows, adjust=False).mean()  # MACD_SIGNAL=EMA(MACD,N3)
        df['macd'] = 2 * (df['dif'] - df['dea'])  # MACD_HISTOGRAM=MACD-MACD_SIGNAL  一般看图指标计算对应实际乘以了2倍
        # 进行去量纲
        f_name = f'macd_bh_{n}'
        df[f_name] = df['macd'] / df['macd'].rolling(macd_windows, min_periods=1).mean() - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)  # 取前一周期防止未来函数  实盘中不需要
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间数据
        del df['ema_short']
        del df['ema_long']
        del df['dif']
        del df['dea']


def ema_d_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # 计算ema的差值, 3n
    for n in back_hour_list:
        """
        与求MACD的dif线一样
        """
        short_windows = n
        long_windows = 3 * n
        df['ema_short'] = df['close'].ewm(span=short_windows, adjust=False).mean()  # 计算短周期ema
        df['ema_long'] = df['close'].ewm(span=long_windows, adjust=False).mean()  # 计算长周期的ema
        df['diff_ema'] = df['ema_short'] - df['ema_long']  # 计算俩条线之间的差值
        df['diff_ema_mean'] = df['diff_ema'].ewm(span=n, adjust=False).mean()

        f_name = f'ema_d_bh_{n}'
        df[f_name] = df['diff_ema'] / df['diff_ema_mean'] - 1  # 去量纲
        df[f_name] = df[f_name].shift(1 if need_shift else 0)  # 取前一周期防止未来函数  实盘中不需要
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间数据
        del df['ema_short']
        del df['ema_long']
        del df['diff_ema']
        del df['diff_ema_mean']


def bbi_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # 计算BBI 的bias
    for n in back_hour_list:
        """
        BBI=(MA(CLOSE,3)+MA(CLOSE,6)+MA(CLOSE,12)+MA(CLOSE,24))/4
        BBI 是对不同时间长度的移动平均线取平均，能够综合不同移动平均
        线的平滑性和滞后性。如果收盘价上穿/下穿 BBI 则产生买入/卖出信
        号。
        """
        # 将BBI指标计算出来求bias
        ma1 = df['close'].rolling(n, min_periods=1).mean()
        ma2 = df['close'].rolling(2 * n, min_periods=1).mean()
        ma3 = df['close'].rolling(4 * n, min_periods=1).mean()
        ma4 = df['close'].rolling(8 * n, min_periods=1).mean()
        bbi = (ma1 + ma2 + ma3 + ma4) / 4  # BBI=(MA(CLOSE,3)+MA(CLOSE,6)+MA(CLOSE,12)+MA(CLOSE,24))/4
        f_name = f'bbi_bh_{n}'
        df[f_name] = df['close'] / bbi - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)  # 取前一周期防止未来函数  实盘中不需要
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def dpo_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # 计算 DPO
    for n in back_hour_list:
        """
        N=20
        DPO=CLOSE-REF(MA(CLOSE,N),N/2+1)
        DPO 是当前价格与延迟的移动平均线的差值，通过去除前一段时间
        的移动平均价格来减少长期的趋势对短期价格波动的影响。DPO>0
        表示目前处于多头市场；DPO<0 表示当前处于空头市场。我们通过
        DPO 上穿/下穿 0 线来产生买入/卖出信号。

        """
        ma = df['close'].rolling(n, min_periods=1).mean()  # 求close移动平均线
        ref = ma.shift(int(n / 2 + 1))  # REF(MA(CLOSE,N),N/2+1)
        df['DPO'] = df['close'] - ref  # DPO=CLOSE-REF(MA(CLOSE,N),N/2+1)
        df['DPO_ma'] = df['DPO'].rolling(n, min_periods=1).mean()  # 求均值
        f_name = f'dpo_bh_{n}'
        df[f_name] = df['DPO'] / df['DPO_ma'] - 1  # 去量纲
        df[f_name] = df[f_name].shift(1 if need_shift else 0)  # 取前一周期防止未来函数  实盘中不需要
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间数据
        del df['DPO']
        del df['DPO_ma']


def er_bull_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # 计算 ER
    for n in back_hour_list:
        """
        N=20
        BullPower=HIGH-EMA(CLOSE,N)
        BearPower=LOW-EMA(CLOSE,N)
        ER 为动量指标。用来衡量市场的多空力量对比。在多头市场，人们
        会更贪婪地在接近高价的地方买入，BullPower 越高则当前多头力量
        越强；而在空头市场，人们可能因为恐惧而在接近低价的地方卖出。
        BearPower 越低则当前空头力量越强。当两者都大于 0 时，反映当前
        多头力量占据主导地位；两者都小于0则反映空头力量占据主导地位。
        如果 BearPower 上穿 0，则产生买入信号；
        如果 BullPower 下穿 0，则产生卖出信号。
        """
        ema = df['close'].ewm(n, adjust=False).mean()  # EMA(CLOSE,N)
        bull_power = df['high'] - ema  # 越高表示上涨 牛市 BullPower=HIGH-EMA(CLOSE,N)
        bear_power = df['low'] - ema  # 越低表示下降越厉害  熊市 BearPower=LOW-EMA(CLOSE,N)
        f_name = f'er_bull_bh_{n}'
        df[f_name] = bull_power / ema  # 去量纲
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def er_bear_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # 计算 ER
    for n in back_hour_list:
        """
        N=20
        BullPower=HIGH-EMA(CLOSE,N)
        BearPower=LOW-EMA(CLOSE,N)
        ER 为动量指标。用来衡量市场的多空力量对比。在多头市场，人们
        会更贪婪地在接近高价的地方买入，BullPower 越高则当前多头力量
        越强；而在空头市场，人们可能因为恐惧而在接近低价的地方卖出。
        BearPower 越低则当前空头力量越强。当两者都大于 0 时，反映当前
        多头力量占据主导地位；两者都小于0则反映空头力量占据主导地位。
        如果 BearPower 上穿 0，则产生买入信号；
        如果 BullPower 下穿 0，则产生卖出信号。
        """
        ema = df['close'].ewm(n, adjust=False).mean()  # EMA(CLOSE,N)
        bull_power = df['high'] - ema  # 越高表示上涨 牛市 BullPower=HIGH-EMA(CLOSE,N)
        bear_power = df['low'] - ema  # 越低表示下降越厉害  熊市 BearPower=LOW-EMA(CLOSE,N)
        f_name = f'er_bear_bh_{n}'
        df[f_name] = bear_power / ema  # 去量纲
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def po_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # PO指标
    for n in back_hour_list:
        """
        EMA_SHORT=EMA(CLOSE,9)
        EMA_LONG=EMA(CLOSE,26)
        PO=(EMA_SHORT-EMA_LONG)/EMA_LONG*100
        PO 指标求的是短期均线与长期均线之间的变化率。
        如果 PO 上穿 0，则产生买入信号；
        如果 PO 下穿 0，则产生卖出信号。
        """
        ema_short = df['close'].ewm(n, adjust=False).mean()  # 短周期的ema
        ema_long = df['close'].ewm(n * 3, adjust=False).mean()  # 长周期的ema   固定倍数关系 减少参数
        f_name = f'po_bh_{n}'
        df[f_name] = (ema_short - ema_long) / ema_long * 100  # 去量纲
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def t3_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # T3 指标
    for n in back_hour_list:
        """
        N=20
        VA=0.5
        T1=EMA(CLOSE,N)*(1+VA)-EMA(EMA(CLOSE,N),N)*VA
        T2=EMA(T1,N)*(1+VA)-EMA(EMA(T1,N),N)*VA
        T3=EMA(T2,N)*(1+VA)-EMA(EMA(T2,N),N)*VA
        当 VA 是 0 时，T3 就是三重指数平均线，此时具有严重的滞后性；当
        VA 是 0 时，T3 就是三重双重指数平均线（DEMA），此时可以快速
        反应价格的变化。VA 值是 T3 指标的一个关键参数，可以用来调节
        T3 指标的滞后性。如果收盘价上穿/下穿 T3，则产生买入/卖出信号。
        """
        va = 0.5
        ema = df['close'].ewm(n, adjust=False).mean()  # EMA(CLOSE,N)
        ema_ema = ema.ewm(n, adjust=False).mean()  # EMA(EMA(CLOSE,N),N)
        T1 = ema * (1 + va) - ema_ema * va  # T1=EMA(CLOSE,N)*(1+VA)-EMA(EMA(CLOSE,N),N)*VA
        T1_ema = T1.ewm(n, adjust=False).mean()  # EMA(T1,N)
        T1_ema_ema = T1_ema.ewm(n, adjust=False).mean()  # EMA(EMA(T1,N),N)
        T2 = T1_ema * (1 + va) - T1_ema_ema * va  # T2=EMA(T1,N)*(1+VA)-EMA(EMA(T1,N),N)*VA
        T2_ema = T2.ewm(n, adjust=False).mean()  # EMA(T2,N)
        T2_ema_ema = T2_ema.ewm(n, adjust=False).mean()  # EMA(EMA(T2,N),N)
        T3 = T2_ema * (1 + va) - T2_ema_ema * va  # T3=EMA(T2,N)*(1+VA)-EMA(EMA(T2,N),N)*VA
        f_name = f't3_bh_{n}'
        df[f_name] = df['close'] / T3 - 1  # 去量纲
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def pos_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # POS指标
    for n in back_hour_list:
        """
        N=100
        PRICE=(CLOSE-REF(CLOSE,N))/REF(CLOSE,N)
        POS=(PRICE-MIN(PRICE,N))/(MAX(PRICE,N)-MIN(PRICE,N))
        POS 指标衡量当前的 N 天收益率在过去 N 天的 N 天收益率最大值和
        最小值之间的位置。当 POS 上穿 80 时产生买入信号；当 POS 下穿
        20 时产生卖出信号。
        """
        ref = df['close'].shift(n)  # REF(CLOSE,N)
        price = (df['close'] - ref) / ref  # PRICE=(CLOSE-REF(CLOSE,N))/REF(CLOSE,N)
        min_price = price.rolling(n).min()  # MIN(PRICE,N)
        max_price = price.rolling(n).max()  # MAX(PRICE,N)
        pos = (price - min_price) / (max_price - min_price)  # POS=(PRICE-MIN(PRICE,N))/(MAX(PRICE,N)-MIN(PRICE,N))
        f_name = f'pos_bh_{n}'
        df[f_name] = pos.shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def adtm_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # ADM 指标
    for n in back_hour_list:
        """
        N=20
        DTM=IF(OPEN>REF(OPEN,1),MAX(HIGH-OPEN,OPEN-REF(OP
        EN,1)),0)
        DBM=IF(OPEN<REF(OPEN,1),MAX(OPEN-LOW,REF(OPEN,1)-O
        PEN),0)
        STM=SUM(DTM,N)
        SBM=SUM(DBM,N)
        ADTM=(STM-SBM)/MAX(STM,SBM)
        ADTM 通过比较开盘价往上涨的幅度和往下跌的幅度来衡量市场的
        人气。ADTM 的值在-1 到 1 之间。当 ADTM 上穿 0.5 时，说明市场
        人气较旺；当 ADTM 下穿-0.5 时，说明市场人气较低迷。我们据此构
        造交易信号。
        当 ADTM 上穿 0.5 时产生买入信号；
        当 ADTM 下穿-0.5 时产生卖出信号。

        """
        df['h_o'] = df['high'] - df['open']  # HIGH-OPEN
        df['diff_open'] = df['open'] - df['open'].shift(1)  # OPEN-REF(OPEN,1)
        max_value1 = df[['h_o', 'diff_open']].max(axis=1)  # MAX(HIGH-OPEN,OPEN-REF(OPEN,1))
        # df.loc[df['open'] > df['open'].shift(1), 'DTM'] = max_value1
        # df['DTM'].fillna(value=0, inplace=True)
        # DBM=IF(OPEN<REF(OPEN,1),MAX(OPEN-LOW,REF(OPEN,1)-OPEN),0)
        df['DTM'] = np.where(df['open'] > df['open'].shift(1), max_value1, 0)
        df['o_l'] = df['open'] - df['low']  # OPEN-LOW
        max_value2 = df[['o_l', 'diff_open']].max(axis=1)  # MAX(OPEN-LOW,REF(OPEN,1)-OPEN),0)
        # DBM=IF(OPEN<REF(OPEN,1),MAX(OPEN-LOW,REF(OPEN,1)-OPEN),0)
        df['DBM'] = np.where(df['open'] < df['open'].shift(1), max_value2, 0)
        # df.loc[df['open'] < df['open'].shift(1), 'DBM'] = max_value2
        # df['DBM'].fillna(value=0, inplace=True)

        df['STM'] = df['DTM'].rolling(n).sum()  # STM=SUM(DTM,N)
        df['SBM'] = df['DBM'].rolling(n).sum()  # SBM=SUM(DBM,N)
        max_value3 = df[['STM', 'SBM']].max(axis=1)  # MAX(STM,SBM)
        ADTM = (df['STM'] - df['SBM']) / max_value3  # ADTM=(STM-SBM)/MAX(STM,SBM)
        f_name = f'adtm_bh_{n}'
        df[f_name] = ADTM.shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)

        # 删除中间数据
        del df['h_o']
        del df['diff_open']
        del df['o_l']
        del df['STM']
        del df['SBM']
        del df['DBM']
        del df['DTM']


def hma_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # HMA 指标
    for n in back_hour_list:
        """
        N=20
        HMA=MA(HIGH,N)
        HMA 指标为简单移动平均线把收盘价替换为最高价。当最高价上穿/
        下穿 HMA 时产生买入/卖出信号。
        """
        hma = df['high'].rolling(n, min_periods=1).mean()  # HMA=MA(HIGH,N)
        f_name = f'hma_bh_{n}'
        df[f_name] = df['high'] / hma - 1  # 去量纲
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def sroc_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # SROC 指标
    for n in back_hour_list:
        """
        N=13
        M=21
        EMAP=EMA(CLOSE,N)
        SROC=(EMAP-REF(EMAP,M))/REF(EMAP,M)
        SROC 与 ROC 类似，但是会对收盘价进行平滑处理后再求变化率。
        """
        ema = df['close'].ewm(n, adjust=False).mean()  # EMAP=EMA(CLOSE,N)
        ref = ema.shift(2 * n)  # 固定俩参数之间的倍数 REF(EMAP,M)
        f_name = f'sroc_bh_{n}'
        df[f_name] = (ema - ref) / ref  # SROC=(EMAP-REF(EMAP,M))/REF(EMAP,M)
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def zlmacd_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # ZLMACD 指标
    for n in back_hour_list:
        """
        N1=20
        N2=100
        ZLMACD=(2*EMA(CLOSE,N1)-EMA(EMA(CLOSE,N1),N1))-(2*EM
        A(CLOSE,N2)-EMA(EMA(CLOSE,N2),N2))
        ZLMACD 指标是对 MACD 指标的改进，它在计算中使用 DEMA 而不
        是 EMA，可以克服 MACD 指标的滞后性问题。如果 ZLMACD 上穿/
        下穿 0，则产生买入/卖出信号。
        """
        ema1 = df['close'].ewm(n, adjust=False).mean()  # EMA(CLOSE,N1)
        ema_ema_1 = ema1.ewm(n, adjust=False).mean()  # EMA(EMA(CLOSE,N1),N1)
        n2 = 5 * n  # 固定俩参数的倍数关系减少参数
        ema2 = df['close'].ewm(n2, adjust=False).mean()  # EMA(CLOSE,N2)
        ema_ema_2 = ema2.ewm(n2, adjust=False).mean()  # EMA(EMA(CLOSE,N2),N2)
        # ZLMACD=(2*EMA(CLOSE,N1)-EMA(EMA(CLOSE,N1),N1))-(2*EMA(CLOSE,N2)-EMA(EMA(CLOSE,N2),N2))
        ZLMACD = (2 * ema1 - ema_ema_1) - (2 * ema2 - ema_ema_2)
        f_name = f'zlmacd_bh_{n}'
        df[f_name] = df['close'] / ZLMACD - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def htma_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # TMA 指标
    for n in back_hour_list:
        """
        N=20
        CLOSE_MA=MA(CLOSE,N)
        TMA=MA(CLOSE_MA,N)
        TMA 均线与其他的均线类似，不同的是，像 EMA 这类的均线会赋予
        越靠近当天的价格越高的权重，而 TMA 则赋予考虑的时间段内时间
        靠中间的价格更高的权重。如果收盘价上穿/下穿 TMA 则产生买入/
        卖出信号。
        """
        ma = df['close'].rolling(n, min_periods=1).mean()  # CLOSE_MA=MA(CLOSE,N)
        tma = ma.rolling(n, min_periods=1).mean()  # TMA=MA(CLOSE_MA,N)
        f_name = f'htma_bh_{n}'
        df[f_name] = df['close'] / tma - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def typ_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # TYP 指标
    for n in back_hour_list:
        """
        N1=10
        N2=30
        TYP=(CLOSE+HIGH+LOW)/3
        TYPMA1=EMA(TYP,N1)
        TYPMA2=EMA(TYP,N2)
        在技术分析中，典型价格（最高价+最低价+收盘价）/3 经常被用来代
        替收盘价。比如我们在利用均线交叉产生交易信号时，就可以用典型
        价格的均线。
        TYPMA1 上穿/下穿 TYPMA2 时产生买入/卖出信号。
        """
        TYP = (df['close'] + df['high'] + df['low']) / 3  # TYP=(CLOSE+HIGH+LOW)/3
        TYPMA1 = TYP.ewm(n, adjust=False).mean()  # TYPMA1=EMA(TYP,N1)
        TYPMA2 = TYP.ewm(n * 3, adjust=False).mean()  # TYPMA2=EMA(TYP,N2) 并且固定俩参数倍数关系
        diff_TYP = TYPMA1 - TYPMA2  # 俩ema相差
        diff_TYP_mean = diff_TYP.rolling(n, min_periods=1).mean()
        # 无量纲
        f_name = f'typ_bh_{n}'
        df[f_name] = diff_TYP / diff_TYP_mean - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def kdjd_k_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # KDJD 指标
    for n in back_hour_list:
        """
        N=20
        M=60
        LOW_N=MIN(LOW,N)
        HIGH_N=MAX(HIGH,N)
        Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100
        Stochastics_LOW=MIN(Stochastics,M)
        Stochastics_HIGH=MAX(Stochastics,M)
        Stochastics_DOUBLE=(Stochastics-Stochastics_LOW)/(Stochastics_HIGH-Stochastics_LOW)*100
        K=SMA(Stochastics_DOUBLE,3,1)
        D=SMA(K,3,1)
        KDJD 可以看作 KDJ 的变形。KDJ 计算过程中的变量 Stochastics 用
        来衡量收盘价位于最近 N 天最高价和最低价之间的位置。而 KDJD 计
        算过程中的 Stochastics_DOUBLE 可以用来衡量 Stochastics 在最近
        N 天的 Stochastics 最大值与最小值之间的位置。我们这里将其用作
        动量指标。当 D 上穿 70/下穿 30 时，产生买入/卖出信号。
        """
        min_low = df['low'].rolling(n).min()  # LOW_N=MIN(LOW,N)
        max_high = df['high'].rolling(n).max()  # HIGH_N=MAX(HIGH,N)
        Stochastics = (df['close'] - min_low) / (max_high - min_low) * \
            100  # Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100
        # 固定俩参数的倍数关系
        Stochastics_LOW = Stochastics.rolling(n * 3).min()  # Stochastics_LOW=MIN(Stochastics,M)
        Stochastics_HIGH = Stochastics.rolling(n * 3).max()  # Stochastics_HIGH=MAX(Stochastics,M)
        # Stochastics_DOUBLE=(Stochastics-Stochastics_LOW)/(Stochastics_HIGH-Stochastics_LOW)*100
        Stochastics_DOUBLE = (Stochastics - Stochastics_LOW) / (Stochastics_HIGH - Stochastics_LOW)
        K = Stochastics_DOUBLE.ewm(com=2).mean()  # K=SMA(Stochastics_DOUBLE,3,1)
        D = K.ewm(com=2).mean()  # D=SMA(K,3,1)
        f_name = f'kdjd_k_bh_{n}'
        df[f_name] = K.shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)


def kdjd_d_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # KDJD 指标
    for n in back_hour_list:
        """
        N=20
        M=60
        LOW_N=MIN(LOW,N)
        HIGH_N=MAX(HIGH,N)
        Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100
        Stochastics_LOW=MIN(Stochastics,M)
        Stochastics_HIGH=MAX(Stochastics,M)
        Stochastics_DOUBLE=(Stochastics-Stochastics_LOW)/(Stochastics_HIGH-Stochastics_LOW)*100
        K=SMA(Stochastics_DOUBLE,3,1)
        D=SMA(K,3,1)
        KDJD 可以看作 KDJ 的变形。KDJ 计算过程中的变量 Stochastics 用
        来衡量收盘价位于最近 N 天最高价和最低价之间的位置。而 KDJD 计
        算过程中的 Stochastics_DOUBLE 可以用来衡量 Stochastics 在最近
        N 天的 Stochastics 最大值与最小值之间的位置。我们这里将其用作
        动量指标。当 D 上穿 70/下穿 30 时，产生买入/卖出信号。
        """
        min_low = df['low'].rolling(n).min()  # LOW_N=MIN(LOW,N)
        max_high = df['high'].rolling(n).max()  # HIGH_N=MAX(HIGH,N)
        Stochastics = (df['close'] - min_low) / (max_high - min_low) * \
            100  # Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100
        # 固定俩参数的倍数关系
        Stochastics_LOW = Stochastics.rolling(n * 3).min()  # Stochastics_LOW=MIN(Stochastics,M)
        Stochastics_HIGH = Stochastics.rolling(n * 3).max()  # Stochastics_HIGH=MAX(Stochastics,M)
        # Stochastics_DOUBLE=(Stochastics-Stochastics_LOW)/(Stochastics_HIGH-Stochastics_LOW)*100
        Stochastics_DOUBLE = (Stochastics - Stochastics_LOW) / (Stochastics_HIGH - Stochastics_LOW)
        K = Stochastics_DOUBLE.ewm(com=2).mean()  # K=SMA(Stochastics_DOUBLE,3,1)
        D = K.ewm(com=2).mean()  # D=SMA(K,3,1)
        f_name = f'kdjd_d_bh_{n}'
        df[f_name] = D.shift(1 if need_shift else 0)
        process_general_procedure(df, f_name, extra_agg_dict, add_diff)
