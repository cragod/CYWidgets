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
    # mag[ic_]cci
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


def qstick_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # Qstick 指标
    for n in back_hour_list:
        """
        N=20
        Qstick=MA(CLOSE-OPEN,N)
        Qstick 通过比较收盘价与开盘价来反映股价趋势的方向和强度。如果
        Qstick 上穿/下穿 0 则产生买入/卖出信号。
        """
        cl = df['close'] - df['open']  # CLOSE-OPEN
        Qstick = cl.rolling(n, min_periods=1).mean()  # Qstick=MA(CLOSE-OPEN,N)
        # 进行无量纲处理
        f_name = f'qstick_bh_{n}'
        df[f_name] = cl / Qstick - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')


def copp_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # COPP 指标
    for n in back_hour_list:
        """
        RC=100*((CLOSE-REF(CLOSE,N1))/REF(CLOSE,N1)+(CLOSE-REF(CLOSE,N2))/REF(CLOSE,N2))
        COPP=WMA(RC,M)
        COPP 指标用不同时间长度的价格变化率的加权移动平均值来衡量
        动量。如果 COPP 上穿/下穿 0 则产生买入/卖出信号。
        """
        df['RC'] = 100 * ((df['close'] - df['close'].shift(n)) / df['close'].shift(n) + (
            df['close'] - df['close'].shift(2 * n)) / df['close'].shift(2 * n))  # RC=100*((CLOSE-REF(CLOSE,N1))/REF(CLOSE,N1)+(CLOSE-REF(CLOSE,N2))/REF(CLOSE,N2))
        df['COPP'] = df['RC'].rolling(n, min_periods=1).mean()  # COPP=WMA(RC,M)  使用ma代替wma
        f_name = f'copp_bh_{n}'
        df[f_name] = df['COPP'].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过程数据
        del df['RC']
        del df['COPP']


def wc_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # WC 指标
    for n in back_hour_list:
        """
        WC=(HIGH+LOW+2*CLOSE)/4
        N1=20
        N2=40
        EMA1=EMA(WC,N1)
        EMA2=EMA(WC,N2)
        WC 也可以用来代替收盘价构造一些技术指标（不过相对比较少用
        到）。我们这里用 WC 的短期均线和长期均线的交叉来产生交易信号。
        """
        WC = (df['high'] + df['low'] + 2 * df['close']) / 4  # WC=(HIGH+LOW+2*CLOSE)/4
        df['ema1'] = WC.ewm(n, adjust=False).mean()  # EMA1=EMA(WC,N1)
        df['ema2'] = WC.ewm(2 * n, adjust=False).mean()  # EMA2=EMA(WC,N2)
        # 去量纲
        f_name = f'wc_bh_{n}'
        df[f_name] = df['ema1'] / df['ema2'] - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过程数据
        del df['ema1']
        del df['ema2']


def fisher_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # FISHER指标
    for n in back_hour_list:
        """
        N=20
        PARAM=0.3
        PRICE=(HIGH+LOW)/2
        PRICE_CH=2*(PRICE-MIN(LOW,N)/(MAX(HIGH,N)-MIN(LOW,N))-
        0.5)
        PRICE_CHANGE=0.999 IF PRICE_CHANGE>0.99 
        PRICE_CHANGE=-0.999 IF PRICE_CHANGE<-0.99
        PRICE_CHANGE=PARAM*PRICE_CH+(1-PARAM)*REF(PRICE_CHANGE,1)
        FISHER=0.5*REF(FISHER,1)+0.5*log((1+PRICE_CHANGE)/(1-PRICE_CHANGE))
        PRICE_CH 用来衡量当前价位于过去 N 天的最高价和最低价之间的
        位置。Fisher Transformation 是一个可以把股价数据变为类似于正态
        分布的方法。Fisher 指标的优点是减少了普通技术指标的滞后性。
        """
        PARAM = 1 / n
        df['price'] = (df['high'] + df['low']) / 2  # PRICE=(HIGH+LOW)/2
        df['min_low'] = df['low'].rolling(n).min()  # MIN(LOW,N)
        df['max_high'] = df['high'].rolling(n).max()  # MAX(HIGH,N)
        df['price_ch'] = 2 * (df['price'] - df['min_low']) / (df['max_high'] - df['low']) - 0.5  # PRICE_CH=2*(PRICE-MIN(LOW,N)/(MAX(HIGH,N)-MIN(LOW,N))-0.5)
        df['price_change'] = PARAM * df['price_ch'] + (1 - PARAM) * df['price_ch'].shift(1)
        df['price_change'] = np.where(df['price_change'] > 0.99, 0.999, df['price_change'])  # PRICE_CHANGE=0.999 IF PRICE_CHANGE>0.99
        df['price_change'] = np.where(df['price_change'] < -0.99, -0.999, df['price_change'])  # PRICE_CHANGE=-0.999 IF PRICE_CHANGE<-0.99
        # 0.5 * np.log((1 + df['price_change']) / (1 - df['price_change']))
        df['FISHER'] = 0.5 * np.log((1 + df['price_change']) / (1 - df['price_change']))
        # FISHER=0.5*REF(FISHER,1)+0.5*log((1+PRICE_CHANGE)/(1-PRICE_CHANGE))
        df['FISHER'] = 0.5 * df['FISHER'].shift(1) + 0.5 * np.log((1 + df['price_change']) / (1 - df['price_change']))

        f_name = f'fisher_bh_{n}'
        df[f_name] = df['FISHER'].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间数据
        del df['price']
        del df['min_low']
        del df['max_high']
        del df['price_ch']
        del df['price_change']
        del df['FISHER']


def demaker_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # Demakder 指标
    for n in back_hour_list:
        """
        N=20
        Demax=HIGH-REF(HIGH,1)
        Demax=IF(Demax>0,Demax,0)
        Demin=REF(LOW,1)-LOW
        Demin=IF(Demin>0,Demin,0)
        Demaker=MA(Demax,N)/(MA(Demax,N)+MA(Demin,N))
        当 Demaker>0.7 时上升趋势强烈，当 Demaker<0.3 时下跌趋势强烈。
        当 Demaker 上穿 0.7/下穿 0.3 时产生买入/卖出信号。
        """
        df['Demax'] = df['high'] - df['high'].shift(1)  # Demax=HIGH-REF(HIGH,1)
        df['Demax'] = np.where(df['Demax'] > 0, df['Demax'], 0)  # Demax=IF(Demax>0,Demax,0)
        df['Demin'] = df['low'].shift(1) - df['low']  # Demin=REF(LOW,1)-LOW
        df['Demin'] = np.where(df['Demin'] > 0, df['Demin'], 0)  # Demin=IF(Demin>0,Demin,0)
        df['Demax_ma'] = df['Demax'].rolling(n, min_periods=1).mean()  # MA(Demax,N)
        df['Demin_ma'] = df['Demin'].rolling(n, min_periods=1).mean()  # MA(Demin,N)
        df['Demaker'] = df['Demax_ma'] / (df['Demax_ma'] + df['Demin_ma'])  # Demaker=MA(Demax,N)/(MA(Demax,N)+MA(Demin,N))
        f_name = f'demaker_bh_{n}'
        df[f_name] = df['Demaker'].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过程数据
        del df['Demax']
        del df['Demin']
        del df['Demax_ma']
        del df['Demin_ma']
        del df['Demaker']


def ic_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # IC 指标
    for n in back_hour_list:
        """
        N1=9
        N2=26
        N3=52
        TS=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        KS=(MAX(HIGH,N2)+MIN(LOW,N2))/2
        SPAN_A=(TS+KS)/2
        SPAN_B=(MAX(HIGH,N3)+MIN(LOW,N3))/2
        在 IC 指标中，SPAN_A 与 SPAN_B 之间的部分称为云。如果价格在
        云上，则说明是上涨趋势（如果 SPAN_A>SPAN_B，则上涨趋势强
        烈；否则上涨趋势较弱）；如果价格在云下，则为下跌趋势（如果
        SPAN_A<SPAN_B，则下跌趋势强烈；否则下跌趋势较弱）。该指
        标的使用方式与移动平均线有许多相似之处，比如较快的线（TS）突
        破较慢的线（KS），价格突破 KS,价格突破云，SPAN_A 突破 SPAN_B
        等。我们产生信号的方式是：如果价格在云上方 SPAN_A>SPAN_B，
        则当价格上穿 KS 时买入；如果价格在云下方且 SPAN_A<SPAN_B，
        则当价格下穿 KS 时卖出。
        """
        n2 = 3 * n
        n3 = 2 * n2
        df['max_high_1'] = df['high'].rolling(n, min_periods=1).max()  # MAX(HIGH,N1)
        df['min_low_1'] = df['low'].rolling(n, min_periods=1).min()  # MIN(LOW,N1)
        df['TS'] = (df['max_high_1'] + df['min_low_1']) / 2  # TS=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        df['max_high_2'] = df['high'].rolling(n2, min_periods=1).max()  # MAX(HIGH,N2)
        df['min_low_2'] = df['low'].rolling(n2, min_periods=1).min()  # MIN(LOW,N2)
        df['KS'] = (df['max_high_2'] + df['min_low_2']) / 2  # KS=(MAX(HIGH,N2)+MIN(LOW,N2))/2
        df['span_A'] = (df['TS'] + df['KS']) / 2  # SPAN_A=(TS+KS)/2
        df['max_high_3'] = df['high'].rolling(n3, min_periods=1).max()  # MAX(HIGH,N3)
        df['min_low_3'] = df['low'].rolling(n3, min_periods=1).min()  # MIN(LOW,N3)
        df['span_B'] = (df['max_high_3'] + df['min_low_3']) / 2  # SPAN_B=(MAX(HIGH,N3)+MIN(LOW,N3))/2

        # 去量纲
        f_name = f'ic_bh_{n}'
        df[f_name] = df['span_A'] / df['span_B']
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过程数据
        del df['max_high_1']
        del df['max_high_2']
        del df['max_high_3']
        del df['min_low_1']
        del df['min_low_2']
        del df['min_low_3']
        del df['TS']
        del df['KS']
        del df['span_A']
        del df['span_B']


def tsi_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # TSI 指标
    for n in back_hour_list:
        """
        N1=25
        N2=13
        TSI=EMA(EMA(CLOSE-REF(CLOSE,1),N1),N2)/EMA(EMA(ABS(
        CLOSE-REF(CLOSE,1)),N1),N2)*100
        TSI 是一种双重移动平均指标。与常用的移动平均指标对收盘价取移
        动平均不同，TSI 对两天收盘价的差值取移动平均。如果 TSI 上穿 10/
        下穿-10 则产生买入/卖出指标。
        """
        n1 = 2 * n
        df['diff_close'] = df['close'] - df['close'].shift(1)  # CLOSE-REF(CLOSE,1)
        df['ema'] = df['diff_close'].ewm(n1, adjust=False).mean()  # EMA(CLOSE-REF(CLOSE,1),N1)
        df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean()  # EMA(EMA(CLOSE-REF(CLOSE,1),N1),N2)

        df['abs_diff_close'] = abs(df['diff_close'])  # ABS(CLOSE-REF(CLOSE,1))
        df['abs_ema'] = df['abs_diff_close'].ewm(n1, adjust=False).mean()  # EMA(ABS(CLOSE-REF(CLOSE,1)),N1)
        df['abs_ema_ema'] = df['abs_ema'].ewm(n, adjust=False).mean()  # EMA(EMA(ABS(CLOSE-REF(CLOSE,1)),N1)
        # TSI=EMA(EMA(CLOSE-REF(CLOSE,1),N1),N2)/EMA(EMA(ABS(CLOSE-REF(CLOSE,1)),N1),N2)*100
        df['TSI'] = df['ema_ema'] / df['abs_ema_ema'] * 100

        f_name = f'tsi_bh_{n}'
        df[f_name] = df['TSI'].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过程数据
        del df['diff_close']
        del df['ema']
        del df['ema_ema']
        del df['abs_diff_close']
        del df['abs_ema']
        del df['abs_ema_ema']
        del df['TSI']


def lma_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # LMA 指标
    for n in back_hour_list:
        """
        N=20
        LMA=MA(LOW,N)
        LMA 为简单移动平均把收盘价替换为最低价。如果最低价上穿/下穿
        LMA 则产生买入/卖出信号。
        """
        df['low_ma'] = df['low'].rolling(n, min_periods=1).mean()  # LMA=MA(LOW,N)
        # 进行去量纲
        f_name = f'lma_bh_{n}'
        df[f_name] = df['low'] / df['low_ma'] - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过程数据
        del df['low_ma']


def imi_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # IMI 指标
    for n in back_hour_list:
        """
        N=14
        INC=SUM(IF(CLOSE>OPEN,CLOSE-OPEN,0),N)
        DEC=SUM(IF(OPEN>CLOSE,OPEN-CLOSE,0),N)
        IMI=INC/(INC+DEC)
        IMI 的计算方法与 RSI 很相似。其区别在于，在 IMI 计算过程中使用
        的是收盘价和开盘价，而 RSI 使用的是收盘价和前一天的收盘价。所
        以，RSI 做的是前后两天的比较，而 IMI 做的是同一个交易日内的比
        较。如果 IMI 上穿 80，则产生买入信号；如果 IMI 下穿 20，则产生
        卖出信号。
        """
        df['INC'] = np.where(df['close'] > df['open'], df['close'] - df['open'], 0)  # IF(CLOSE>OPEN,CLOSE-OPEN,0)
        df['INC_sum'] = df['INC'].rolling(n).sum()  # INC=SUM(IF(CLOSE>OPEN,CLOSE-OPEN,0),N)
        df['DEC'] = np.where(df['open'] > df['close'], df['open'] - df['close'], 0)  # IF(OPEN>CLOSE,OPEN-CLOSE,0)
        df['DEC_sum'] = df['DEC'].rolling(n).sum()  # DEC=SUM(IF(OPEN>CLOSE,OPEN-CLOSE,0),N)
        df['IMI'] = df['INC_sum'] / (df['INC_sum'] + df['DEC_sum'])  # IMI=INC/(INC+DEC)

        f_name = f'imi_bh_{n}'
        df[f_name] = df['IMI'].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过程数据
        del df['INC']
        del df['INC_sum']
        del df['DEC']
        del df['DEC_sum']
        del df['IMI']


def osc_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # OSC 指标
    for n in back_hour_list:
        """
        N=40
        M=20
        OSC=CLOSE-MA(CLOSE,N)
        OSCMA=MA(OSC,M)
        OSC 反映收盘价与收盘价移动平均相差的程度。如果 OSC 上穿/下 穿 OSCMA 则产生买入/卖出信号。
        """
        df['ma'] = df['close'].rolling(2 * n, min_periods=1).mean()  # MA(CLOSE,N)
        df['OSC'] = df['close'] - df['ma']  # OSC=CLOSE-MA(CLOSE,N)
        df['OSCMA'] = df['OSC'].rolling(n, min_periods=1).mean()  # OSCMA=MA(OSC,M)
        f_name = f'osc_bh_{n}'
        df[f_name] = df['OSCMA'].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过程数据
        del df['ma']
        del df['OSC']
        del df['OSCMA']


def clv_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # CLV 指标
    for n in back_hour_list:
        """
        N=60
        CLV=(2*CLOSE-LOW-HIGH)/(HIGH-LOW)
        CLVMA=MA(CLV,N)
        CLV 用来衡量收盘价在最低价和最高价之间的位置。当
        CLOSE=HIGH 时，CLV=1;当 CLOSE=LOW 时，CLV=-1;当 CLOSE
        位于 HIGH 和 LOW 的中点时，CLV=0。CLV>0（<0），说明收盘价
        离最高（低）价更近。我们用 CLVMA 上穿/下穿 0 来产生买入/卖出
        信号。
        """
        # CLV=(2*CLOSE-LOW-HIGH)/(HIGH-LOW)
        df['CLV'] = (2 * df['close'] - df['low'] - df['high']) / (df['high'] - df['low'])
        df['CLVMA'] = df['CLV'].rolling(n, min_periods=1).mean()  # CLVMA=MA(CLV,N)
        f_name = f'clv_bh_{n}'
        df[f_name] = df['CLVMA'].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过程数据
        del df['CLV']
        del df['CLVMA']


def wad_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    #  WAD 指标
    for n in back_hour_list:
        """
        TRH=MAX(HIGH,REF(CLOSE,1))
        TRL=MIN(LOW,REF(CLOSE,1))
        AD=IF(CLOSE>REF(CLOSE,1),CLOSE-TRL,CLOSE-TRH) 
        AD=IF(CLOSE>REF(CLOSE,1),0,CLOSE-REF(CLOSE,1))  # 该指标怀疑有误
        WAD=CUMSUM(AD)
        N=20
        WADMA=MA(WAD,N)
        我们用 WAD 上穿/下穿其均线来产生买入/卖出信号。
        """
        df['ref_close'] = df['close'].shift(1)  # REF(CLOSE,1)
        df['TRH'] = df[['high', 'ref_close']].max(axis=1)  # TRH=MAX(HIGH,REF(CLOSE,1))
        df['TRL'] = df[['low', 'ref_close']].min(axis=1)  # TRL=MIN(LOW,REF(CLOSE,1))
        # AD=IF(CLOSE>REF(CLOSE,1),CLOSE-TRL,CLOSE-TRH)
        df['AD'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['TRL'], df['close'] - df['TRH'])
        # AD=IF(CLOSE>REF(CLOSE,1),0,CLOSE-REF(CLOSE,1))
        df['AD'] = np.where(df['close'] > df['close'].shift(1), 0, df['close'] - df['close'].shift(1))
        # WAD=CUMSUM(AD)
        df['WAD'] = df['AD'].cumsum()
        # WADMA=MA(WAD,N)
        df['WADMA'] = df['WAD'].rolling(n, min_periods=1).mean()
        # 去量纲
        f_name = f'wad_bh_{n}'
        df[f_name] = df['WAD'] / df['WADMA'] - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过程数据
        del df['ref_close']
        del df['TRH']
        del df['AD']
        del df['WAD']
        del df['WADMA']


def bias36_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # BIAS36
    for n in back_hour_list:
        """
        N=6
        BIAS36=MA(CLOSE,3)-MA(CLOSE,6)
        MABIAS36=MA(BIAS36,N)
        类似于乖离用来衡量当前价格与移动平均价的差距，三六乖离用来衡
        量不同的移动平均价间的差距。当三六乖离上穿/下穿其均线时，产生
        买入/卖出信号。
        """
        df['ma3'] = df['close'].rolling(n, min_periods=1).mean()  # MA(CLOSE,3)
        df['ma6'] = df['close'].rolling(2 * n, min_periods=1).mean()  # MA(CLOSE,6)
        df['BIAS36'] = df['ma3'] - df['ma6']  # BIAS36=MA(CLOSE,3)-MA(CLOSE,6)
        df['MABIAS36'] = df['BIAS36'].rolling(2 * n, min_periods=1).mean()  # MABIAS36=MA(BIAS36,N)
        # 去量纲
        f_name = f'bias36_bh_{n}'
        df[f_name] = df['BIAS36'] / df['MABIAS36']
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过程数据
        del df['ma3']
        del df['ma6']
        del df['BIAS36']
        del df['MABIAS36']


def tema_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # TEMA 指标
    for n in back_hour_list:
        """
        N=20,40
        TEMA=3*EMA(CLOSE,N)-3*EMA(EMA(CLOSE,N),N)+EMA(EMA(EMA(CLOSE,N),N),N)
        TEMA 结合了单重、双重和三重的 EMA，相比于一般均线延迟性较
        低。我们用快、慢 TEMA 的交叉来产生交易信号。
        """
        df['ema'] = df['close'].ewm(n, adjust=False).mean()  # EMA(CLOSE,N)
        df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean()  # EMA(EMA(CLOSE,N),N)
        df['ema_ema_ema'] = df['ema_ema'].ewm(n, adjust=False).mean()  # EMA(EMA(EMA(CLOSE,N),N),N)
        df['TEMA'] = 3 * df['ema'] - 3 * df['ema_ema'] + df['ema_ema_ema']  # TEMA=3*EMA(CLOSE,N)-3*EMA(EMA(CLOSE,N),N)+EMA(EMA(EMA(CLOSE,N),N),N)
        # 去量纲
        f_name = f'tema_bh_{n}'
        df[f_name] = df['ema'] / df['TEMA'] - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过程数据
        del df['ema']
        del df['ema_ema']
        del df['ema_ema_ema']
        del df['TEMA']


def dma_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # DMA 指标
    for n in back_hour_list:
        """
        DMA=MA(CLOSE,N1)-MA(CLOSE,N2)
        AMA=MA(DMA,N1)
        DMA 衡量快速移动平均与慢速移动平均之差。用 DMA 上穿/下穿其
        均线产生买入/卖出信号。
        """
        df['ma1'] = df['close'].rolling(n, min_periods=1).mean()  # MA(CLOSE,N1)
        df['ma2'] = df['close'].rolling(n * 3, min_periods=1).mean()  # MA(CLOSE,N2)
        df['DMA'] = df['ma1'] - df['ma2']  # DMA=MA(CLOSE,N1)-MA(CLOSE,N2)
        df['AMA'] = df['DMA'].rolling(n, min_periods=1).mean()  # AMA=MA(DMA,N1)
        # 去量纲
        f_name = f'dma_bh_{n}'
        df[f_name] = df['DMA'] / df['AMA'] - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过程数据
        del df['ma1']
        del df['ma2']
        del df['DMA']
        del df['AMA']


def kst_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # KST 指标
    for n in back_hour_list:
        """
        ROC_MA1=MA(CLOSE-REF(CLOSE,10),10)
        ROC_MA2=MA(CLOSE -REF(CLOSE,15),10)
        ROC_MA3=MA(CLOSE -REF(CLOSE,20),10)
        ROC_MA4=MA(CLOSE -REF(CLOSE,30),10)
        KST_IND=ROC_MA1+ROC_MA2*2+ROC_MA3*3+ROC_MA4*4
        KST=MA(KST_IND,9)
        KST 结合了不同时间长度的 ROC 指标。如果 KST 上穿/下穿 0 则产
        生买入/卖出信号。
        """
        df['ROC1'] = df['close'] - df['close'].shift(n)  # CLOSE-REF(CLOSE,10)
        df['ROC_MA1'] = df['ROC1'].rolling(n, min_periods=1).mean()  # ROC_MA1=MA(CLOSE-REF(CLOSE,10),10)
        df['ROC2'] = df['close'] - df['close'].shift(int(n * 1.5))
        df['ROC_MA2'] = df['ROC2'].rolling(n, min_periods=1).mean()
        df['ROC3'] = df['close'] - df['close'].shift(int(n * 2))
        df['ROC_MA3'] = df['ROC3'].rolling(n, min_periods=1).mean()
        df['ROC4'] = df['close'] - df['close'].shift(int(n * 3))
        df['ROC_MA4'] = df['ROC4'].rolling(n, min_periods=1).mean()
        # KST_IND=ROC_MA1+ROC_MA2*2+ROC_MA3*3+ROC_MA4*4
        df['KST_IND'] = df['ROC_MA1'] + df['ROC_MA2'] * 2 + df['ROC_MA3'] * 3 + df['ROC_MA4'] * 4
        # KST=MA(KST_IND,9)
        df['KST'] = df['KST_IND'].rolling(n, min_periods=1).mean()
        # 去量纲
        f_name = 'kst_bh_{n}'
        df[f_name] = df['KST_IND'] / df['KST'] - 1
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过程数据
        del df['ROC1']
        del df['ROC2']
        del df['ROC3']
        del df['ROC4']
        del df['ROC_MA1']
        del df['ROC_MA2']
        del df['ROC_MA3']
        del df['ROC_MA4']
        del df['KST_IND']
        del df['KST']


def micd_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # MICD 指标
    for n in back_hour_list:
        """
        N=20
        N1=10
        N2=20
        M=10
        MI=CLOSE-REF(CLOSE,1)
        MTMMA=SMA(MI,N,1)
        DIF=MA(REF(MTMMA,1),N1)-MA(REF(MTMMA,1),N2)
        MICD=SMA(DIF,M,1)
        如果 MICD 上穿 0，则产生买入信号；
        如果 MICD 下穿 0，则产生卖出信号。
        """
        df['MI'] = df['close'] - df['close'].shift(1)  # MI=CLOSE-REF(CLOSE,1)
        # df['MIMMA'] = df['MI'].rolling(n, min_periods=1).mean()
        df['MIMMA'] = df['MI'].ewm(span=n).mean()  # MTMMA=SMA(MI,N,1)
        df['MIMMA_MA1'] = df['MIMMA'].shift(1).rolling(n, min_periods=1).mean()  # MA(REF(MTMMA,1),N1)
        df['MIMMA_MA2'] = df['MIMMA'].shift(1).rolling(2 * n, min_periods=1).mean()  # MA(REF(MTMMA,1),N2)
        df['DIF'] = df['MIMMA_MA1'] - df['MIMMA_MA2']  # DIF=MA(REF(MTMMA,1),N1)-MA(REF(MTMMA,1),N2)
        # df['MICD'] = df['DIF'].rolling(n, min_periods=1).mean()
        df['MICD'] = df['DIF'].ewm(span=n).mean()
        # 去量纲
        f_name = f'micd_bh_{n}'
        df[f_name] = df['DIF'] / df['MICD']
        df[f_name] = df[f_name].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间过渡数据
        del df['MI']
        del df['MIMMA']
        del df['MIMMA_MA1']
        del df['MIMMA_MA2']
        del df['DIF']
        del df['MICD']


def ppo_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # PPO 指标
    for n in back_hour_list:
        """
        N1=12
        N2=26
        N3=9
        PPO=(EMA(CLOSE,N1)-EMA(CLOSE,N2))/EMA(CLOSE,N2)
        PPO_SIGNAL=EMA(PPO,N3)
        PPO 是 MACD 的变化率版本。
        MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)，而
        PPO=(EMA(CLOSE,N1)-EMA(CLOSE,N2))/EMA(CLOSE,N2)。
        PPO 上穿/下穿 PPO_SIGNAL 产生买入/卖出信号。
        """
        #
        N3 = n
        N1 = int(n * 1.382)  # 黄金分割线
        N2 = 3 * n
        df['ema_1'] = df['close'].ewm(N1, adjust=False).mean()  # EMA(CLOSE,N1)
        df['ema_2'] = df['close'].ewm(N2, adjust=False).mean()  # EMA(CLOSE,N2)
        df['PPO'] = (df['ema_1'] - df['ema_2']) / df['ema_2']  # PPO=(EMA(CLOSE,N1)-EMA(CLOSE,N2))/EMA(CLOSE,N2)
        df['PPO_SIGNAL'] = df['PPO'].ewm(N3, adjust=False).mean()  # PPO_SIGNAL=EMA(PPO,N3)

        f_name = f'ppo_bh_{n}'
        df[f_name] = df['PPO_SIGNAL'].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间数据
        del df['ema_1']
        del df['ema_2']
        del df['PPO']
        del df['PPO_SIGNAL']


def smi_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # SMI 指标
    for n in back_hour_list:
        """
        N1=20
        N2=20
        N3=20
        M=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        D=CLOSE-M
        DS=EMA(EMA(D,N2),N2)
        DHL=EMA(EMA(MAX(HIGH,N1)-MIN(LOW,N1),N2),N2)
        SMI=100*DS/DHL
        SMIMA=MA(SMI,N3)
        SMI 指标可以看作 KDJ 指标的变形。不同的是，KD 指标衡量的是当
        天收盘价位于最近 N 天的最高价和最低价之间的位置，而 SMI 指标
        是衡量当天收盘价与最近 N 天的最高价与最低价均值之间的距离。我
        们用 SMI 指标上穿/下穿其均线产生买入/卖出信号。
        """
        df['max_high'] = df['high'].rolling(n, min_periods=1).mean()  # MAX(HIGH,N1)
        df['min_low'] = df['low'].rolling(n, min_periods=1).mean()  # MIN(LOW,N1)
        df['M'] = (df['max_high'] + df['min_low']) / 2  # M=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        df['D'] = df['close'] - df['M']  # D=CLOSE-M
        df['ema'] = df['D'].ewm(n, adjust=False).mean()  # EMA(D,N2)
        df['DS'] = df['ema'].ewm(n, adjust=False).mean()  # DS=EMA(EMA(D,N2),N2)
        df['HL'] = df['max_high'] - df['min_low']  # MAX(HIGH,N1) - MIN(LOW,N1)
        df['ema_hl'] = df['HL'].ewm(n, adjust=False).mean()  # EMA(MAX(HIGH,N1)-MIN(LOW,N1),N2)
        df['DHL'] = df['ema_hl'].ewm(n, adjust=False).mean()  # DHL=EMA(EMA(MAX(HIGH,N1)-MIN(LOW,N1),N2),N2)
        df['SMI'] = 100 * df['DS'] / df['DHL']  # SMI=100*DS/DHL
        df['SMIMA'] = df['SMI'].rolling(n, min_periods=1).mean()  # SMIMA=MA(SMI,N3)

        f_name = f'smi_bh_{n}'
        df[f_name] = df['SMIMA'].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间数据
        del df['max_high']
        del df['min_low']
        del df['M']
        del df['D']
        del df['ema']
        del df['DS']
        del df['HL']
        del df['ema_hl']
        del df['DHL']
        del df['SMI']
        del df['SMIMA']


def arbr_ar_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # ARBR指标
    for n in back_hour_list:
        """
        AR=SUM((HIGH-OPEN),N)/SUM((OPEN-LOW),N)*100
        BR=SUM((HIGH-REF(CLOSE,1)),N)/SUM((REF(CLOSE,1)-LOW),N)*100
        AR 衡量开盘价在最高价、最低价之间的位置；BR 衡量昨日收盘价在
        今日最高价、最低价之间的位置。AR 为人气指标，用来计算多空双
        方的力量对比。当 AR 值偏低（低于 50）时表示人气非常低迷，股价
        很低，若从 50 下方上穿 50，则说明股价未来可能要上升，低点买入。
        当 AR 值下穿 200 时卖出。
        """
        df['HO'] = df['high'] - df['open']  # (HIGH-OPEN)
        df['OL'] = df['open'] - df['low']  # (OPEN-LOW)
        df['AR'] = df['HO'].rolling(n).sum() / df['OL'].rolling(n).sum() * 100  # AR=SUM((HIGH-OPEN),N)/SUM((OPEN-LOW),N)*100
        df['HC'] = df['high'] - df['close'].shift(1)  # (HIGH-REF(CLOSE,1))
        df['CL'] = df['close'].shift(1) - df['low']  # (REF(CLOSE,1)-LOW)
        df['BR'] = df['HC'].rolling(n).sum() / df['CL'].rolling(n).sum() * 100  # BR=SUM((HIGH-REF(CLOSE,1)),N)/SUM((REF(CLOSE,1)-LOW),N)*100

        f_name = f'arbr_ar_bh_{n}'
        df[f_name] = df['AR'].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间数据
        del df['HO']
        del df['OL']
        del df['AR']
        del df['HC']
        del df['CL']
        del df['BR']


def arbr_br_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # ARBR指标
    for n in back_hour_list:
        """
        AR=SUM((HIGH-OPEN),N)/SUM((OPEN-LOW),N)*100
        BR=SUM((HIGH-REF(CLOSE,1)),N)/SUM((REF(CLOSE,1)-LOW),N)*100
        AR 衡量开盘价在最高价、最低价之间的位置；BR 衡量昨日收盘价在
        今日最高价、最低价之间的位置。AR 为人气指标，用来计算多空双
        方的力量对比。当 AR 值偏低（低于 50）时表示人气非常低迷，股价
        很低，若从 50 下方上穿 50，则说明股价未来可能要上升，低点买入。
        当 AR 值下穿 200 时卖出。
        """
        df['HO'] = df['high'] - df['open']  # (HIGH-OPEN)
        df['OL'] = df['open'] - df['low']  # (OPEN-LOW)
        df['AR'] = df['HO'].rolling(n).sum() / df['OL'].rolling(n).sum() * 100  # AR=SUM((HIGH-OPEN),N)/SUM((OPEN-LOW),N)*100
        df['HC'] = df['high'] - df['close'].shift(1)  # (HIGH-REF(CLOSE,1))
        df['CL'] = df['close'].shift(1) - df['low']  # (REF(CLOSE,1)-LOW)
        df['BR'] = df['HC'].rolling(n).sum() / df['CL'].rolling(n).sum() * 100  # BR=SUM((HIGH-REF(CLOSE,1)),N)/SUM((REF(CLOSE,1)-LOW),N)*100

        f_name = f'arbr_br_bh_{n}'
        df[f_name] = df['BR'].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间数据
        del df['HO']
        del df['OL']
        del df['AR']
        del df['HC']
        del df['CL']
        del df['BR']


def do_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # DO 指标
    for n in back_hour_list:
        """
        DO=EMA(EMA(RSI,N),M)
        DO 是平滑处理（双重移动平均）后的 RSI 指标。DO 大于 0 则说明
        市场处于上涨趋势，小于 0 说明市场处于下跌趋势。我们用 DO 上穿
        /下穿其移动平均线来产生买入/卖出信号。
        """
        # 计算RSI
        # 以下为基础策略分享会代码
        # diff = df['close'].diff()
        # df['up'] = np.where(diff > 0, diff, 0)
        # df['down'] = np.where(diff < 0, abs(diff), 0)
        # A = df['up'].rolling(n).sum()
        # B = df['down'].rolling(n).sum()
        # df['rsi'] = A / (A + B)
        diff = df['close'].diff()  # CLOSE-REF(CLOSE,1) 计算当前close 与前一周期的close的差值
        df['up'] = np.where(diff > 0, diff, 0)  # IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0) 表示当前是上涨状态，记录上涨幅度
        df['down'] = np.where(diff < 0, abs(diff), 0)  # IF(CLOSE<REF(CLOSE,1),ABS(CLOSE-REF(CLOSE,1)),0) 表示当前为下降状态，记录下降幅度
        A = df['up'].ewm(span=n).mean()  # SMA(CLOSEUP,N,1) 计算周期内的上涨幅度的sma
        B = df['down'].ewm(span=n).mean()  # SMA(CLOSEDOWN,N,1)计算周期内的下降幅度的sma
        df['rsi'] = A / (A + B)  # RSI=100*CLOSEUP_MA/(CLOSEUP_MA+CLOSEDOWN_MA)  没有乘以100   没有量纲即可
        df['ema_rsi'] = df['rsi'].ewm(n, adjust=False).mean()  # EMA(RSI,N)
        df['DO'] = df['ema_rsi'].ewm(n, adjust=False).mean()  # DO=EMA(EMA(RSI,N),M)

        f_name = f'do_bh_{n}'
        df[f_name] = df['DO'].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间数据
        del df['up']
        del df['down']
        del df['rsi']
        del df['ema_rsi']
        del df['DO']


def si_indicator(df, back_hour_list, need_shift, extra_agg_dict={}, add_diff=False):
    # SI 指标
    for n in back_hour_list:
        """
        A=ABS(HIGH-REF(CLOSE,1))
        B=ABS(LOW-REF(CLOSE,1))
        C=ABS(HIGH-REF(LOW,1))
        D=ABS(REF(CLOSE,1)-REF(OPEN,1))
        N=20
        K=MAX(A,B)
        M=MAX(HIGH-LOW,N)
        R1=A+0.5*B+0.25*D
        R2=B+0.5*A+0.25*D
        R3=C+0.25*D
        R4=IF((A>=B) & (A>=C),R1,R2)
        R=IF((C>=A) & (C>=B),R3,R4)
        SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+
        0.5*(CLOSE-OPEN))/R*K/M
        SI 用价格变化（即两天收盘价之差，昨日收盘与开盘价之差，今日收
        盘与开盘价之差）的加权平均来反映价格的变化。如果 SI 上穿/下穿
        0 则产生买入/卖出信号。
        """
        df['A'] = abs(df['high'] - df['close'].shift(1))  # A=ABS(HIGH-REF(CLOSE,1))
        df['B'] = abs(df['low'] - df['close'].shift(1))  # B=ABS(LOW-REF(CLOSE,1))
        df['C'] = abs(df['high'] - df['low'].shift(1))  # C=ABS(HIGH-REF(LOW,1))
        df['D'] = abs(df['close'].shift(1) - df['open'].shift(1))  # D=ABS(REF(CLOSE,1)-REF(OPEN,1))
        df['K'] = df[['A', 'B']].max(axis=1)  # K=MAX(A,B)
        df['M'] = (df['high'] - df['low']).rolling(n).max()  # M=MAX(HIGH-LOW,N)
        df['R1'] = df['A'] + 0.5 * df['B'] + 0.25 * df['D']  # R1=A+0.5*B+0.25*D
        df['R2'] = df['B'] + 0.5 * df['A'] + 0.25 * df['D']  # R2=B+0.5*A+0.25*D
        df['R3'] = df['C'] + 0.25 * df['D']  # R3=C+0.25*D
        df['R4'] = np.where((df['A'] >= df['B']) & (df['A'] >= df['C']), df['R1'], df['R2'])  # R4=IF((A>=B) & (A>=C),R1,R2)
        df['R'] = np.where((df['C'] >= df['A']) & (df['C'] >= df['B']), df['R3'], df['R4'])  # R=IF((C>=A) & (C>=B),R3,R4)
        # SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+0.5*(CLOSE-OPEN))/R*K/M
        df['SI'] = 50 * (df['close'] - df['close'].shift(1) + (df['close'].shift(1) - df['open'].shift(1)) +
                         0.5 * (df['close'] - df['open'])) / df['R'] * df['K'] / df['M']
        f_name = f'si_bh_{n}'
        df[f_name] = df['SI'].shift(1 if need_shift else 0)
        extra_agg_dict[f_name] = 'first'
        if type(add_diff) is list:
            add_diff_columns(df, f_name, extra_agg_dict, 'first', diff_d=add_diff)
        elif add_diff:
            add_diff_columns(df, f_name, extra_agg_dict, 'first')
        # 删除中间数据
        del df['A']
        del df['B']
        del df['C']
        del df['D']
        del df['K']
        del df['M']
        del df['R1']
        del df['R2']
        del df['R3']
        del df['R4']
        del df['R']
        del df['SI']
