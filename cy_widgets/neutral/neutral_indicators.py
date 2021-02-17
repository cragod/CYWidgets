import pandas as pd
import numpy as np
import talib as ta
from sklearn.linear_model import LinearRegression  # 版本0.0


def indicator_field_name(indicator, back_hour):
    return f'{indicator}_bh_{back_hour}'

# ===== 技术指标 =====


def kdj_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # 正常K线数据 计算 KDJ
    for n in back_hour_list:
        low_list = df['low'].rolling(n, min_periods=1).min()  # 过去n(含当前行)行数据 最低价的最小值
        high_list = df['high'].rolling(n, min_periods=1).max()  # 过去n(含当前行)行数据 最高价的最大值
        rsv = (df['close'] - low_list) / (high_list - low_list) * 100  # 未成熟随机指标值

        df[f'K_bh_{n}'] = rsv.ewm(com=2).mean().shift(1 if need_shift else 0)  # K
        extra_agg_dict[f'K_bh_{n}'] = 'first'

        df[f'D_bh_{n}'] = df[f'K_bh_{n}'].ewm(com=2).mean()  # D
        extra_agg_dict[f'D_bh_{n}'] = 'first'

        df[f'J_bh_{n}'] = 3 * df[f'K_bh_{n}'] - 2 * df[f'D_bh_{n}']  # J
        extra_agg_dict[f'J_bh_{n}'] = 'first'


def rsi_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- RSI ---  在期货市场很有效
    close_dif = df['close'].diff()
    df['up'] = np.where(close_dif > 0, close_dif, 0)
    df['down'] = np.where(close_dif < 0, abs(close_dif), 0)
    for n in back_hour_list:
        a = df['up'].rolling(n).sum()
        b = df['down'].rolling(n).sum()
        df[f'RSI_bh_{n}'] = (a / (a + b)).shift(1 if need_shift else 0)  # RSI
        extra_agg_dict[f'RSI_bh_{n}'] = 'first'


def avg_price_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- 均价 ---  对应低价股策略(预计没什么用)
    # 策略改进思路：以下所有用到收盘价的因子，都可尝试使用均价代替
    for n in back_hour_list:
        df[f'均价_bh_{n}'] = (df['quote_volume'].rolling(n).sum() / df['volume'].rolling(n).sum()).shift(1 if need_shift else 0)
        extra_agg_dict[f'均价_bh_{n}'] = 'first'


def 涨跌幅_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    for n in back_hour_list:
        df[f'涨跌幅_bh_{n}'] = df['close'].pct_change(n).shift(1 if need_shift else 0)
        extra_agg_dict[f'涨跌幅_bh_{n}'] = 'first'


def bias_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- bias ---  涨跌幅更好的表达方式 bias 币价偏离均线的比例。
    for n in back_hour_list:
        ma = df['close'].rolling(n, min_periods=1).mean()
        df[f'bias_bh_{n}'] = (df['close'] / ma - 1).shift(1 if need_shift else 0)
        extra_agg_dict[f'bias_bh_{n}'] = 'first'


def 振幅_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    for n in back_hour_list:
        high = df['high'].rolling(n, min_periods=1).max()
        low = df['low'].rolling(n, min_periods=1).min()
        df[f'振幅_bh_{n}'] = (high / low - 1).shift(1 if need_shift else 0)
        extra_agg_dict[f'振幅_bh_{n}'] = 'first'


def 振幅2_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- 振幅2 ---  收盘价、开盘价
    high = df[['close', 'open']].max(axis=1)
    low = df[['close', 'open']].min(axis=1)
    for n in back_hour_list:
        high = high.rolling(n, min_periods=1).max()
        low = low.rolling(n, min_periods=1).min()
        df[f'振幅2_bh_{n}'] = (high / low - 1).shift(1 if need_shift else 0)
        extra_agg_dict[f'振幅2_bh_{n}'] = 'first'


def 涨跌幅std_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- 涨跌幅std ---  振幅的另外一种形式
    change = df['close'].pct_change()
    for n in back_hour_list:
        df[f'涨跌幅std_bh_{n}'] = change.rolling(n).std().shift(1 if need_shift else 0)
        extra_agg_dict[f'涨跌幅std_bh_{n}'] = 'first'


def 涨跌幅skew_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- 涨跌幅skew ---  在商品期货市场有效
    # skew偏度rolling最小周期为3才有数据
    change = df['close'].pct_change()
    for n in back_hour_list:
        df[f'涨跌幅skew_bh_{n}'] = change.rolling(n).skew().shift(1 if need_shift else 0)
        extra_agg_dict[f'涨跌幅skew_bh_{n}'] = 'first'


def 成交额_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- 成交额 ---  对应小市值概念
    for n in back_hour_list:
        df[f'成交额_bh_{n}'] = df['quote_volume'].rolling(n, min_periods=1).sum().shift(1 if need_shift else 0)
        extra_agg_dict[f'成交额_bh_{n}'] = 'first'


def 成交额std_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- 成交额std ---  191选股因子中最有效的因子
    for n in back_hour_list:
        df[f'成交额std_bh_{n}'] = df['quote_volume'].rolling(n, min_periods=2).std().shift(1 if need_shift else 0)
        extra_agg_dict[f'成交额std_bh_{n}'] = 'first'


def 资金流入比例_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- 资金流入比例 --- 币安独有的数据
    for n in back_hour_list:
        volume = df['quote_volume'].rolling(n, min_periods=1).sum()
        buy_volume = df['taker_buy_quote_asset_volume'].rolling(n, min_periods=1).sum()
        df[f'资金流入比例_bh_{n}'] = (buy_volume / volume).shift(1 if need_shift else 0)
        extra_agg_dict[f'资金流入比例_bh_{n}'] = 'first'


def 量比_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- 量比 ---
    for n in back_hour_list:
        df[f'量比_bh_{n}'] = (df['quote_volume'] / df['quote_volume'].rolling(n, min_periods=1).mean()).shift(1 if need_shift else 0)
        extra_agg_dict[f'量比_bh_{n}'] = 'first'


def 成交笔数_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- 成交笔数 ---
    for n in back_hour_list:
        df[f'成交笔数_bh_{n}'] = df['trade_num'].rolling(n, min_periods=1).sum().shift(1 if need_shift else 0)
        extra_agg_dict[f'成交笔数_bh_{n}'] = 'first'


def 量价相关系数_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- 量价相关系数 ---  量价相关选股策略
    for n in back_hour_list:
        df[f'量价相关系数_bh_{n}'] = df['close'].rolling(n).corr(df['quote_volume']).shift(1 if need_shift else 0)
        extra_agg_dict[f'量价相关系数_bh_{n}'] = 'first'


def cci_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- cci ---  量价相关选股策略
    for n in back_hour_list:
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['ma'] = df['tp'].rolling(window=n, min_periods=1).mean()
        df['md'] = abs(df['close'] - df['ma']).rolling(window=n, min_periods=1).mean()
        df[f'cci_bh_{n}'] = (df['tp'] - df['ma']) / df['md'] / 0.015
        df[f'cci_bh_{n}'] = df[f'cci_bh_{n}'].shift(1 if need_shift else 0)
        extra_agg_dict[f'cci_bh_{n}'] = 'first'


def psy_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- psy ---  量价相关选股策略
    for n in back_hour_list:
        df['rtn'] = df['close'].diff()
        df['up'] = np.where(df['rtn'] > 0, 1, 0)
        df[f'psy_bh_{n}'] = df['up'].rolling(window=n).sum() / n
        df[f'psy_bh_{n}'] = df[f'psy_bh_{n}'].shift(1 if need_shift else 0)
        extra_agg_dict[f'psy_bh_{n}'] = 'first'


def cmo_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- cmo ---  量价相关选股策略
    for n in back_hour_list:
        df['momentum'] = df['close'] - df['close'].shift(1)
        df['up'] = np.where(df['momentum'] > 0, df['momentum'], 0)
        df['dn'] = np.where(df['momentum'] < 0, abs(df['momentum']), 0)
        df['up_sum'] = df['up'].rolling(window=n, min_periods=1).sum()
        df['dn_sum'] = df['dn'].rolling(window=n, min_periods=1).sum()
        df[f'cmo_bh_{n}'] = (df['up_sum'] - df['dn_sum']) / (df['up_sum'] + df['dn_sum'])
        df[f'cmo_bh_{n}'] = df[f'cmo_bh_{n}'].shift(1 if need_shift else 0)
        extra_agg_dict[f'cmo_bh_{n}'] = 'first'


def vma_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # VMA 指标
    for n in back_hour_list:
        """
        N=20
        PRICE=(HIGH+LOW+OPEN+CLOSE)/4
        VMA=MA(PRICE,N)
        VMA 就是简单移动平均把收盘价替换为最高价、最低价、开盘价和
        收盘价的平均值。当 PRICE 上穿/下穿 VMA 时产生买入/卖出信号。
        """
        price = (df['high'] + df['low'] + df['open'] + df['close']) / 4  # PRICE=(HIGH+LOW+OPEN+CLOSE)/4
        vma = price.rolling(n, min_periods=1).mean()  # VMA=MA(PRICE,N)
        df[f'vma_bh_{n}'] = price / vma - 1  # 去量纲
        df[f'vma_bh_{n}'] = df[f'vma_bh_{n}'].shift(1 if need_shift else 0)
        extra_agg_dict[f'vma_bh_{n}'] = 'first'


def pmo_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # PMO 指标, n^3 * 8
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
        df['ROC'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1) * 100  # ROC=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*100
        df['ROC_MA'] = df['ROC'].rolling(n, min_periods=1).mean()  # ROC_MA=DMA(ROC,2/N1)
        df['ROC_MA10'] = df['ROC_MA'] * 10  # ROC_MA10=ROC_MA*10
        df['PMO'] = df['ROC_MA10'].rolling(4 * n, min_periods=1).mean()  # PMO=DMA(ROC_MA10,2/N2)
        df['PMO_SIGNAL'] = df['PMO'].rolling(2 * n, min_periods=1).mean()  # PMO_SIGNAL=DMA(PMO,2/(N3+1))

        df[f'pmo_bh_{n}'] = df['PMO_SIGNAL'].shift(1 if need_shift else 0)
        extra_agg_dict[f'pmo_bh_{n}'] = 'first'
        # 删除中间过渡数据
        del df['ROC']
        del df['ROC_MA']
        del df['ROC_MA10']
        del df['PMO']
        del df['PMO_SIGNAL']


def reg_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
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

        # df['reg_close'] = talib.LINEARREG(df['close'], timeperiod=n) # 该部分为talib内置求线性回归
        # df['reg'] = df['close'] / df['ref_close'] - 1

        # sklearn 线性回归
        def reg_ols(_y, n):
            _x = np.arange(n) + 1
            model = LinearRegression().fit(_x.reshape(-1, 1), _y)  # 线性回归训练
            y_pred = model.coef_ * _x + model.intercept_  # y = ax + b
            return y_pred[-1]

        df['reg_close'] = df['close'].rolling(n).apply(lambda y: reg_ols(y, n))  # 求数据拟合的线性回归
        df['reg'] = df['close'] / df['reg_close'] - 1

        df[f'reg_bh_{n}'] = df['reg'].shift(1 if need_shift else 0)
        extra_agg_dict[f'reg_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['reg']
        del df['reg_close']


def dema_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # DEMA 指标
    for n in back_hour_list:
        """
        N=60
        EMA=EMA(CLOSE,N)
        DEMA=2*EMA-EMA(EMA,N)
        DEMA 结合了单重 EMA 和双重 EMA，在保证平滑性的同时减少滞后
        性。
        """
        ema = df['close'].ewm(n, adjust=False).mean()  # EMA=EMA(CLOSE,N)
        ema_ema = ema.ewm(n, adjust=False).mean()  # EMA(EMA,N)
        dema = 2 * ema - ema_ema  # DEMA=2*EMA-EMA(EMA,N)
        # dema 去量纲
        df[f'dema_bh_{n}'] = dema / ema - 1
        df[f'dema_bh_{n}'] = df[f'dema_bh_{n}'].shift(1 if need_shift else 0)
        extra_agg_dict[f'dema_bh_{n}'] = 'first'


def cr_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
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
        df['TYP'] = (df['high'] + df['low'] + df['close']) / 3  # TYP=(HIGH+LOW+CLOSE)/3
        df['H_TYP'] = df['high'] - df['TYP'].shift(1)  # HIGH-REF(TYP,1)
        df['H'] = np.where(df['high'] > df['TYP'].shift(1), df['H_TYP'], 0)  # H=MAX(HIGH-REF(TYP,1),0)
        df['L_TYP'] = df['TYP'].shift(1) - df['low']  # REF(TYP,1)-LOW
        df['L'] = np.where(df['TYP'].shift(1) > df['low'], df['L_TYP'], 0)  # L=MAX(REF(TYP,1)-LOW,0)
        df['CR'] = df['H'].rolling(n).sum() / df['L'].rolling(n).sum() * 100  # CR=SUM(H,N)/SUM(L,N)*100
        df[f'cr_bh_{n}'] = df['CR'].shift(1 if need_shift else 0)
        extra_agg_dict[f'cr_bh_{n}'] = 'first'
        # 删除中间数据
        del df['TYP']
        del df['H_TYP']
        del df['H']
        del df['L_TYP']
        del df['L']
        del df['CR']


def bop_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
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
        df['co'] = df['close'] - df['open']  # CLOSE-OPEN
        df['hl'] = df['high'] - df['low']  # HIGH-LOW
        df['BOP'] = (df['co'] / df['hl']).rolling(n, min_periods=1).mean()  # BOP=MA((CLOSE-OPEN)/(HIGH-LOW),N)

        df[f'bop_bh_{n}'] = df['BOP'].shift(1 if need_shift else 0)
        extra_agg_dict[f'bop_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['co']
        del df['hl']
        del df['BOP']


def hullma_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # HULLMA 指标
    for n in back_hour_list:
        """
        N=20,80
        X=2*EMA(CLOSE,[N/2])-EMA(CLOSE,N)
        HULLMA=EMA(X,[√𝑁])
        HULLMA 也是均线的一种，相比于普通均线有着更低的延迟性。我们
        用短期均线上/下穿长期均线来产生买入/卖出信号。
        """
        ema1 = df['close'].ewm(n, adjust=False).mean()  # EMA(CLOSE,[N/2])
        ema2 = df['close'].ewm(n * 2, adjust=False).mean()  # EMA(CLOSE,N)
        df['X'] = 2 * ema1 - ema2  # X=2*EMA(CLOSE,[N/2])-EMA(CLOSE,N)
        df['HULLMA'] = df['X'].ewm(int(np.sqrt(2 * n)), adjust=False).mean()  # HULLMA=EMA(X,[√𝑁])
        # 去量纲
        df[f'hullma_bh_{n}'] = df['HULLMA'].shift(1) - 1
        extra_agg_dict[f'hullma_bh_{n}'] = 'first'
        # 删除过程数据
        del df['X']
        del df['HULLMA']


def angle_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # --- Angle ---
    for n in back_hour_list:
        column_name = f'angle_bh_{n}'
        ma = df['close'].rolling(window=n, min_periods=1).mean()
        df[column_name] = ta.LINEARREG_ANGLE(ma, n)
        df[column_name] = df[column_name].shift(1 if need_shift else 0)
        extra_agg_dict[column_name] = 'first'


def gap_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # ---- Gap, n^2 ----
    for n in back_hour_list:
        ma = df['close'].rolling(window=n, min_periods=1).mean()
        wma = ta.WMA(df['close'], n)
        gap = wma - ma
        column_name = f'gap_bh_{n}'
        df[column_name] = gap / abs(gap).rolling(window=n).sum()
        df[column_name] = df[column_name].shift(1 if need_shift else 0)
        extra_agg_dict[column_name] = 'first'


def 癞子_indicator(df, back_hour_list, need_shift, extra_agg_dict={}):
    # ---- 癞子 ----
    for n in back_hour_list:
        diff = df['close'] / df['close'].shift(1) - 1
        column_name = f'癞子_bh_{n}'
        df[column_name] = diff / abs(diff).rolling(window=n).sum()
        df[column_name] = df[column_name].shift(1 if need_shift else 0)
        extra_agg_dict[column_name] = 'first'
