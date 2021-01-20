import pandas as pd
import numpy as np
import itertools
import json


def generic_statistic_process(df, slippage=1 / 1000, c_rate=5 / 10000, leverage_rate=1, face_value=0.01,
                              min_margin_ratio=1 / 100):
    """整个评价流程"""
    equity_df = equity_curve_for_OKEx_USDT_future_next_open(df.copy())
    return strategy_evaluate(equity_df, transfer_equity_curve_to_trade(equity_df.copy()))

# =====计算资金曲线
# okex交割合约（usdt本位）资金曲线


def equity_curve_for_OKEx_USDT_future_next_open(df, slippage=1 / 1000, c_rate=5 / 10000, leverage_rate=3, face_value=0.01,
                                                min_margin_ratio=1 / 100):
    """
    okex交割合约（usdt本位）资金曲线
    开仓价格是下根K线的开盘价，可以是其他的形式
    相比之前杠杆交易的资金曲线函数，逻辑简单很多：手续费的处理、爆仓的处理等。
    在策略中增加滑点的。滑点的处理和手续费是不同的。
    :param df:
    :param slippage:  滑点 ，可以用百分比，也可以用固定值。建议币圈用百分比，股票用固定值
    :param c_rate:  手续费，commission fees，默认为万分之5。不同市场手续费的收取方法不同，对结果有影响。比如和股票就不一样。
    :param leverage_rate:  杠杆倍数
    :param face_value:  一张合约的面值，0.01BTC
    :param min_margin_ratio: 最低保证金率，低于就会爆仓
    :return:
    """
    # =====下根k线开盘价
    df['next_open'] = df['open'].shift(-1)  # 下根K线的开盘价
    df['next_open'].fillna(value=df['close'], inplace=True)

    # =====找出开仓、平仓的k线
    condition1 = df['pos'] != 0  # 当前周期不为空仓
    condition2 = df['pos'] != df['pos'].shift(1)  # 当前周期和上个周期持仓方向不一样。
    open_pos_condition = condition1 & condition2

    condition1 = df['pos'] != 0  # 当前周期不为空仓
    condition2 = df['pos'] != df['pos'].shift(-1)  # 当前周期和下个周期持仓方向不一样。
    close_pos_condition = condition1 & condition2

    # =====对每次交易进行分组
    df.loc[open_pos_condition, 'start_time'] = df['candle_begin_time']
    df['start_time'].fillna(method='ffill', inplace=True)
    df.loc[df['pos'] == 0, 'start_time'] = pd.NaT

    # =====开始计算资金曲线
    initial_cash = 10000  # 初始资金，默认为10000元
    # ===在开仓时
    # 在open_pos_condition的K线，以开盘价计算买入合约的数量。（当资金量大的时候，可以用5分钟均价）
    df.loc[open_pos_condition, 'contract_num'] = initial_cash * leverage_rate / (face_value * df['open'])
    df['contract_num'] = np.floor(df['contract_num'])  # 对合约张数向下取整
    # 开仓价格：理论开盘价加上相应滑点
    df.loc[open_pos_condition, 'open_pos_price'] = df['open'] * (1 + slippage * df['pos'])
    # 开仓之后剩余的钱，扣除手续费
    df['cash'] = initial_cash - df['open_pos_price'] * face_value * df['contract_num'] * c_rate  # 即保证金

    # ===开仓之后每根K线结束时
    # 买入之后cash，contract_num，open_pos_price不再发生变动
    for _ in ['contract_num', 'open_pos_price', 'cash']:
        df[_].fillna(method='ffill', inplace=True)
    df.loc[df['pos'] == 0, ['contract_num', 'open_pos_price', 'cash']] = None

    # ===在平仓时
    # 平仓价格
    df.loc[close_pos_condition, 'close_pos_price'] = df['next_open'] * (1 - slippage * df['pos'])
    # 平仓之后剩余的钱，扣除手续费
    df.loc[close_pos_condition, 'close_pos_fee'] = df['close_pos_price'] * face_value * df['contract_num'] * c_rate

    # ===计算利润
    # 开仓至今持仓盈亏
    df['profit'] = face_value * df['contract_num'] * (df['close'] - df['open_pos_price']) * df['pos']
    # 平仓时理论额外处理
    df.loc[close_pos_condition, 'profit'] = face_value * df['contract_num'] * (
        df['close_pos_price'] - df['open_pos_price']) * df['pos']
    # 账户净值
    df['net_value'] = df['cash'] + df['profit']

    # ===计算爆仓
    # 至今持仓盈亏最小值
    df.loc[df['pos'] == 1, 'price_min'] = df['low']
    df.loc[df['pos'] == -1, 'price_min'] = df['high']
    df['profit_min'] = face_value * df['contract_num'] * (df['price_min'] - df['open_pos_price']) * df['pos']
    # 账户净值最小值
    df['net_value_min'] = df['cash'] + df['profit_min']
    # 计算保证金率
    df['margin_ratio'] = df['net_value_min'] / (face_value * df['contract_num'] * df['price_min'])
    # 计算是否爆仓
    df.loc[df['margin_ratio'] <= (min_margin_ratio + c_rate), '是否爆仓'] = 1

    # ===平仓时扣除手续费
    df.loc[close_pos_condition, 'net_value'] -= df['close_pos_fee']
    # 应对偶然情况：下一根K线开盘价格价格突变，在平仓的时候爆仓。此处处理有省略，不够精确。
    df.loc[close_pos_condition & (df['net_value'] < 0), '是否爆仓'] = 1

    # ===对爆仓进行处理
    df['是否爆仓'] = df.groupby('start_time')['是否爆仓'].fillna(method='ffill')
    df.loc[df['是否爆仓'] == 1, 'net_value'] = 0

    # =====计算资金曲线
    df['equity_change'] = df['net_value'].pct_change()
    df.loc[open_pos_condition, 'equity_change'] = df.loc[open_pos_condition, 'net_value'] / initial_cash - 1  # 开仓日的收益率
    df['equity_change'].fillna(value=0, inplace=True)
    df['equity_curve'] = (1 + df['equity_change']).cumprod()

    # =====删除不必要的数据，并存储
    df.drop(['next_open', 'contract_num', 'open_pos_price', 'cash', 'close_pos_price', 'close_pos_fee',
             'profit', 'net_value', 'price_min', 'profit_min', 'net_value_min', 'margin_ratio', '是否爆仓'],
            axis=1, inplace=True)

    return df

# ======= 策略评价 =========
# 将资金曲线数据，转化为交易数据


def transfer_equity_curve_to_trade(equity_curve):
    """
    将资金曲线数据，转化为一笔一笔的交易
    :param equity_curve: 资金曲线函数计算好的结果，必须包含pos
    :return:
    """
    # =选取开仓、平仓条件
    condition1 = equity_curve['pos'] != 0
    condition2 = equity_curve['pos'] != equity_curve['pos'].shift(1)
    open_pos_condition = condition1 & condition2

    # =计算每笔交易的start_time
    if 'start_time' not in equity_curve.columns:
        equity_curve.loc[open_pos_condition, 'start_time'] = equity_curve['candle_begin_time']
        equity_curve['start_time'].fillna(method='ffill', inplace=True)
        equity_curve.loc[equity_curve['pos'] == 0, 'start_time'] = pd.NaT

    # =对每次交易进行分组，遍历每笔交易
    trade = pd.DataFrame()  # 计算结果放在trade变量中

    grouped = False
    for _index, group in equity_curve.groupby('start_time'):
        grouped = True
        # 记录每笔交易
        # 本次交易方向
        trade.loc[_index, 'signal'] = group['pos'].iloc[0]

        # 本次交易杠杆倍数
        if 'leverage_rate' in group:
            trade.loc[_index, 'leverage_rate'] = group['leverage_rate'].iloc[0]

        g = group[group['pos'] != 0]  # 去除pos=0的行
        # 本次交易结束那根K线的开始时间
        trade.loc[_index, 'end_bar'] = g.iloc[-1]['candle_begin_time']
        # 开仓价格
        trade.loc[_index, 'start_price'] = g.iloc[0]['open']
        # 平仓信号的价格
        trade.loc[_index, 'end_price'] = g.iloc[-1]['close']
        # 持仓k线数量
        trade.loc[_index, 'bar_num'] = g.shape[0]
        # 本次交易收益
        trade.loc[_index, 'change'] = (group['equity_change'] + 1).prod() - 1
        # 本次交易结束时资金曲线
        trade.loc[_index, 'end_equity_curve'] = g.iloc[-1]['equity_curve']
        # 本次交易中资金曲线最低值
        trade.loc[_index, 'min_equity_curve'] = g['equity_curve'].min()
    if grouped:
        trade['end_bar'] = pd.to_datetime(trade.end_bar)
        trade.index = pd.to_datetime(trade.index)
        trade.tz_convert('UTC')
    return trade


# 计算策略评价指标
def strategy_evaluate(equity_curve, trade):
    """
    :param equity_curve: 带资金曲线的df
    :param trade: transfer_equity_curve_to_trade的输出结果，每笔交易的df
    :return:
    """

    if len(trade) == 0:
        return {
            "累积净值": 0,
            "年化收益": "0",
            "最大回撤": "0",
            "最大回撤开始时间": "",
            "最大回撤结束时间": "",
            "年化收益/回撤比": 0,
            "盈利笔数": 0,
            "亏损笔数": 0,
            "胜率": "0%",
            "每笔交易平均盈亏": "0%",
            "盈亏收益比": 0,
            "单笔最大盈利": "0",
            "单笔最大亏损": "0",
            "单笔最长持有时间": "0",
            "单笔最短持有时间": "0",
            "平均持仓周期": "0",
            "最大连续盈利笔数": 0,
            "最大连续亏损笔数": 0,
        }

    # ===新建一个dataframe保存回测指标
    results = pd.DataFrame()

    # ===计算累积净值
    results.loc[0, '累积净值'] = round(equity_curve['equity_curve'].iloc[-1], 2)

    # ===计算年化收益
    annual_return = (equity_curve['equity_curve'].iloc[-1] / equity_curve['equity_curve'].iloc[0]) ** (
        '1 days 00:00:00' / (equity_curve['candle_begin_time'].iloc[-1] - equity_curve['candle_begin_time'].iloc[0]) * 365) - 1
    results.loc[0, '年化收益'] = str(round(annual_return, 2))

    # ===计算最大回撤，最大回撤的含义：《如何通过3行代码计算最大回撤》https://mp.weixin.qq.com/s/Dwt4lkKR_PEnWRprLlvPVw
    # 计算当日之前的资金曲线的最高点
    equity_curve['max2here'] = equity_curve['equity_curve'].expanding().max()
    # 计算到历史最高值到当日的跌幅，drowdwon
    equity_curve['dd2here'] = equity_curve['equity_curve'] / equity_curve['max2here'] - 1
    # 计算最大回撤，以及最大回撤结束时间
    end_date, max_draw_down = tuple(equity_curve.sort_values(by=['dd2here']).iloc[0][['candle_begin_time', 'dd2here']])
    # 计算最大回撤开始时间
    start_date = equity_curve[equity_curve['candle_begin_time'] <= end_date].sort_values(by='equity_curve', ascending=False).iloc[0]['candle_begin_time']
    # 将无关的变量删除
    equity_curve.drop(['max2here', 'dd2here'], axis=1, inplace=True)
    results.loc[0, '最大回撤'] = format(max_draw_down, '.2%')
    results.loc[0, '最大回撤开始时间'] = str(start_date)
    results.loc[0, '最大回撤结束时间'] = str(end_date)

    # ===年化收益/回撤比
    results.loc[0, '年化收益/回撤比'] = round(abs(annual_return / max_draw_down), 2)

    # ===统计每笔交易
    results.loc[0, '盈利笔数'] = len(trade.loc[trade['change'] > 0])  # 盈利笔数
    results.loc[0, '亏损笔数'] = len(trade.loc[trade['change'] <= 0])  # 亏损笔数
    results.loc[0, '胜率'] = format(results.loc[0, '盈利笔数'] / len(trade), '.2%')  # 胜率

    results.loc[0, '每笔交易平均盈亏'] = format(trade['change'].mean(), '.2%')  # 每笔交易平均盈亏
    results.loc[0, '盈亏收益比'] = round(trade.loc[trade['change'] > 0]['change'].mean() /
                                    trade.loc[trade['change'] < 0]['change'].mean() * (-1), 2)  # 盈亏比

    results.loc[0, '单笔最大盈利'] = format(trade['change'].max(), '.2%')  # 单笔最大盈利
    results.loc[0, '单笔最大亏损'] = format(trade['change'].min(), '.2%')  # 单笔最大亏损

    # ===统计持仓时间，会比实际时间少一根K线的是距离
    trade['持仓时间'] = trade['end_bar'] - trade.index
    max_days, max_seconds = trade['持仓时间'].max().days, trade['持仓时间'].max().seconds
    max_hours = max_seconds // 3600
    max_minute = (max_seconds - max_hours * 3600) // 60
    results.loc[0, '单笔最长持有时间'] = str(max_days) + ' 天 ' + str(max_hours) + ' 小时 ' + str(max_minute) + ' 分钟'  # 单笔最长持有时间

    min_days, min_seconds = trade['持仓时间'].min().days, trade['持仓时间'].min().seconds
    min_hours = min_seconds // 3600
    min_minute = (min_seconds - min_hours * 3600) // 60
    results.loc[0, '单笔最短持有时间'] = str(min_days) + ' 天 ' + str(min_hours) + ' 小时 ' + str(min_minute) + ' 分钟'  # 单笔最短持有时间

    mean_days, mean_seconds = trade['持仓时间'].mean().days, trade['持仓时间'].mean().seconds
    mean_hours = mean_seconds // 3600
    mean_minute = (mean_seconds - mean_hours * 3600) // 60
    results.loc[0, '平均持仓周期'] = str(mean_days) + ' 天 ' + str(mean_hours) + ' 小时 ' + str(mean_minute) + ' 分钟'  # 平均持仓周期

    # ===连续盈利亏算
    results.loc[0, '最大连续盈利笔数'] = max(
        [len(list(v)) for k, v in itertools.groupby(np.where(trade['change'] > 0, 1, np.nan))])  # 最大连续盈利笔数
    results.loc[0, '最大连续亏损笔数'] = max(
        [len(list(v)) for k, v in itertools.groupby(np.where(trade['change'] < 0, 1, np.nan))])  # 最大连续亏损笔数

    json_result = json.loads(results.T.to_json())['0']

    # ===每月收益率
    equity_curve.set_index('candle_begin_time', inplace=True)
    monthly_return = equity_curve[['equity_change']].resample(rule='M').apply(lambda x: (1 + x).prod() - 1)

    json_monthly = json.loads(monthly_return.T.to_json())
    # {"1604102400000":{"equity_change":-0.1965637834},"1606694400000":{"equity_change":-0.2367361317},"1609372800000":{"equity_change":0.1857263391}}
    monthly_list = {x: json_monthly[x]["equity_change"] for x in json_monthly.keys()}
    json_result['月收益率'] = monthly_list
    return json_result
