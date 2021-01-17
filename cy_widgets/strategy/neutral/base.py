import pandas as pd
import pytz
from abc import abstractmethod, abstractproperty, abstractclassmethod
from datetime import datetime, timedelta


class NeutralStrategyBase:

    def __init__(self, select_coin_num, hold_period, leverage):
        self.select_coin_num = select_coin_num
        self.leverage = leverage
        self.hold_period = hold_period

    @abstractclassmethod
    def strategy_with_parameters(cls, parameters):
        """初始化"""
        raise NotImplementedError('Subclass')

    @abstractproperty
    def candle_count_4_cal_factor(self):
        raise NotImplementedError('需要多少根K线')

    @abstractmethod
    def cal_factor(self, df):
        raise NotImplementedError('计算后需要保证有 factor 列作为alpha')

    def cal_factor_and_select_coins(self, candle_df_dictionay, run_time):
        # 获取策略参数
        hold_period = self.hold_period
        selected_coin_num = self.select_coin_num

        # ===逐个遍历每一个币种，计算其因子，并且转化周期
        period_df_list = []
        for symbol in candle_df_dictionay.keys():
            # =获取相应币种1h的k线，深度拷贝
            df = candle_df_dictionay[symbol].copy()

            # =计算因子
            df = self.cal_factor(df)  # 计算信号

            # =将数据转化为需要的周期
            df['s_time'] = df['candle_begin_time']
            df['e_time'] = df['candle_begin_time']
            df.set_index('candle_begin_time', inplace=True)
            agg_dict = {'symbol': 'first', 's_time': 'first', 'e_time': 'last', 'close': 'last', 'factor': 'last'}
            # 转换生成每个策略所有offset的因子
            for offset in range(int(hold_period[:-1])):
                # 转换周期
                period_df = df.resample(hold_period, base=offset).agg(agg_dict)
                period_df['offset'] = offset
                # 保存策略信息到结果当中
                # period_df['key'] = f'{factor}_{para}_{hold_period}_{offset}H'  # 创建主键值
                # 截取指定周期的数据
                run_time = run_time.astimezone(tz=pytz.utc)
                period_df = period_df[
                    (period_df['s_time'] <= run_time - timedelta(hours=int(hold_period[:-1]))) &
                    (period_df['s_time'] > run_time - 2 * timedelta(hours=int(hold_period[:-1])))
                ]
                # 合并数据
                period_df_list.append(period_df)

        # ===将不同offset的数据，合并到一张表
        df = pd.concat(period_df_list)
        df = df.sort_values(['s_time', 'symbol'])

        # ===选币数据整理完成，接下来开始选币
        # 多空双向rank
        df['币总数'] = df.groupby(df.index).size()
        df['rank'] = df.groupby('s_time')['factor'].rank(method='first')
        # 关于rank的first参数的说明https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.rank.html
        # 删除不要的币
        df['方向'] = 0
        df.loc[df['rank'] <= selected_coin_num, '方向'] = 1
        df.loc[(df['币总数'] - df['rank']) < selected_coin_num, '方向'] = -1
        df = df[df['方向'] != 0]

        # ===将每个币种的数据保存到dict中
        # 删除不需要的列
        df.drop(['factor', '币总数', 'rank'], axis=1, inplace=True)
        df.reset_index(inplace=True)
        return df