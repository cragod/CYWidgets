from .base import *
from cy_widgets.neutral.neutral_indicators import *


class BinanceNeutralCompoundBase(NeutralStrategyBase):
    # 横截面基类
    @abstractproperty
    def factor_configs(self):
        NotImplementedError("Need")

    def final_factor_column_name(self, f_name, f_reverse, f_bh, f_diff):
        return f'{f_name}_{f_reverse}_{f_bh}_{f_diff}_factor'

    def cal_factor(self, df):
        for (f_name, f_reverse, f_bh, f_diff, _) in self.factor_configs:
            # 加差分
            if f_diff > 0:
                add_diff = [f_diff]
                f_column = f'{f_name}_bh_{f_bh}_diff_{f_diff}'
            else:
                add_diff = False
                f_column = f'{f_name}_bh_{f_bh}'
            # 计算因子
            eval(f'{f_name}_indicator')(df, [f_bh], False, add_diff=add_diff)
            # 设置到 factor
            final_factor_name = self.final_factor_column_name(f_name, f_reverse, f_bh, f_diff)
            if f_reverse:
                df[final_factor_name] = -df[f_column]
            else:
                df[final_factor_name] = df[f_column]
        # 横截面这里没有 factor，先给0
        df['factor'] = 0
        return df

    def update_agg_dict(self, agg_dict):
        for (f_name, f_reverse, f_bh, f_diff, _) in self.factor_configs:
            final_factor_name = self.final_factor_column_name(f_name, f_reverse, f_bh, f_diff)
            agg_dict[final_factor_name] = 'last'

    def cal_compound_factors(self, df):
        """ 横截面计算步骤 """
        for (f_name, f_reverse, f_bh, f_diff, f_weight) in self.factor_configs:
            final_factor_name = self.final_factor_column_name(f_name, f_reverse, f_bh, f_diff)
            df['factor'] += df.groupby('s_time')[final_factor_name].rank(method='first') * f_weight

# ======= 具体因子策略 =========


class BinanceNeutralPMOGAPREGStrategy(BinanceNeutralCompoundBase):
    # 1. ('pmo', True, 3, 0, 1), ('gap', True, 24, 0, 0.2), ('reg_ta', False, 12, 0, 0.7)

    @property
    def display_name(self):
        return "1.PMO_GAP_REG_6H"

    @property
    def candle_count_4_cal_factor(self):
        return 100

    @property
    def factor_configs(self):
        return ('pmo', True, 3, 0, 1), ('gap', True, 24, 0, 0.2), ('reg_ta', False, 12, 0, 0.7)


class BinanceNeutral_REG_RCCD_VIDYA_RWI_APZ_8H(BinanceNeutralCompoundBase):
    # ~2~. ('reg_ta', False, 8, 0, 1.0), ('rccd', 1, 6, 0, 0.6), ('vidya', True, 12, 0, 0.1), ('rwih', True, 96, 0, 0.1), ('rwil', True, 48, 0, 0.3), ('apz', True, 9, 0, 0.1) 4H
    # 2. ('vwap_bias', 1, 12, 0.5, 0.4), ('收高差值', 0, 8, 0.5, 1.0), ('mtm_mean', 1, 96, 0.7, 0.3), ('v1_up', 1, 8, 0.3, 1.0), ('rsi', 1, 36, 0.5, 0.3) 8H 2021.3.22 换

    @property
    def display_name(self):
        return "2.REG_RCCD_VIDYA_RWI_APZ_8H"

    @property
    def candle_count_4_cal_factor(self):
        return 200

    @property
    def factor_configs(self):
        return ('vwap_bias', 1, 12, 0.5, 0.4), ('收高差值', 0, 8, 0.5, 1.0), ('mtm_mean', 1, 96, 0.7, 0.3), ('v1_up', 1, 8, 0.3, 1.0), ('rsi', 1, 36, 0.5, 0.3)


class BinanceNeutralStrategy_V1UP_REG_PMO(BinanceNeutralCompoundBase):
    # 3. ('v1_up', True, 9, 0.5, 1), ('reg_ta', False, 24, 0.7, 0.9), ('pmo', True, 3, 0, 1)

    @property
    def display_name(self):
        return "3.V1UP_REG_PMO_6H"

    @property
    def candle_count_4_cal_factor(self):
        return 80

    @property
    def factor_configs(self):
        # return ('v1_up', True, 9, 0.5, 1), ('reg', False, 24, 0.7, 1), ('pmo', True, 3, 0, 1)
        return ('v1_up', True, 9, 0.5, 1), ('reg_ta', False, 24, 0.7, 0.9), ('pmo', True, 3, 0, 1)


class BinanceNeutralStrategy_V1UP_REG_CCI_BIAS(BinanceNeutralCompoundBase):
    # ~4~. ('cci_ema', 1, 24, 0, 0.4), ('mtm_mean', 0, 3, 0.3, 1) 4H
    # 4. ('v1_up', True, 9, 0.3, 0.3), ('reg_ta', False, 24, 0.7, 0.4), ('cci', 1, 96, 0, 0.4), ('bias', 0, 12, 0.5, 0.9) 6H  2021.3.18 换

    @property
    def display_name(self):
        return "4.V1UP_REG_CCI_BIAS_6H"

    @property
    def candle_count_4_cal_factor(self):
        return 200

    @property
    def factor_configs(self):
        return ('v1_up', True, 9, 0.3, 0.3), ('reg_ta', False, 24, 0.7, 0.4), ('cci', 1, 96, 0, 0.4), ('bias', 0, 12, 0.5, 0.9)


class BinanceNeutral_REG_PMO(BinanceNeutralCompoundBase):
    # 5. ('reg_ta', 0, 24, 0.5, 1), ('pmo', 1, 3, 0, 1)

    @property
    def display_name(self):
        return "5.REG_PMO_8H"

    @property
    def candle_count_4_cal_factor(self):
        return 80

    @property
    def factor_configs(self):
        return ('reg_ta', 0, 24, 0.5, 1), ('pmo', 1, 3, 0, 1)


class BinanceNeutral_REG_ZJ_BIAS_CCI_GAP(BinanceNeutralCompoundBase):
    # 6. ('reg_ta', 0, 30, 0.3, 1), ('reg_ta', 0, 24, 0.5, 1), ('资金流入比例', 1, 96, 0, 0.3), ('bias', 0, 16, 0.5, 1), ('cci', 1, 12, 0.5, 0.4), ('gap', 1, 24, 0.3, 1), ('gap', 1, 48, 0.5, 0.3)

    @property
    def display_name(self):
        return "6.REG_ZJ_BIAS_CCI_GAP_6H"

    @property
    def candle_count_4_cal_factor(self):
        return 150

    @property
    def factor_configs(self):
        return ('reg_ta', 0, 30, 0.3, 1), ('reg_ta', 0, 24, 0.5, 1), ('资金流入比例', 1, 96, 0, 0.3), ('bias', 0, 16, 0.5, 1), ('cci', 1, 12, 0.5, 0.4), ('gap', 1, 24, 0.3, 1), ('gap', 1, 48, 0.5, 0.3)


class BinanceNeutral_RCCD_VIX_MARKETPNL_VMA_BIAS(BinanceNeutralCompoundBase):
    # 7. ('rccd', 1, 6, 0.3, 0.7), ('vix', 0, 9, 0.5, 1.0), ('market_pnl', 1, 96, 0.7, 0.1), ('vma', 0, 8, 0.5, 0.7), ('bias', 0, 8, 0.7, 1.0)

    @property
    def display_name(self):
        return "7. RCCD_VIX_MARKETPNL_VMA_BIAS_8H"

    @property
    def candle_count_4_cal_factor(self):
        return 100

    @property
    def factor_configs(self):
        return ('rccd', 1, 6, 0.3, 0.7), ('vix', 0, 9, 0.5, 1.0), ('market_pnl', 1, 96, 0.7, 0.1), ('vma', 0, 8, 0.5, 0.7), ('bias', 0, 8, 0.7, 1.0)


class BinanceNeutral_TRIX_T3_HMA_BBI_SROC_ER_KDJD(BinanceNeutralCompoundBase):
    # 8. ('trix', 1, 30, 0.7, 1.0), ('t3', 0, 8, 0.7, 1.0), ('hma', 0, 28, 0.7, 1.0), ('bbi', 1, 4, 0.7, 0.3), ('sroc', 1, 10, 0.7, 0.9), ('er_bull', 0, 12, 0, 0.7), ('kdjd_d', 1, 52, 0.7, 0.3), ('market_pnl', 0, 96, 0.7, 1.0), ('dbcd', 1, 6, 0.7, 0.2), ('rccd', 1, 8, 0.7, 0.7), ('ko', 1, 6, 0.7, 0.5)

    @property
    def display_name(self):
        return "8. TRIX_T3_HMA_BBI_SROC..._6H"

    @property
    def candle_count_4_cal_factor(self):
        return 250

    @property
    def factor_configs(self):
        return ('trix', 1, 30, 0.7, 1.0), ('t3', 0, 8, 0.7, 1.0), ('hma', 0, 28, 0.7, 1.0), ('bbi', 1, 4, 0.7, 0.3), ('sroc', 1, 10, 0.7, 0.9), ('er_bull', 0, 12, 0, 0.7), ('kdjd_d', 1, 52, 0.7, 0.3), ('market_pnl', 0, 96, 0.7, 1.0), ('dbcd', 1, 6, 0.7, 0.2), ('rccd', 1, 8, 0.7, 0.7), ('ko', 1, 6, 0.7, 0.5)


class BinanceNeutral_SROC_KDJD_HMA_TRIX_MTMMEAM(BinanceNeutralCompoundBase):
    # 9. ('sroc', 0, 3, 0.5, 1.0), ('gap', 1, 80, 0.7, 1.0), ('cci', 1, 36, 0, 1.0), ('kdjd_d', 1, 36, 0.3, 0.3), ('hma', 0, 18, 0.7, 1.0), ('trix', 1, 80, 0.7, 0.9), ('market_pnl', 0, 96, 0.7, 1.0), ('dbcd', 1, 6, 0.5, 1.0), ('mtm_mean', 0, 3, 0.7, 1.0)

    @property
    def display_name(self):
        return "9. SROC_KDJD_HMA_TRIX_MTMMEAM..._8H"

    @property
    def candle_count_4_cal_factor(self):
        return 650

    @property
    def factor_configs(self):
        return ('sroc', 0, 3, 0.5, 1.0), ('gap', 1, 80, 0.7, 1.0), ('cci', 1, 36, 0, 1.0), ('kdjd_d', 1, 36, 0.3, 0.3), ('hma', 0, 18, 0.7, 1.0), ('trix', 1, 80, 0.7, 0.9), ('market_pnl', 0, 96, 0.7, 1.0), ('dbcd', 1, 6, 0.5, 1.0), ('mtm_mean', 0, 3, 0.7, 1.0)
