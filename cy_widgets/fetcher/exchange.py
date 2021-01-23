import time
import pandas as pd
from ..exchange.provider import *
from cy_components.defines.column_names import *
from cy_components.helpers.formatter import CandleFormatter as cf, DateFormatter as dfr
from cy_components.defines.enums import TimeFrame
from cy_components.utils.coin_pair import CoinPair


class ExchangeFetcher:
    """现货交易的抓取数据类, 统一流程"""

    def __init__(self, ccxt_provider: CCXTProvider):
        # API 对象都用外部传入
        self.__ccxt_provider = ccxt_provider

    def __fetch_candle_data_by_ccxt_object(self, coin_pair: CoinPair, time_frame: TimeFrame, since_timestamp, limit, params={}):
        """通过 CCXT 抓取数据，转为统一格式"""
        data = self.__ccxt_provider.ccxt_object_for_fetching.fetch_ohlcv(
            coin_pair.formatted(), time_frame.value, since_timestamp, limit, params)
        df = cf.convert_raw_data_to_data_frame(data, extra_columns_mapping=self.__ccxt_provider.exchange_type.candle_extra_columns_mapping)
        return df

    # 公开的业务逻辑

    def fetch_historical_candle_data(self, coin_pair: CoinPair, time_frame: TimeFrame, since_timestamp, limit, params={}):
        """获取历史K线数据

        Parameters
        ----------
        coin_pair : CoinPair
            币对对象
        time_frame : TimeFrame
            TimeFrame 对象
        since_timestamp : int
            时间戳(ms)
        limit : int
            条数
        params : dict, optional
            额外配置
        """
        return self.__fetch_candle_data_by_ccxt_object(coin_pair, time_frame, since_timestamp, limit, params=params)

    def fetch_historical_candle_data_by_end_date(self, coin_pair: CoinPair, time_frame: TimeFrame, end_date, limit, params={}):
        """从结束时间往前推，请求K线"""
        since_ts = dfr.convert_local_date_to_timestamp(end_date)
        since_ts = time_frame.timestamp_backward_offset(since_ts, limit)
        if limit < 900:
            return self.fetch_historical_candle_data(coin_pair, time_frame, since_ts, limit + 10, params)
        else:
            return self.fetch_real_time_candle_data(coin_pair, time_frame, limit, params)

    def fetch_real_time_candle_data(self, coin_pair: CoinPair, time_frame: TimeFrame, limit, params={}):
        """获取实时K线

        Parameters
        ----------
        coin_pair : CoinPair
            币对对象
        time_frame : TimeFrame
            TimeFrame
        limit : int
            条数
        params : dict, optional
            额外参数, by default {}

        Returns
        -------
        df
            [description]

        Raises
        ------
        ConnectionError
            连接失败
        """        """"""
        result_df = None
        # each time count
        fetch_lmt = 200
        # last count
        last_count = 0

        fetch_lmt = limit if limit < fetch_lmt else fetch_lmt

        # loop until enough
        while result_df is None or result_df.shape[0] < limit:
            earliest_date = pd.datetime.now()
            if result_df is not None and result_df.shape[0] > 0:
                earliest_date = result_df.iloc[0][COL_CANDLE_BEGIN_TIME]

            # fetch
            earliest_ts = int(time.mktime(earliest_date.timetuple()))
            fetch_ts = earliest_ts - fetch_lmt * time_frame.time_interval(res_unit='s')
            # OKEx
            if self.__ccxt_provider.exchange_type == ExchangeType.Okex:
                if limit > 1000 and coin_pair.trade_coin.upper() in ['BTC', 'ETH', 'LTC', 'ETC', 'XRP', 'EOS', 'BCH', 'BSV', 'TRX']:
                    params['type'] = 'HistoryCandles'
                    fetch_lmt = 300
                else:
                    params['type'] = 'Candles'
            elif self.__ccxt_provider.exchange_type == ExchangeType.BinanceSpotFetching or self.__ccxt_provider.exchange_type == ExchangeType.Binance:
                params['endTime'] = earliest_ts * 1000
            df = self.__fetch_candle_data_by_ccxt_object(
                coin_pair, time_frame, fetch_ts * 1000, fetch_lmt, params)

            # update to df
            if result_df is None:
                result_df = df
            else:
                result_df = df.append(result_df, ignore_index=True, verify_integrity=True)
                result_df.drop_duplicates(subset=[COL_CANDLE_BEGIN_TIME], inplace=True)

            # check count (count did not increase and not enough)
            if last_count >= result_df.shape[0] and result_df.shape[0] < limit:
                print("{} 只能抓到 {}".format(coin_pair.formatted(), result_df.shape[0]))
                return result_df
            else:
                last_count = result_df.shape[0]
        return result_df
