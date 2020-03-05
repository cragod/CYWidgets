import re
import time
import pandas as pd
from abc import abstractproperty

from cy_components.utils.one_token import OneToken
from cy_components.utils.coin_pair import CoinPair
from cy_components.defines.enums import TimeFrame
from cy_components.helpers.formatter import CandleFormatter as cf
from cy_components.defines.column_names import *
from ..exchange.base import ExchangeBase


class ExchangeFetcher(ExchangeBase):
    """抓取数据基础类, ExchangeBase 的抽象方法继续往下传"""

    one_token = OneToken()

    def __init__(self, apiKey='', apiSecret='', params={}):
        super().__init__(apiKey=apiKey, apiSecret=apiSecret, params=params)
        self._process_one_token_cfgs(params)

    @abstractproperty
    def _ccxt_object_for_fetching(self):
        """获取用于抓取数据的 CCXT 对象"""
        raise NotImplementedError

    def _process_one_token_cfgs(self, params={}):
        """尝试配置 OneToken 对象"""
        if 'one_token_cfgs' in params:
            self.one_token = OneToken(params['one_token_cfgs'])

    def _fetch_candle_data_by_ccxt_object(self, coin_pair: CoinPair, time_frame: TimeFrame, since_timestamp, limit, hour_offset=0, params={}):
        """通过 CCXT 抓取数据，转为统一格式"""
        data = self._ccxt_object_for_fetching.fetch_ohlcv(coin_pair.pair(), time_frame.value, since_timestamp, limit)
        df = cf.convert_raw_data_to_data_frame(data, hour_offset=hour_offset)
        return df

    def _fetch_candle_data_by_one_token(self, coin_pair: CoinPair, time_frame: TimeFrame, since_timestamp, limit, params={}):
        """通过 OneToken 抓取数据，转为统一格式"""
        exchange_name = re.sub('Fetcher$', '', self.__class__.__name__)
        data = self.one_token.fetch_candle_data(exchange_name, coin_pair, time_frame, limit, since_timestamp)
        df = pd.DataFrame(data=data, columns=[COL_CANDLE_BEGIN_TIME, COL_OPEN, COL_HIGH, COL_LOW, COL_CLOSE,
                                              COL_VOLUME])
        return df

    # 公开的业务逻辑

    def fetch_historical_candle_data(self, coin_pair: CoinPair, time_frame: TimeFrame, since_timestamp, limit, params={}):
        """Fetch historical ohlcv, use ccxt object by default."""
        return self._fetch_candle_data_by_ccxt_object(coin_pair, time_frame, since_timestamp, limit, params=params)

    def fetch_real_time_candle_data(self, coin_pair: CoinPair, time_frame: TimeFrame, limit, params={}):
        """Fetch real time ohlcv, use ccxt object."""
        result_df = None
        # each time count
        fetch_lmt = 888
        # last count
        last_count = 0

        # loop until enough
        while result_df is None or result_df.shape[0] < limit:
            earliest_date = pd.datetime.now()
            if result_df is not None and result_df.shape[0] > 0:
                earliest_date = result_df.iloc[0][COL_CANDLE_BEGIN_TIME]

            # fetch
            earliest_ts = int(time.mktime(earliest_date.timetuple()))
            fetch_ts = earliest_ts - fetch_lmt * time_frame.time_interval(res_unit='s')
            df = self._fetch_candle_data_by_ccxt_object(coin_pair, time_frame, fetch_ts * 1000, fetch_lmt, hour_offset=8)

            # update to df
            if result_df is None:
                result_df = df
            else:
                result_df = df.append(result_df, ignore_index=True, verify_integrity=True)
                result_df.drop_duplicates(subset=[COL_CANDLE_BEGIN_TIME], inplace=True)

            # check count (count did not increase and not enough)
            if last_count >= result_df.shape[0] and result_df.shape[0] < limit:
                raise ConnectionError('Fetch candle failed')
            else:
                last_count = result_df.shape[0]
        return result_df
