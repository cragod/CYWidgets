import pytz
from abc import ABC, abstractmethod
from ..exchange.provider import *
from cy_components.utils.coin_pair import *
from cy_components.defines.enums import TimeFrame
from cy_components.helpers.formatter import CandleFormatter, CandleDateFromType, DateFormatter


class BaseContractFetcher(ABC):

    @classmethod
    def dispatched_fetcher(cls, ccxt_provider: CCXTProvider):
        e_type = ccxt_provider.exchange_type
        if e_type == ExchangeType.Okex:
            return OKExContractFetcher(ccxt_provider)
        NotImplementedError('NotImped')

    def __init__(self, ccxt_provider: CCXTProvider):
        self._ccxt_provider = ccxt_provider

    @abstractmethod
    def fetch_all_contracts_info(self):
        NotImplementedError('NotImped')

    @abstractmethod
    def fetch_futures_candle_data(self, coin_pair: CoinPair, time_frame: TimeFrame, since_timestamp, limit, params={}):
        """抓取交割合约数据，转为统一格式"""
        NotImplementedError('NotImped')

    @abstractmethod
    def fetch_perpetual_candle_data(self, coin_pair: CoinPair, time_frame: TimeFrame, since_timestamp, limit, params={}):
        """抓取永续合约数据，转为统一格式"""
        NotImplementedError('NotImped')


class OKExContractFetcher(BaseContractFetcher):
    """Okex 合约"""

    def fetch_all_contracts_info(self):
        futures = self._ccxt_provider.ccxt_object_for_fetching.futures_get_instruments()
        perpetuals = self._ccxt_provider.ccxt_object_for_fetching.swap_get_instruments()
        return {
            'futures': futures,
            'perpetuals': perpetuals
        }

    def fetch_futures_candle_data(self, coin_pair: CoinPair, time_frame: TimeFrame, since_timestamp, limit, params={}):
        return self.__fetch_instrument_candle(coin_pair, time_frame, since_timestamp, limit, params, self._ccxt_provider.ccxt_object_for_fetching.futures_get_instruments_instrument_id_candles)

    def fetch_perpetual_candle_data(self, coin_pair, time_frame, since_timestamp, limit, params={}):
        return self.__fetch_instrument_candle(coin_pair, time_frame, since_timestamp, limit, params, self._ccxt_provider.ccxt_object_for_fetching.swap_get_instruments_instrument_id_candles)

    def __fetch_instrument_candle(self, coin_pair, time_frame, since_timestamp, limit, params={}, fetching_func=None):
        start_date = DateFormatter.convert_timepstamp_to_local_date(since_timestamp).replace(microsecond=0)
        start_date_iso8601 = DateFormatter.convert_date_to_iso8601(
            start_date.astimezone(tz=pytz.utc).replace(tzinfo=None))  # 请求用 UTC 时间但是不加后缀
        request_params = {
            'instrument_id': coin_pair.formatted('-'),
            'start': '{}Z'.format(start_date_iso8601),
            'granularity': '{}'.format(time_frame.time_interval('s'))
        }
        data = fetching_func(request_params)
        # 结束后转成 +8
        df = CandleFormatter.convert_raw_data_to_data_frame(data, from_type=CandleDateFromType.ISO8601)
        return df
