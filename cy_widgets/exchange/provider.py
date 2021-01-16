import ccxt
import enum


class ExchangeType(enum.IntEnum):
    """交易所类型"""
    Bitfinex = 0
    Okex = 1
    HuobiPro = 2
    Binance = 3
    BinanceSpotFetching = 4

    @classmethod
    def from_name(cls, name):
        """根据交易所名返回具体Type"""
        fname = name.lower() if isinstance(name, str) else ''
        if fname in ['binance']:
            return ExchangeType.Binance
        elif fname in ['huobi', 'huobipro']:
            return ExchangeType.HuobiPro
        elif fname in ['bfx', 'binance']:
            return ExchangeType.Bitfinex
        elif fname in ['ok', 'okex']:
            return ExchangeType.Okex
        else:
            return None

    @property
    def candle_extra_columns_mapping(self):
        if self.value == ExchangeType.BinanceSpotFetching:
            return {
                6: 'quote_volume',
                7: 'trade_num',
                8: 'taker_buy_base_asset_volume',
                9: 'taker_buy_quote_asset_volume',
            }
        return {}


class CCXTObjectFactory:
    """ CCXT 对象工厂类 """

    @staticmethod
    def _config_ccxt_object(ccxt_object, api_key, api_secret):
        ccxt_object.apiKey = api_key
        ccxt_object.secret = api_secret
        return ccxt_object

    @staticmethod
    def binance_ccxt_object(apiKey, apiSecret):
        ccxt_object = ccxt.binance({
            'timeout': 3000,
            'rateLimit': 10,
            'enableRateLimit': False
        })
        return CCXTObjectFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)

    @staticmethod
    def binance_spot_fetching_ccxt_object(apiKey, apiSecret):
        ccxt_object = ccxt_binance_fetching({
            'timeout': 3000,
            'rateLimit': 10,
            'enableRateLimit': False
        })
        return CCXTObjectFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)

    @staticmethod
    def huobipro_ccxt_object(apiKey, apiSecret):
        ccxt_object = ccxt.huobipro()
        return CCXTObjectFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)

    @staticmethod
    def okex_ccxt_object(apiKey, apiSecret, param):
        ccxt_object = ccxt.okex({
            'timeout': 1000,  # timeout时间短一点
            'rateLimit': 10,
            'enableRateLimit': False
        })
        password = param.get('password')
        if password is not None:
            ccxt_object.password = password
        return CCXTObjectFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)

    @staticmethod
    def bitfinex_v1_ccxt_object(apiKey, apiSecret):
        ccxt_object = ccxt.bitfinex()
        ccxt_object.enableRateLimit = True
        ccxt_object.rateLimit = 10000
        return CCXTObjectFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)

    @staticmethod
    def bitfinex_v2_ccxt_object(apiKey, apiSecret):
        ccxt_object = ccxt.bitfinex2()
        ccxt_object.enableRateLimit = True
        ccxt_object.rateLimit = 10000
        return CCXTObjectFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)


class CCXTProvider:
    """CCXT 提供类，负责为外提供对应的 CCXT 对象"""

    def __init__(self, api_key, secret, exg_type: ExchangeType, params={}):
        """
        params:
            'proxies': CCXT Proxies
            'one_token_cfgs': [{
                'key': 'abc',
                'secret': '123',
            }, {
                'key': 'abc',
                'secret': '123',
            }, ...]
            'password': '...."
        """
        self.exchange_type = exg_type
        self.__setup_ccxt_objects(api_key, secret, exg_type, params)
        self.__process_extra_params(params)

    @property
    def ccxt_object_for_fetching(self):
        """用于抓取数据"""
        return self.__object_4_fetching

    @property
    def ccxt_object_for_query(self):
        """用于查询"""
        return self.__object_4_query

    @property
    def ccxt_object_for_order(self):
        """用于下单"""
        return self.__object_4_order

    @property
    def display_name(self):
        """display name"""
        return getattr(self.ccxt_object_for_fetching, 'name')

    @property
    def markets(self):
        """all markets"""
        return self.ccxt_object_for_fetching.load_markets(True)

    def __process_extra_params(self, params={}):
        """ccxt object 公共配置流程"""
        # 所有 obj 都设置
        ccxt_objects = [self.__object_4_fetching, self.__object_4_query, self.__object_4_order]
        # 代理参数
        for obj in ccxt_objects:
            for _, (key, value) in enumerate(params.items()):
                obj.options[key] = value
            # 加载基本数据
            obj.load_markets(True)
            obj.enableRateLimit = True

    def __setup_ccxt_objects(self, api_key, secret, exg_type: ExchangeType, param={}):
        """初始化内部 Objects"""
        if exg_type == ExchangeType.Binance:
            self.__object_4_fetching = self.__object_4_query = self.__object_4_order = CCXTObjectFactory.binance_ccxt_object(
                api_key, secret)
        elif exg_type == ExchangeType.BinanceSpotFetching:
            self.__object_4_fetching = self.__object_4_query = self.__object_4_order = CCXTObjectFactory.binance_spot_fetching_ccxt_object(
                api_key, secret)
        elif exg_type == ExchangeType.HuobiPro:
            self.__object_4_fetching = self.__object_4_query = self.__object_4_order = CCXTObjectFactory.huobipro_ccxt_object(
                api_key, secret)
        elif exg_type == ExchangeType.Okex:
            self.__object_4_fetching = self.__object_4_query = self.__object_4_order = CCXTObjectFactory.okex_ccxt_object(
                api_key, secret, param)
        elif exg_type == ExchangeType.Bitfinex:
            self.__object_4_fetching = self.__object_4_order = CCXTObjectFactory.bitfinex_v1_ccxt_object(
                api_key, secret)
            self.__object_4_query = CCXTObjectFactory.bitfinex_v2_ccxt_object(api_key, secret)


class ccxt_binance_fetching(ccxt.binance):
    def parse_ohlcv(self, ohlcv, market=None):
        #
        #     [
        #         1591478520000,
        #         "0.02501300",
        #         "0.02501800",
        #         "0.02500000",
        #         "0.02500000",
        #         "22.19000000",
        #         1591478579999,
        #         "0.55490906",
        #         40,
        #         "10.92900000",
        #         "0.27336462",
        #         "0"
        #     ]
        #
        return [
            self.safe_integer(ohlcv, 0),
            self.safe_float(ohlcv, 1),
            self.safe_float(ohlcv, 2),
            self.safe_float(ohlcv, 3),
            self.safe_float(ohlcv, 4),
            self.safe_float(ohlcv, 5),
            self.safe_float(ohlcv, 7),  # quote_volume
            self.safe_float(ohlcv, 8),  # trade_num
            self.safe_float(ohlcv, 9),  # taker_buy_base_asset_volume
            self.safe_float(ohlcv, 10),  # taker_buy_quote_asset_volume
        ]
